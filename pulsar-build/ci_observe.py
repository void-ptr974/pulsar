#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Record bounded host counters while running one CI command, preserving its exit code."""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import subprocess
import threading
import time


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read_file(path):
    try:
        return Path(path).read_text()[:65536]
    except OSError:
        return None


def command_output(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)
        return {"stdout": result.stdout[:16384], "stderr": result.stderr[:1024],
                "exit_code": result.returncode}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"stdout": "", "error": type(error).__name__}


def host_sample():
    disk = shutil.disk_usage(".")
    result = {"utc": utc_now(), "monotonic_seconds": time.monotonic(),
              "disk_free_bytes": disk.free}
    if platform.system() == "Linux":
        # Cumulative counters permit interval deltas without installing monitoring packages.
        result["linux"] = {name: read_file(path) for name, path in {
            "cpu": "/proc/stat", "memory": "/proc/meminfo", "load": "/proc/loadavg",
            "disk": "/proc/diskstats", "network": "/proc/net/dev",
            "cpu_pressure": "/proc/pressure/cpu", "memory_pressure": "/proc/pressure/memory",
            "io_pressure": "/proc/pressure/io", "cgroup_cpu": "/sys/fs/cgroup/cpu.stat",
            "cgroup_cpu_limit": "/sys/fs/cgroup/cpu.max",
            "cgroup_memory": "/sys/fs/cgroup/memory.current",
            "cgroup_memory_events": "/sys/fs/cgroup/memory.events",
        }.items()}
    elif platform.system() == "Darwin":
        top = command_output(["/usr/bin/top", "-l", "2", "-s", "1", "-n", "0"])
        prefixes = ("Load Avg:", "CPU usage:", "PhysMem:", "VM:", "Networks:", "Disks:")
        last_summary = {}
        for line in top["stdout"].splitlines():
            for prefix in prefixes:
                if line.startswith(prefix):
                    last_summary[prefix] = line
        result["macos"] = {
            "top_summary": "\n".join(last_summary.values()),
            "cpu_measurement_interval_seconds": 1,
            "top_status": {key: value for key, value in top.items() if key != "stdout"},
            "vm_stat": command_output(["/usr/bin/vm_stat"]),
            "disk_counters": command_output(["/usr/sbin/iostat", "-I", "-d", "-c", "1"]),
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.interval <= 0:
        parser.error("a command and a positive sampling interval are required")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    metadata = {"utc": utc_now(), "system": platform.system(), "release": platform.release(),
                "machine": platform.machine(), "logical_cpus": os.cpu_count(),
                "sample_interval_seconds": args.interval,
                "note": "Host counters are cumulative; no environment or process arguments collected."}
    if platform.system() == "Linux":
        cpu_info = read_file("/proc/cpuinfo") or ""
        metadata["cpu_model"] = next((line.split(":", 1)[1].strip() for line in cpu_info.splitlines()
                                      if line.startswith("model name")), None)
    elif platform.system() == "Darwin":
        metadata["cpu_model"] = command_output(["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"])
        metadata["memory_bytes"] = command_output(["/usr/sbin/sysctl", "-n", "hw.memsize"])
    (output / "host.json").write_text(json.dumps(metadata, indent=2) + "\n")
    stop = threading.Event()

    def record_samples():
        with (output / "resources.jsonl").open("w") as samples:
            while True:
                sample_started = time.monotonic()
                try:
                    item = host_sample()
                except Exception as error:  # Recording failure must not cancel the CI command.
                    item = {"utc": utc_now(), "sampling_error": type(error).__name__}
                samples.write(json.dumps(item) + "\n")
                samples.flush()
                if stop.wait(max(0, args.interval - (time.monotonic() - sample_started))):
                    break

    sampler = threading.Thread(target=record_samples, daemon=True)
    sampler.start()
    started_utc, started = utc_now(), time.monotonic()
    print(f"CI_OBSERVE command_start utc={started_utc} monotonic={started:.6f}", flush=True)
    child = subprocess.Popen(command, start_new_session=True)

    def forward_signal(signum, _frame):
        try:
            os.killpg(child.pid, signum)
        except ProcessLookupError:
            pass

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, forward_signal)
    _, status, usage = os.wait4(child.pid, 0)
    child.returncode = os.waitstatus_to_exitcode(status)
    ended, ended_utc = time.monotonic(), utc_now()
    stop.set()
    sampler.join(timeout=20)
    summary = {"started_utc": started_utc, "ended_utc": ended_utc,
               "wall_seconds": ended - started, "exit_code": child.returncode,
               "child_user_cpu_seconds": usage.ru_utime, "child_system_cpu_seconds": usage.ru_stime,
               "child_max_rss_native_units": usage.ru_maxrss,
               "max_rss_units": "bytes" if platform.system() == "Darwin" else "KiB",
               "child_major_page_faults": usage.ru_majflt,
               "child_voluntary_context_switches": usage.ru_nvcsw,
               "child_involuntary_context_switches": usage.ru_nivcsw,
               "limitations": ["Kernel child usage may omit persistent Gradle daemon work.",
                               "Sampling and profiling add overhead; this is a diagnostic run.",
                               "Sampler ends with the command; runner diagnostic logs cover later finalization."]}
    (output / "command.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"CI_OBSERVE command_end utc={ended_utc} wall_seconds={ended - started:.3f} "
          f"exit_code={child.returncode}", flush=True)
    return child.returncode if child.returncode >= 0 else 128 - child.returncode


if __name__ == "__main__":
    raise SystemExit(main())
