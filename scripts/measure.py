"""Run one owned command with 50 ms process-tree RSS monitoring.

The 768 MiB synthetic-test ceiling is deliberately below the 2 GiB working
budget. Only this command and its descendants are stopped on a breach.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil


def stop_tree(processes):
    for process in reversed(processes):
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(processes, timeout=2)
    for process in alive:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass


def main():
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    command = sys.argv[2:]
    if not command:
        raise SystemExit('Provide an output JSON path followed by the worker command')
    started = time.monotonic()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',
               OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', MPLBACKEND='Agg')
    peak = 0
    stop = None
    with output.with_suffix('.log').open('x') as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        process = psutil.Process(child.pid)
        while child.poll() is None:
            try:
                processes = [process, *process.children(recursive=True)]
                rss = 0
                for descendant in processes:
                    try:
                        rss += descendant.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass
                peak = max(peak, rss)
                if rss >= 768 * 1024**2:
                    stop = '768 MiB synthetic-test ceiling'
                    stop_tree(processes)
                    break
            except psutil.NoSuchProcess:
                pass
            time.sleep(.05)
        code = child.wait()
    result = dict(command=command, exit_code=code, stop_reason=stop,
                  peak_process_tree_rss_bytes=peak,
                  elapsed_seconds=round(time.monotonic()-started, 3),
                  sampling_interval_seconds=.05)
    with output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))
    return code if not stop else 1


if __name__ == '__main__':
    raise SystemExit(main())
