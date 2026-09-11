"""Run one command with live process-tree RSS observation; save compact evidence."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import psutil

output = Path(sys.argv[1])
command = sys.argv[2:]
started = time.monotonic()
env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTEST_DISABLE_PLUGIN_AUTOLOAD='1',
           OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', MPLBACKEND='Agg')
peak = 0
stop = None
with output.with_suffix('.log').open('w') as log:
    child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
    process = psutil.Process(child.pid)
    while child.poll() is None:
        try:
            processes = [process, *process.children(recursive=True)]
            rss = sum(p.memory_info().rss for p in processes if p.is_running())
            peak = max(peak, rss)
            if rss >= 768 * 1024**2:
                stop = '768 MiB curation test ceiling'
                for p in reversed(processes):
                    try: p.terminate()
                    except psutil.NoSuchProcess: pass
                break
        except psutil.NoSuchProcess:
            pass
        time.sleep(.05)
    code = child.wait()
result = dict(command=command, exit_code=code, stop_reason=stop, peak_process_tree_rss_bytes=peak,
              elapsed_seconds=round(time.monotonic()-started, 3), sampling_interval_seconds=.05)
output.write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result))
sys.exit(code if not stop else 1)
