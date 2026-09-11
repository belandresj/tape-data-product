"""Independent, fail-closed resource supervision. Never signals incumbent PIDs."""
from __future__ import annotations
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import psutil

MiB=1024**2
GiB=1024**3


class ResourceWait(RuntimeError):
    pass


def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.json.partial')
    temporary.write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n')
    temporary.replace(path)


def discover_incumbent(research, now=None):
    """Fresh status + exact PID/create-time + queue path in command line."""
    now=time.time() if now is None else now
    candidates=[]
    for path in Path(research).glob('tape_feature_queue*/status.json'):
        try:
            state=json.loads(path.read_text())
            updated=datetime.fromisoformat(state['updated_at']).timestamp()
            if now-updated > 120 or updated > now+5:
                continue
            p=psutil.Process(state['pid'])
            if abs(p.create_time()-state['process_created'])>.01:
                continue
            cmd=p.cmdline()
            if str(path.parent.resolve()) not in cmd or not any('run_tape_feature_queue' in x for x in cmd):
                continue
            candidates.append((updated,dict(pid=p.pid,created=p.create_time(),root=str(path.parent.resolve()))))
        except (OSError,ValueError,KeyError,psutil.Error):
            continue
    # An unidentified live queue is uncertainty, not evidence of zero incumbent RSS.
    live=[]
    for p in psutil.process_iter():
        try:
            cmd=p.cmdline()
        except psutil.NoSuchProcess:
            continue
        except (psutil.AccessDenied, SystemError):
            # macOS can wrap KERN_PROCARGS2 permission errors in SystemError.
            # Inspect this PID through ps rather than abandoning the live scan.
            result=subprocess.run(['/bin/ps','-p',str(p.pid),'-o','command='],capture_output=True,text=True)
            if result.returncode==1 and not result.stdout.strip():
                continue
            if result.returncode or not result.stdout.strip():
                raise ResourceWait(f'cannot inspect process {p.pid}')
            cmd=result.stdout.split()
        if any('run_tape_feature_queue' in x for x in cmd) and '_daemon' in cmd:
            live.append(p.pid)
    known={x[1]['pid'] for x in candidates}
    if set(live)-known:
        raise ResourceWait('live feature supervisor has stale or unverified status')
    if len(known)>1:
        raise ResourceWait('multiple live feature supervisors require explicit tree accounting')
    return max(candidates,key=lambda x:x[0])[1] if candidates else None


def tree(identity):
    if identity is None:return {}
    try:
        p=psutil.Process(identity['pid'])
        if abs(p.create_time()-identity['created'])>.01:return {}
        values={}
        for x in [p,*p.children(recursive=True)]:
            try:values[x.pid]=x.memory_info().rss
            except psutil.NoSuchProcess:pass
        return values
    except psutil.NoSuchProcess:return {}


def scratch_bytes(path):
    total=0
    for root,dirs,files in os.walk(path):
        for f in files:
            try:total+=(Path(root)/f).stat().st_size
            except FileNotFoundError:pass
    return total


def pressure(*, new_rss, incumbent_rss, available, free, needed=0):
    if new_rss>=480*MiB:return 'new worker approaching 512 MiB stop'
    if available<GiB:return 'system available memory below 1 GiB'
    if new_rss+incumbent_rss>=2*GiB:return 'combined RSS reached 2 GiB'
    if free<3*GiB+needed:return 'disk reserve/headroom unavailable'
    return None


def admit(research,scratch,projected_peak,needed=0,seconds=10,interval=.1):
    if projected_peak<=0 or projected_peak>384*MiB:
        raise ResourceWait('representative peak unavailable or above 384 MiB target')
    start=time.monotonic();minimum_available=None;peak_incumbent=0;identity=None
    rediscovered=-float("inf");current=None
    while True:
        now=time.monotonic()
        if now-rediscovered>=1:
            current=discover_incumbent(research);rediscovered=now
        if identity is not None and current!=identity:
            raise ResourceWait('incumbent changed during admission; rediscover before another attempt')
        identity=current
        incumbent_values=tree(current)
        if current and not incumbent_values:raise ResourceWait('incumbent exited or PID identity changed')
        incumbent=sum(incumbent_values.values())
        supervisor=psutil.Process().memory_info().rss
        available=psutil.virtual_memory().available
        minimum_available=available if minimum_available is None else min(minimum_available,available)
        peak_incumbent=max(peak_incumbent,incumbent)
        if available<1.25*GiB or incumbent+supervisor+1.2*projected_peak>=2*GiB:
            raise ResourceWait('insufficient stable memory headroom')
        if shutil.disk_usage(scratch).free<3*GiB+needed:
            raise ResourceWait('insufficient projected disk headroom')
        if time.monotonic()-start>=seconds:
            return dict(incumbent=identity,admission_seconds=time.monotonic()-start,
                        minimum_available_bytes=minimum_available,peak_incumbent_rss_bytes=peak_incumbent)
        time.sleep(interval)


class AdmissionHistory:
    """Reuse only a successful, recently and continuously monitored session.

    A fresh process still receives a fresh instantaneous admission check. At
    most one second may pass since the last successful monitor observation;
    otherwise the full ten-second observation period is required again.
    This object is local to one sequential run and is never serialized.
    """
    def __init__(self):self.clear()
    def clear(self):self.observed=None;self.incumbent=None
    def seconds(self, research):
        if self.observed is None or not 0<=time.monotonic()-self.observed<=1:
            self.clear();return 10
        if discover_incumbent(research)!=self.incumbent:
            self.clear();return 10
        return 0
    def record(self, observed, incumbent):
        self.observed=observed;self.incumbent=incumbent


def supervise(command, *, research, scratch, report, projected_peak, needed=0, timeout=3600, admission_history=None, on_observation=None):
    """Single child process; pressure marker then termination only of owned tree.

    100 ms observations, 2 s cooperative grace; descendants identified by
    create time. The supervisor's own RSS is included in extension totals.
    """
    scratch=Path(scratch);scratch.mkdir(parents=True,exist_ok=True)
    marker=scratch/'yield.request'
    if marker.exists():raise ResourceWait('prior pressure marker requires reviewed resume')
    admission_seconds=10 if admission_history is None else admission_history.seconds(research)
    expected_incumbent=admission_history.incumbent if admission_history is not None else None
    if admission_history is not None:admission_history.clear()
    admission=admit(research,scratch,projected_peak,needed,seconds=admission_seconds)
    if admission_seconds==0 and admission['incumbent']!=expected_incumbent:
        admission_seconds=10
        admission=admit(research,scratch,projected_peak,needed,seconds=10)
    admission['reused_continuous_observations']=admission_seconds==0
    env=os.environ.copy()
    env.update({x:'1' for x in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','VECLIB_MAXIMUM_THREADS','ARROW_NUM_THREADS')})
    env['TAPE_EDA_YIELD_FILE']=str(marker.resolve())
    started=time.monotonic();reason=None;requested=None
    measurement=dict(admission=admission,peak_new_rss_bytes=0,peak_combined_rss_bytes=0,
                     minimum_available_bytes=admission['minimum_available_bytes'],scratch_high_water_bytes=0,
                     maximum_observation_gap_seconds=0,requested_observation_period_seconds=.1)
    Path(report).parent.mkdir(parents=True,exist_ok=True)
    with Path(report).with_suffix('.log').open('a') as log:
        child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
        identity=dict(pid=child.pid,created=psutil.Process(child.pid).create_time())
        owned={};previous_observation=time.monotonic()
        current=admission["incumbent"];rediscovered=sized=reported=-float("inf")
        while child.poll() is None:
            observed=time.monotonic()
            measurement['maximum_observation_gap_seconds']=max(measurement['maximum_observation_gap_seconds'],observed-previous_observation)
            previous_observation=observed
            new=tree(identity)
            for pid in new:
                try:owned[pid]=psutil.Process(pid).create_time()
                except psutil.NoSuchProcess:pass
            worker_rss=sum(new.values());supervisor_rss=psutil.Process().memory_info().rss
            new_rss=worker_rss+supervisor_rss
            measurement['peak_worker_rss_bytes']=max(measurement.get('peak_worker_rss_bytes',0),worker_rss)
            measurement['peak_supervisor_rss_bytes']=max(measurement.get('peak_supervisor_rss_bytes',0),supervisor_rss)
            try:
                if observed-rediscovered>=1:
                    current=discover_incumbent(research);rediscovered=observed
                incumbent_values=tree(current)
                if current and not incumbent_values:raise ResourceWait('incumbent exited or PID identity changed')
                incumbent=sum(incumbent_values.values())
                if current!=admission['incumbent']:reason=reason or 'incumbent changed; yield before readmission'
            except ResourceWait as exc:
                incumbent=0;reason=reason or str(exc)
            available=psutil.virtual_memory().available
            free=shutil.disk_usage(scratch).free
            measurement['peak_new_rss_bytes']=max(measurement['peak_new_rss_bytes'],new_rss)
            measurement['peak_combined_rss_bytes']=max(measurement['peak_combined_rss_bytes'],new_rss+incumbent)
            measurement['minimum_available_bytes']=min(measurement['minimum_available_bytes'],available)
            if observed-sized>=1:
                measurement['scratch_high_water_bytes']=max(measurement['scratch_high_water_bytes'],scratch_bytes(scratch));sized=observed
            if on_observation is not None and observed-reported>=1:
                on_observation(dict(measurement,worker=identity,current_new_rss_bytes=new_rss,current_available_bytes=available))
                reported=observed
            reason=reason or pressure(new_rss=new_rss,incumbent_rss=incumbent,available=available,free=free,needed=needed)
            if time.monotonic()-started>timeout:reason=reason or 'worker timeout'
            if reason and requested is None:
                marker.write_text(reason);requested=time.monotonic()
            if reason and (new_rss>=500*MiB or time.monotonic()-requested>=2):
                for pid,created in owned.items():
                    try:
                        p=psutil.Process(pid)
                        if abs(p.create_time()-created)<.01:p.terminate()
                    except psutil.NoSuchProcess:pass
                try:child.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    for pid,created in owned.items():
                        try:
                            p=psutil.Process(pid)
                            if abs(p.create_time()-created)<.01:p.kill()
                        except psutil.NoSuchProcess:pass
            time.sleep(max(0,.1-(time.monotonic()-observed)))
        measurement['scratch_high_water_bytes']=max(measurement['scratch_high_water_bytes'],scratch_bytes(scratch))
        measurement.update(elapsed_seconds=time.monotonic()-started,returncode=child.returncode,
                           stop_reason=reason,os_memory_limit='macOS RLIMIT_AS not usable for shared mappings; independently monitored RSS',
                           expensive_discovery_period_seconds=1,scratch_scan_period_seconds=1,target_met=measurement['peak_new_rss_bytes']<=384*MiB)
        save(report,measurement)
        if admission_history is not None and not reason and child.returncode==0 and measurement['target_met'] and measurement['maximum_observation_gap_seconds']<=1:
            admission_history.record(previous_observation,current)
    return measurement


def check_yield():
    path=os.environ.get('TAPE_EDA_YIELD_FILE')
    if path and Path(path).exists():raise ResourceWait(Path(path).read_text())


def worker_setup():
    os.nice(10)
    import pyarrow as pa
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    # RLIMIT_AS is unreliable on macOS shared mappings; RSS is supervised live.
