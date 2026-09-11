"""Bounded retry classification, durable diagnostics and supervised execution."""
import json
import os
import pathlib
import time


def transient(exc):
    from botocore.exceptions import ClientError, IncompleteReadError, ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError, ResponseStreamingError
    if isinstance(exc,(TimeoutError,ConnectionError,IncompleteReadError,ConnectionClosedError,ConnectTimeoutError,EndpointConnectionError,ReadTimeoutError,ResponseStreamingError)):
        return True
    if isinstance(exc,ClientError):
        response=exc.response
        return response.get('ResponseMetadata',{}).get('HTTPStatusCode') in (429,500,502,503,504) or response.get('Error',{}).get('Code') in ('SlowDown','RequestTimeout','InternalError','ServiceUnavailable')
    return False


def fatal(exc):
    from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
    if isinstance(exc,(MemoryError,OSError,NoCredentialsError,PartialCredentialsError)) and not transient(exc):return True
    if isinstance(exc,ClientError):
        return exc.response.get('ResponseMetadata',{}).get('HTTPStatusCode') in (401,403)
    return not (transient(exc) or isinstance(exc,ValueError))


def journal(path,event):
    with pathlib.Path(path).open('a') as stream:
        stream.write(json.dumps({'time_ns':time.time_ns(),**event},sort_keys=True)+'\n');stream.flush();os.fsync(stream.fileno())


def retry_download(operation,partial,log,*,sleep=time.sleep):
    """Three attempts; only delete this attempt's private incomplete download."""
    for attempt in range(1,4):
        try:return operation()
        except Exception as exc:
            retry=transient(exc) and attempt<3
            journal(log,{'attempt':attempt,'error_type':type(exc).__name__,'retry':retry})
            if not retry:raise
            pathlib.Path(partial).unlink(missing_ok=True)
            sleep((2,5)[attempt-1])


def supervise(plan_path,*,resume=False):
    import fcntl,psutil,shutil,subprocess,sys,signal
    from tape_cohort_pipeline import _load_plan
    from tape_cohort_outputs import atomic_json
    plan=_load_plan(plan_path);root=pathlib.Path(plan['result_root']);settings=plan['settings'];root.mkdir(parents=True,exist_ok=True)
    with (root/(plan['query_run_hash']+'.supervisor.lock')).open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        stamp=str(time.time_ns());logpath=root/('run-'+stamp+'.log');resource=root/('resources-'+stamp+'.json')
        cmd=[sys.executable,'-B',str(pathlib.Path(__file__).with_name('run_tape_cohort_query.py')),'run','--worker','--plan',str(pathlib.Path(plan_path).resolve())]
        if resume:cmd.append('--resume')
        start=time.monotonic();last=start;peak=0;gap=0;reason=None;minimum=psutil.virtual_memory().available
        def stop(child):
            try:
                procs=psutil.Process(child.pid).children(recursive=True)
                for proc in reversed(procs):
                    try:proc.kill()
                    except psutil.Error:pass
                child.kill()
            except ProcessLookupError:pass
        def interrupted(signum,frame): raise KeyboardInterrupt('supervisor terminated')
        previous_term=signal.signal(signal.SIGTERM,interrupted)
        with logpath.open('x') as log:
            environment=dict(os.environ,COHORT_SUPERVISOR_PID=str(os.getpid()))
            child=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,env=environment)
            try:
                while child.poll() is None:
                    now=time.monotonic();gap=max(gap,now-last);last=now
                    try:
                        p=psutil.Process(child.pid);rss=psutil.Process().memory_info().rss+sum(x.memory_info().rss for x in [p,*p.children(recursive=True)])
                        peak=max(peak,rss)
                    except psutil.Error:pass
                    available=psutil.virtual_memory().available;minimum=min(minimum,available)
                    if peak>=settings['rss_stop_bytes']:reason='rss_limit'
                    elif available<settings['minimum_available_bytes']:reason='available_memory_floor'
                    elif shutil.disk_usage(root).free<settings['minimum_free_disk_bytes']:reason='disk_floor'
                    if reason:stop(child);break
                    time.sleep(.1)
            except BaseException:
                reason='supervisor_interrupted';stop(child);raise
            finally:
                code=child.wait()
                signal.signal(signal.SIGTERM,previous_term)
                if code or reason:
                    completion=root/plan['query_run_hash']/'manifest.json'
                    if completion.exists():
                        value=json.loads(completion.read_text())
                        if value.get('state')!='complete':
                            value.update(state='incomplete',supervisor_stop_reason=reason or 'worker_error')
                            atomic_json(completion,value)
                atomic_json(resource,{'exit_code':code,'stop_reason':reason,'peak_tree_rss_bytes':peak,'minimum_available_bytes':minimum,'maximum_monitor_gap_seconds':gap,'elapsed_seconds':time.monotonic()-start,'log':str(logpath)})
        return {'exit_code':code or (1 if reason else 0),'resources':str(resource),'log':str(logpath)}
