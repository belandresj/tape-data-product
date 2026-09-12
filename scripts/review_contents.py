"""Audit the exact staged Git bytes for exclusions, paths, secrets and local links.

Heuristic secret detection is defense in depth, not a proof of all possible secrets.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote

ROOT=Path(__file__).resolve().parents[1]
files=subprocess.check_output(['git','ls-files','-z'],cwd=ROOT).decode().split('\0')[:-1]
findings=[];sizes=[]
contents={name:subprocess.check_output(['git','show',':'+name],cwd=ROOT) for name in files}
manifest_path='reports/report_assets/publication_manifest.json'
approved_images=set()
if manifest_path in contents:
    try:
        manifest=json.loads(contents[manifest_path])
        records=manifest['files']
        if not isinstance(records,dict):raise ValueError('files must be an object')
        for asset,record in records.items():
            if not isinstance(asset,str) or Path(asset).name!=asset or asset in ('.','..'):
                raise ValueError('asset names must be plain filenames')
            name='reports/report_assets/'+asset
            data=contents.get(name)
            if data is None or len(data)!=record['bytes'] or hashlib.sha256(data).hexdigest()!=record['sha256']:
                findings.append([name,'publication asset missing or identity mismatch'])
                continue
            if Path(asset).suffix.lower()=='.png':
                if not data.startswith(b'\x89PNG\r\n\x1a\n') or len(data)>2*1024**2:
                    findings.append([name,'publication PNG invalid or over 2 MiB'])
                else:approved_images.add(name)
    except (ValueError,KeyError,TypeError) as exc:
        findings.append([manifest_path,'invalid publication manifest: '+str(exc)])
for name,data in contents.items():
    if re.search(r'(?:^|/)(?:implementation-progress|.*implementation_spec|.*handoff|.*agent_prompt|.*work-plan)', name, re.I):
        findings.append([name,'private implementation material'])
    sizes.append((len(data),name))
    if len(data)>512*1024 and name not in approved_images:findings.append([name,'file over 512 KiB'])
    if name not in approved_images and Path(name).suffix.lower() in {'.parquet','.sqlite','.db','.duckdb','.png','.jpg','.pdf','.log','.pyc'}:
        findings.append([name,'excluded file type'])
    if name.endswith('.env') or (Path(name).name.startswith('.env.') and name!='.env.example'):
        findings.append([name,'credential file'])
    if re.search(rb'/(?:Users|Volumes|home)/[^\s\x22\x27]+',data):findings.append([name,'machine-specific absolute path'])
    for pattern in (rb'-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----',
                    rb'gh[pousr]_[A-Za-z0-9]{30,}',rb'github_pat_[A-Za-z0-9_]{30,}',
                    rb'AKIA[A-Z0-9]{16}',rb'sk-[A-Za-z0-9_-]{30,}'):
        if re.search(pattern,data):findings.append([name,'possible credential'])
    if name.endswith('.md'):
        text=data.decode()
        # Check actual Markdown targets, excluding fenced text examples.
        text=re.sub(r'```.*?```','',text,flags=re.S)
        for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',text):
            if '://' in target or target.startswith('#') or target.startswith('mailto:'):continue
            target=unquote(target.split('#')[0])
            resolved=(ROOT/Path(name).parent/target).resolve()
            if not resolved.is_relative_to(ROOT):
                findings.append([name,'Markdown target outside repository: '+target])
            elif resolved.relative_to(ROOT).as_posix() not in contents:
                findings.append([name,'Markdown target absent from Git index: '+target])
# Only variable names and empty values in the environment example.
for line in contents.get('.env.example',b'').decode().splitlines():
    if line and not re.fullmatch(r'[A-Z_][A-Z_0-9]*=',line):findings.append(['.env.example','nonempty variable value'])
result=dict(files=len(files),staged_bytes=sum(x[0] for x in sizes),largest=[{'path':p,'bytes':n} for n,p in sorted(sizes,reverse=True)[:8]],findings=findings)
print(json.dumps(result,indent=2))
sys.exit(bool(findings))
