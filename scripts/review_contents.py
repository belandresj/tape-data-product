"""Audit the exact staged Git bytes for exclusions, paths, secrets and local links.

Heuristic secret detection is defense in depth, not a proof of all possible secrets.
"""
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote

ROOT=Path(__file__).resolve().parents[1]
files=subprocess.check_output(['git','ls-files','-z'],cwd=ROOT).decode().split('\0')[:-1]
findings=[];sizes=[];contents={}
for name in files:
    data=subprocess.check_output(['git','show',':'+name],cwd=ROOT)
    contents[name]=data;sizes.append((len(data),name))
    if len(data)>512*1024:findings.append([name,'file over 512 KiB'])
    if Path(name).suffix.lower() in {'.parquet','.sqlite','.db','.duckdb','.png','.jpg','.pdf','.log','.pyc'}:
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
            if not resolved.exists():findings.append([name,'missing Markdown target: '+target])
# Only variable names and empty values in the environment example.
for line in contents.get('.env.example',b'').decode().splitlines():
    if line and not re.fullmatch(r'[A-Z_][A-Z_0-9]*=',line):findings.append(['.env.example','nonempty variable value'])
result=dict(files=len(files),staged_bytes=sum(x[0] for x in sizes),largest=[{'path':p,'bytes':n} for n,p in sorted(sizes,reverse=True)[:8]],findings=findings)
print(json.dumps(result,indent=2))
sys.exit(bool(findings))
