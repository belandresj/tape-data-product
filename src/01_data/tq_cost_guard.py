"""Conservative R2 limits, persistent across resumes (not an account billing cap)."""
import json
from pathlib import Path
import sqlite3

POLICY={'max_published_bytes':100_000_000_000,'max_class_a_attempts':50_000,'max_class_b_attempts':250_000,'storage_class':'STANDARD','user_monthly_budget_usd':30}
POLICY_FIELDS=set(POLICY)

class CostLimit(RuntimeError):pass

def validate_policy(policy):
    if set(policy)!=POLICY_FIELDS:raise ValueError('Invalid R2 cost policy fields')
    if policy['storage_class']!='STANDARD':raise ValueError('Only measured STANDARD storage is supported')
    for name in ('max_published_bytes','max_class_a_attempts','max_class_b_attempts','user_monthly_budget_usd'):
        if not isinstance(policy[name],int) or policy[name]<=0:raise ValueError(f'Invalid R2 cost policy value: {name}')
    return dict(policy)

def load_policy(path):
    return validate_policy(json.loads(Path(path).read_text()))

class CostGuard:
    def __init__(self,path:Path,policy):
        self.path=path;self.policy=validate_policy(policy)
        with sqlite3.connect(path) as con:
            con.execute('CREATE TABLE IF NOT EXISTS requests (class TEXT PRIMARY KEY, attempts INTEGER NOT NULL)')
            con.execute('CREATE TABLE IF NOT EXISTS reservations (object_key TEXT PRIMARY KEY, bytes INTEGER NOT NULL)')

    def before_send(self,request=None,event_name='',**kwargs):
        # Botocore invokes before-send for each HTTP attempt, including SDK retries.
        operation=event_name.rsplit('.',1)[-1]
        if operation in {'PutObject','ListObjectsV2','CreateMultipartUpload','UploadPart','CompleteMultipartUpload'}:category='a'
        elif operation in {'HeadObject','GetObject'}:category='b'
        # Cleanup of an SDK-owned incomplete upload is free in R2. Permit it
        # even after the write budget is exhausted; object deletion stays denied.
        elif operation=='AbortMultipartUpload':return
        else:raise CostLimit(f'Unbudgeted R2 operation: {operation}')
        with sqlite3.connect(self.path,timeout=30) as con:
            con.execute('BEGIN IMMEDIATE')
            old=con.execute('SELECT attempts FROM requests WHERE class=?',(category,)).fetchone()
            count=old[0] if old else 0
            if count>=self.policy[f'max_class_{category}_attempts']:raise CostLimit(f'R2 class {category.upper()} request budget reached')
            con.execute('INSERT OR REPLACE INTO requests VALUES (?,?)',(category,count+1))

    def reserve(self,key,size):
        if size<0:raise ValueError('Negative object size')
        with sqlite3.connect(self.path,timeout=30) as con:
            con.execute('BEGIN IMMEDIATE')
            old=con.execute('SELECT bytes FROM reservations WHERE object_key=?',(key,)).fetchone()
            if old:
                if old[0]!=size:raise CostLimit('Changed size for reserved immutable object')
                return
            total=con.execute('SELECT COALESCE(SUM(bytes),0) FROM reservations').fetchone()[0]
            if total+size>self.policy['max_published_bytes']:raise CostLimit('100 GB T/Q publication budget would be exceeded')
            con.execute('INSERT INTO reservations VALUES (?,?)',(key,size))

    def summary(self):
        with sqlite3.connect(self.path) as con:
            return dict(request_attempts=dict(con.execute('SELECT class,attempts FROM requests')),reserved_bytes=con.execute('SELECT COALESCE(SUM(bytes),0) FROM reservations').fetchone()[0],policy=self.policy,scope='this acquisition only; no account-wide billing guarantee')
