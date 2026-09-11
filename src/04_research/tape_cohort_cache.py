"""Owned, content-addressed, locked feature cache with immutable admission."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import uuid

import duckdb
import pyarrow.parquet as pq

import compact_preview_reader as PREVIEW
import compact_product_schema as S
import report_release_inventory as RELEASE
from tape_cohort_outputs import atomic_json, file_identity

OWNER_SCHEMA = "tape_cohort_feature_cache_v1"
GIB = 1024**3
MIB = 1024**2


@dataclass
class CachedMember:
    path: Path
    manifest: dict
    cache_hit: bool


class FeatureCache:
    def __init__(self, root, *, client=None, limit_bytes=GIB, reserve_free_bytes=3*GIB, run_id=None):
        self.root = Path(root).resolve(); self.client = client; self.limit_bytes = int(limit_bytes)
        self.reserve_free_bytes = int(reserve_free_bytes); self.run_id = run_id or uuid.uuid4().hex
        self.lock_handle = self.db = None; self.verified = set()
        self.metrics = {"get_requests": 0, "head_requests": 0, "bytes_transferred": 0,
                        "feature_bytes_transferred": 0, "manifest_bytes_transferred": 0,
                        "cache_hits": 0, "cache_misses": 0}

    def __enter__(self):
        if self.root.exists() and self.root.is_symlink(): raise ValueError("cache root cannot be symlink")
        if self.root.exists() and not (self.root/"cache_owner.json").exists() and any(self.root.iterdir()):
            raise ValueError("refusing to adopt unowned nonempty cache root")
        self.root.mkdir(parents=True, exist_ok=True)
        owner = self.root/"cache_owner.json"
        if not owner.exists(): atomic_json(owner, {"schema": OWNER_SCHEMA, "owner_uuid": str(uuid.uuid4()), "root": str(self.root)})
        marker = json.loads(owner.read_text())
        if marker.get("schema") != OWNER_SCHEMA or marker.get("root") != str(self.root): raise ValueError("cache ownership marker mismatch")
        for name in ("objects", "manifests", "attempts", "temp"): (self.root/name).mkdir(exist_ok=True)
        self.lock_handle = (self.root/"cache.lock").open("a+"); fcntl.flock(self.lock_handle, fcntl.LOCK_EX)
        self.db = duckdb.connect(str(self.root/"catalog.duckdb")); self.db.execute("SET threads=1"); self.db.execute("SET memory_limit='256MB'")
        self.db.execute("CREATE TABLE IF NOT EXISTS source_releases(release_identity VARCHAR PRIMARY KEY, control_hashes_json VARCHAR, complete BOOLEAN, created_at VARCHAR)")
        self.db.execute("CREATE TABLE IF NOT EXISTS source_members(release_identity VARCHAR, partition_identity VARCHAR, session_date VARCHAR, symbol VARCHAR, feature_sha VARCHAR, feature_bytes BIGINT, manifest_sha VARCHAR, support_sha VARCHAR, metadata_json VARCHAR, PRIMARY KEY(release_identity,partition_identity), UNIQUE(release_identity,session_date,symbol))")
        self.db.execute("CREATE TABLE IF NOT EXISTS cached_objects(feature_sha VARCHAR PRIMARY KEY, relative_path VARCHAR, size_bytes BIGINT, admission_hash VARCHAR, verified_at VARCHAR, last_used_at VARCHAR, state VARCHAR)")
        self.db.execute("CREATE TABLE IF NOT EXISTS cache_pins(run_id VARCHAR, feature_sha VARCHAR, PRIMARY KEY(run_id,feature_sha))")
        self.db.execute("CREATE TABLE IF NOT EXISTS query_runs(run_id VARCHAR PRIMARY KEY, query_hash VARCHAR, query_run_hash VARCHAR, source_release_identity VARCHAR, mode VARCHAR, state VARCHAR, result_relative_root VARCHAR, manifest_hash VARCHAR)")
        self.db.execute("DELETE FROM cache_pins")
        return self

    def __exit__(self, *exc):
        if self.db is not None: self.db.close(); self.db = None
        if self.lock_handle is not None:
            fcntl.flock(self.lock_handle, fcntl.LOCK_UN); self.lock_handle.close(); self.lock_handle = None

    def _contained(self, path):
        path = Path(path)
        resolved_parent = path.parent.resolve()
        if self.root != resolved_parent and self.root not in resolved_parent.parents: raise ValueError("cache path escape")
        if path.exists() and path.is_symlink(): raise ValueError("cache symlink escape")
        return path

    def used_bytes(self):
        total = 0
        for base, _, files in os.walk(self.root, followlinks=False):
            for name in files:
                path = Path(base)/name
                if not path.is_symlink(): total += path.stat().st_size
        return total

    def _reserve(self, amount):
        if amount < 0 or amount > 128*MIB: raise ValueError("invalid or oversized reservation")
        self._evict_until(amount)
        used = self.used_bytes(); free = shutil.disk_usage(self.root).free
        if used + amount > self.limit_bytes or free - amount < self.reserve_free_bytes:
            raise OSError(f"cache reservation unavailable: required={amount}, used={used}, free={free}")

    def _evict_until(self, amount):
        while self.used_bytes()+amount > self.limit_bytes:
            row = self.db.execute("SELECT feature_sha,relative_path FROM cached_objects WHERE state='admitted' AND feature_sha NOT IN (SELECT feature_sha FROM cache_pins) ORDER BY last_used_at,feature_sha LIMIT 1").fetchone()
            if row is None: return
            sha, relative = row; path = self._contained(self.root/relative)
            admission = path.parent/"admission.json"
            if path.exists(): path.unlink()
            if admission.exists(): admission.unlink()
            try: path.parent.rmdir()
            except OSError: pass
            self.db.execute("UPDATE cached_objects SET state='missing' WHERE feature_sha=?", [sha])

    def register_release(self, release_manifest, members):
        identity = release_manifest["release_identity"]
        controls = json.dumps({k: release_manifest.get(k) for k in ("accepted_sha256", "reconciliation_sha256", "compatibility_policy")}, sort_keys=True)
        self.db.execute("INSERT OR REPLACE INTO source_releases VALUES (?,?,?,?)", [identity, controls, release_manifest.get("release_status")=="complete", datetime.now(timezone.utc).isoformat()])
        for member in members:
            self.db.execute("DELETE FROM source_members WHERE release_identity=? AND (partition_identity=? OR (session_date=? AND symbol=?))",
                            [identity,member["partition_identity"],member["session_date"],member["symbol"]])
            self.db.execute("INSERT INTO source_members VALUES (?,?,?,?,?,?,?,?,?)", [identity, member["partition_identity"], member["session_date"], member["symbol"], member["objects"]["features"]["sha256"], member["objects"]["features"]["size_bytes"], member["objects"]["manifest"]["sha256"], member["objects"]["support"]["sha256"], json.dumps(member, sort_keys=True)])

    def _remote_client(self):
        if self.client is None:
            import compact_product_storage as STORAGE
            self.client = STORAGE.client(STORAGE.S.load_r2_settings())
        return self.client

    def _get(self, bucket, obj, target, kind):
        from tape_cohort_reliability import retry_download
        return retry_download(lambda: self._get_once(bucket,obj,target,kind), target, Path(target).parent/"retries.jsonl")

    def _get_once(self, bucket, obj, target, kind):
        self._reserve(obj["size_bytes"])
        client = self._remote_client(); stats = PREVIEW.stats_new()
        response = PREVIEW.get_response(client, bucket, obj, stats)
        sha = hashlib.sha256(); size = 0
        with Path(target).open("xb") as out:
            try:
                while block := response["Body"].read(MIB):
                    size += len(block)
                    if size > obj["size_bytes"]: raise ValueError("remote body exceeds identity")
                    sha.update(block); out.write(block)
                out.flush(); os.fsync(out.fileno())
            finally: response["Body"].close()
        self.metrics["get_requests"] += 1; self.metrics["bytes_transferred"] += size
        self.metrics[kind + "_bytes_transferred"] += size
        if size != obj["size_bytes"] or sha.hexdigest() != obj["sha256"]: raise ValueError("remote body SHA/length mismatch")

    def _manifest(self, member, accepted):
        obj = member["objects"]["manifest"]; path = self._contained(self.root/"manifests"/(obj["sha256"]+".json"))
        if path.exists():
            if file_identity(path) != {"sha256": obj["sha256"], "size_bytes": obj["size_bytes"]}: raise ValueError("cached manifest changed")
        else:
            attempt = self.root/"attempts"/uuid.uuid4().hex; attempt.mkdir(); partial = attempt/"manifest.partial"
            try: self._get(member["bucket"], obj, partial, "manifest"); partial.replace(path); atomic_json(attempt/"attempt.json", {"state":"complete", "kind":"manifest", "sha256":obj["sha256"]})
            except Exception:
                atomic_json(attempt/"attempt.json", {"state":"failed", "kind":"manifest", "sha256":obj["sha256"]}); raise
        manifest = json.loads(path.read_text()); RELEASE.manifest_compatibility(manifest, member, accepted)
        return manifest

    @contextmanager
    def acquire(self, member, accepted_calculations):
        if self.db is None: raise RuntimeError("FeatureCache must be entered")
        manifest = self._manifest(member, accepted_calculations); obj = member["objects"]["features"]
        directory = self._contained(self.root/"objects"/obj["sha256"]); path = directory/"features.parquet"; admission = directory/"admission.json"
        hit = path.exists() and admission.exists()
        if hit:
            record = json.loads(admission.read_text())
            expected = {"feature_sha":obj["sha256"], "size_bytes":obj["size_bytes"], "rows":obj["rows"], "object_key":obj["object_key"], "manifest_sha":member["objects"]["manifest"]["sha256"], "schema_hash":S.FEATURE_SCHEMA_HASH, "body_verified":True}
            if any(record.get(k) != v for k,v in expected.items()) or file_identity(path) != {"sha256":obj["sha256"], "size_bytes":obj["size_bytes"]}:
                self.db.execute("UPDATE cached_objects SET state='quarantined' WHERE feature_sha=?", [obj["sha256"]]); raise ValueError("cached feature admission changed")
        else:
            self.metrics["cache_misses"] += 1; directory.mkdir(parents=True, exist_ok=True)
            attempt = self.root/"attempts"/uuid.uuid4().hex; attempt.mkdir(); partial = attempt/"features.partial"
            try:
                self._get(member["bucket"], obj, partial, "feature")
                pf = pq.ParquetFile(partial)
                if pf.metadata.num_rows != obj["rows"] or not pf.schema_arrow.equals(S.FEATURE_SCHEMA, check_metadata=False): raise ValueError("feature physical schema/count mismatch")
                if any(pf.metadata.row_group(i).num_rows > 25000 for i in range(pf.metadata.num_row_groups)): raise ValueError("unbounded feature row group")
                record = {"feature_sha":obj["sha256"], "size_bytes":obj["size_bytes"], "rows":obj["rows"], "object_key":obj["object_key"], "manifest_sha":member["objects"]["manifest"]["sha256"], "schema_hash":S.FEATURE_SCHEMA_HASH, "body_verified":True}
                partial.replace(path); atomic_json(admission, record); atomic_json(attempt/"attempt.json", {"state":"complete", "kind":"feature", "sha256":obj["sha256"]})
                ahash = file_identity(admission)["sha256"]; now = datetime.now(timezone.utc).isoformat()
                self.db.execute("INSERT OR REPLACE INTO cached_objects VALUES (?,?,?,?,?,?,?)", [obj["sha256"], str(path.relative_to(self.root)), obj["size_bytes"], ahash, now, now, "admitted"])
            except Exception:
                atomic_json(attempt/"attempt.json", {"state":"failed", "kind":"feature", "sha256":obj["sha256"]}); raise
        if hit: self.metrics["cache_hits"] += 1
        now = datetime.now(timezone.utc).isoformat(); self.db.execute("INSERT OR REPLACE INTO cache_pins VALUES (?,?)", [self.run_id,obj["sha256"]])
        try: yield CachedMember(path=path, manifest=manifest, cache_hit=hit)
        finally:
            self.db.execute("DELETE FROM cache_pins WHERE run_id=? AND feature_sha=?", [self.run_id,obj["sha256"]])
            self.db.execute("UPDATE cached_objects SET last_used_at=? WHERE feature_sha=?", [now,obj["sha256"]])

    def status(self):
        states = dict(self.db.execute("SELECT state,count(*) FROM cached_objects GROUP BY state").fetchall())
        return {"root":str(self.root), "used_bytes":self.used_bytes(), "limit_bytes":self.limit_bytes,
                "filesystem_free_bytes":shutil.disk_usage(self.root).free, "states":states, "metrics":dict(self.metrics)}

    def reconcile(self, attempt_id, disposition="retain"):
        if not isinstance(attempt_id,str) or not attempt_id or "/" in attempt_id or attempt_id in (".",".."):
            raise ValueError("invalid attempt id")
        path=self._contained(self.root/"attempts"/attempt_id)
        if not path.is_dir(): raise FileNotFoundError(path)
        files=[];total=0
        for item in sorted(path.iterdir()):
            if item.is_symlink() or not item.is_file(): raise ValueError("unexpected attempt content")
            files.append({"name":item.name,**file_identity(item)});total+=item.stat().st_size
        report={"attempt_id":attempt_id,"owned_path":str(path),"bytes":total,"files":files,"disposition":disposition}
        if disposition=="retain": return report
        if disposition=="verify-and-admit":
            raise ValueError("automatic orphan admission is refused without the frozen member binding")
        if disposition!="discard-owned-incomplete": raise ValueError("unknown reconciliation disposition")
        record=json.loads((path/"attempt.json").read_text()) if (path/"attempt.json").exists() else {}
        if record.get("state")!="failed": raise ValueError("only an explicitly failed owned attempt can be discarded")
        for item in path.iterdir(): item.unlink()
        path.rmdir();report["deleted_recoverability"]="remote immutable object remains recoverable";return report
