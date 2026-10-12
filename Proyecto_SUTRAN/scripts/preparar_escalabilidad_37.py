"""Prepara réplicas físicas 1x, 10x y 100x para medir el pipeline."""

import csv
import hashlib
import importlib.util
import json
import re
import subprocess
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import openpyxl

p = Path("/project")
manifests = []
for f in (p / "data_lake/metadata").glob("manifest_*.json"):
    data = json.loads(f.read_text(encoding="utf-8"))
    if data.get("status") == "success" and data.get("target") == "hdfs":
        manifests.append((f, data))
_, manifest = max(manifests, key=lambda x: x[0].stat().st_mtime)
sources = {x["id"]: x for x in manifest["records"]}
run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = p / "documentation/escalabilidad_37" / run
out.mkdir(parents=True, exist_ok=False)
root = f"hdfs://sutran/sutran/benchmark_37/test={run}"


def hdfs(*args):
    subprocess.run(["hdfs", "--config", "/tmp/hadoop-conf", "dfs", *args], check=True)


def header(v):
    v = unicodedata.normalize("NFKD", str(v or ""))
    v = "".join(x for x in v if not unicodedata.combining(x))
    return re.sub("[^a-z0-9]+", "_", v.lower()).strip("_")


spec = importlib.util.spec_from_file_location("audit", "/project/scripts/revisar_integracion_33.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
plan = {
    "test_id": run,
    "hdfs_root": root,
    "factors": [1, 10, 100],
    "sources": {},
    "scope": "Preparation converts XLSX and DBF once; measured pipeline reads replicated CSV representations of all seven sources.",
}
with tempfile.TemporaryDirectory(prefix="sutran_scaling_") as tmp:
    tmp = Path(tmp)
    for sid, src in sorted(sources.items()):
        print("PREPARANDO " + sid, flush=True)
        original = tmp / src["stored_name"]
        hdfs("-get", src["destination"], str(original))
        assert hashlib.file_digest(original.open("rb"), "sha256").hexdigest() == src["sha256"]
        if sid == "F05":
            records = [
                {header(k): str(v) if v is not None else "" for k, v in row.items()}
                for row in audit.dbf_rows(original)
            ]
            keys = list(records[0])
            rows = [[x[k] for k in keys] for x in records]
        elif sid == "F06":
            book = openpyxl.load_workbook(original, read_only=True, data_only=True)
            assert len(book.worksheets) == 1
            iterator = book.worksheets[0].iter_rows(values_only=True)
            keys = [header(x) for x in next(iterator)]
            rows = [
                [
                    v.strftime("%Y%m%d")
                    if isinstance(v, datetime)
                    else str(v)
                    if v is not None
                    else ""
                    for v in row
                ]
                for row in iterator
                if any(v is not None for v in row)
            ]
            book.close()
        else:
            with original.open(encoding="cp1252", newline="") as f:
                reader = csv.reader(f, delimiter=";")
                keys = [header(c) or f"extra_{i}" for i, c in enumerate(next(reader))]
                rows = list(reader)
        assert len(set(keys)) == len(keys)
        assert all(len(row) == len(keys) for row in rows)
        if src.get("records_before_cleaning") is not None:
            assert len(rows) == src["records_before_cleaning"]
        plan["sources"][sid] = {"base_rows": len(rows), "original_sha256": src["sha256"]}
        parts = tmp / sid
        parts.mkdir()
        for replica in range(100):
            with (parts / f"part-{replica:03d}.csv").open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, delimiter=";", lineterminator="\n")
                writer.writerow(keys + ["replica_id"])
                writer.writerows(row + [replica] for row in rows)
        destination = f"{root}/raw/{sid}"
        hdfs("-mkdir", "-p", f"{root}/raw")
        hdfs("-put", str(parts), destination)
        local_sha = hashlib.sha256()
        size = 0
        for f in sorted(parts.glob("*.csv")):
            size += f.stat().st_size
            with f.open("rb") as stream:
                while data := stream.read(1024 * 1024):
                    local_sha.update(data)
        remote_sha = hashlib.sha256()
        proc = subprocess.Popen(
            ["hdfs", "--config", "/tmp/hadoop-conf", "dfs", "-cat", destination + "/*.csv"],
            stdout=subprocess.PIPE,
        )
        while data := proc.stdout.read(1024 * 1024):
            remote_sha.update(data)
        assert proc.wait() == 0 and local_sha.hexdigest() == remote_sha.hexdigest()
        plan["sources"][sid].update(
            all_100_bytes=size, replicated_sha256=local_sha.hexdigest(), verified=True
        )
        for f in parts.glob("*.csv"):
            f.unlink()
        parts.rmdir()
        original.unlink()
    for factor in plan["factors"]:
        test_manifest = dict(manifest)
        test_manifest["benchmark"] = {"factor": factor, "root": root, "test_id": run}
        test_manifest["records"] = []
        for sid, src in sorted(sources.items()):
            entry = dict(src)
            entry["benchmark_paths"] = [f"{root}/raw/{sid}/part-{i:03d}.csv" for i in range(factor)]
            entry["records_before_cleaning"] = plan["sources"][sid]["base_rows"] * factor
            entry["replicated_integrity_verified"] = plan["sources"][sid]["verified"]
            test_manifest["records"].append(entry)
        (out / f"manifest_{factor}x.json").write_text(
            json.dumps(test_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
plan["status"] = "prepared"
(out / "preparacion.json").write_text(
    json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8"
)
(p / "config/prueba_escalabilidad.json").write_text(
    json.dumps({"test_id": run, "report_directory": str(out.relative_to(p))}, indent=2),
    encoding="utf-8",
)
print("PREPARACION_OK " + str(out), flush=True)
