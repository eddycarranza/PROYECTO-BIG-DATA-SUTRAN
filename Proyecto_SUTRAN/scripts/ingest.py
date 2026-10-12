"""Carga batch de originales sin transformaciones. Python 3.10 o posterior."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ZONES = ("raw", "standardized", "curated", "serving", "quarantine", "metadata", "logs")


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False
    ) as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        tmp = Path(f.name)
    tmp.replace(path)


def hdfs_command(binary, *args, check=True):
    result = subprocess.run(
        [binary, "dfs", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    if check and result.returncode:
        raise RuntimeError(f"HDFS {args[0]}: {result.stderr.strip()}")
    return result


def hdfs_digest(binary, path):
    # stdout se consume por bloques; stderr va a archivo para evitar bloqueo.
    with tempfile.TemporaryFile() as errors:
        p = subprocess.Popen([binary, "dfs", "-cat", path], stdout=subprocess.PIPE, stderr=errors)
        h = hashlib.sha256()
        try:
            for block in iter(lambda: p.stdout.read(1024 * 1024), b""):
                h.update(block)
            code = p.wait(timeout=300)
        except BaseException:
            p.kill()
            p.wait()
            raise
        finally:
            p.stdout.close()
        if code:
            errors.seek(0)
            raise RuntimeError(errors.read().decode("utf-8", errors="replace"))
        return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", default=str(Path(__file__).parents[1] / "config" / "sources.json")
    )
    parser.add_argument("--target", choices=("local", "hdfs"), default="local")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    project_root = config_path.parents[1]
    raw_root = (project_root / cfg["local_lake"]).resolve()
    input_root = Path(cfg["input_dir"])
    if not input_root.is_absolute():
        input_root = project_root / input_root
    input_root = input_root.resolve()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    when = datetime.now(timezone(timedelta(hours=-5))).isoformat()
    run = {
        "run_id": run_id,
        "started_at_lima": when,
        "target": args.target,
        "status": "running",
        "records": [],
        "hdfs_executed": args.target == "hdfs",
    }
    for zone in ZONES:
        (raw_root / zone).mkdir(parents=True, exist_ok=True)
    log_path = raw_root / "logs" / f"ingest_{run_id}.json"
    binary = None
    try:
        # Validación de TODAS las entradas antes de comenzar la copia.
        checked = []
        for source in cfg["sources"]:
            path = (input_root / source["original_name"]).resolve()
            if path.parent != input_root or not path.is_file():
                raise ValueError(
                    f"Entrada ausente o fuera de la carpeta: {source['original_name']}"
                )
            actual = digest(path)
            if actual != source["sha256"] or path.stat().st_size != source["bytes"]:
                raise ValueError(f"El original cambió respecto a 3.1: {source['original_name']}")
            rel = (
                Path("raw")
                / f"source={source['slug']}"
                / f"acquired={source['acquisition_date']}"
                / f"version={actual}"
                / source["stored_name"]
            )
            checked.append((source, path, actual, rel))
        if args.target == "hdfs":
            hdfs_root = cfg.get("hdfs_root", "").rstrip("/")
            if not hdfs_root.startswith("hdfs://"):
                raise ValueError(
                    "Configurar hdfs_root con URI hdfs://host:puerto/ruta; no se asume un puerto"
                )
            binary = shutil.which(cfg.get("hdfs_binary", "hdfs"))
            if not binary:
                raise RuntimeError(
                    "No se encontró el cliente hdfs; ejecutar desde el entorno Hadoop del equipo"
                )
            hdfs_command(
                binary, "-ls", hdfs_root.split("/", 3)[0] + "//" + hdfs_root.split("/")[2] + "/"
            )
            for zone in ZONES:
                hdfs_command(binary, "-mkdir", "-p", f"{hdfs_root}/{zone}")
        for source, original, checksum, rel in checked:
            if args.target == "local":
                dest = raw_root / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    if digest(dest) != checksum:
                        raise RuntimeError(
                            f"Conflicto de integridad: {rel.as_posix()}; no se sobrescribe"
                        )
                    action = "already_present_verified"
                else:
                    # La carpeta de versión permite conservar varias versiones.
                    with tempfile.NamedTemporaryFile(
                        dir=dest.parent, suffix=".tmp", delete=False
                    ) as f:
                        tmp = Path(f.name)
                    try:
                        shutil.copyfile(original, tmp)
                        if digest(tmp) != checksum:
                            raise RuntimeError(f"Copia incompleta: {source['id']}")
                        # No reemplazar una versión que haya aparecido durante la copia.
                        if dest.exists():
                            if digest(dest) != checksum:
                                raise RuntimeError(f"Conflicto concurrente: {source['id']}")
                            action = "already_present_verified"
                        else:
                            # Enlace exclusivo: publica el archivo completo sin sobrescribir.
                            os.link(tmp, dest)
                            action = "copied_and_verified"
                    finally:
                        tmp.unlink(missing_ok=True)
                stored_hash = digest(dest)
                stored_bytes = dest.stat().st_size
                destination = str(dest)
            else:
                destination = f"{hdfs_root}/{rel.as_posix()}"
                present = hdfs_command(binary, "-test", "-e", destination, check=False)
                if present.returncode == 0:
                    stored_hash = hdfs_digest(binary, destination)
                    if stored_hash != checksum:
                        raise RuntimeError(f"Conflicto HDFS: {source['id']}; no se sobrescribe")
                    action = "already_present_verified"
                elif present.returncode == 1 and not present.stderr.strip():
                    hdfs_command(binary, "-mkdir", "-p", destination.rsplit("/", 1)[0])
                    # Ruta temporal ASCII para evitar diferencias de codificación
                    # del cliente Java; el contenido original se conserva y verifica.
                    with tempfile.TemporaryDirectory(prefix="sutran_ingest_") as tmp_dir:
                        staged = Path(tmp_dir) / source["stored_name"]
                        shutil.copyfile(original, staged)
                        if digest(staged) != checksum:
                            raise RuntimeError(f"Copia temporal incompleta: {source['id']}")
                        hdfs_command(binary, "-put", str(staged), destination)
                    stored_hash = hdfs_digest(binary, destination)
                    action = "copied_and_verified"
                else:
                    raise RuntimeError(
                        f"No se pudo verificar destino HDFS: {present.stderr.strip()}"
                    )
                stored_bytes = int(hdfs_command(binary, "-stat", "%b", destination).stdout.strip())
            if stored_hash != checksum or stored_bytes != source["bytes"]:
                raise RuntimeError(f"Integridad no confirmada: {source['id']}")
            run["records"].append(
                {
                    **source,
                    "relative_path": rel.as_posix(),
                    "destination": destination,
                    "action": action,
                    "stored_bytes": stored_bytes,
                    "stored_sha256": stored_hash,
                    "integrity_verified": True,
                }
            )
            atomic_json(log_path, run)
        run["status"] = "success"
        run["files_verified"] = len(run["records"])
        run["total_bytes"] = sum(s["bytes"] for s in run["records"])
        run["finished_at_lima"] = datetime.now(timezone(timedelta(hours=-5))).isoformat()
        atomic_json(log_path, run)
        manifest_path = raw_root / "metadata" / f"manifest_{run_id}.json"
        atomic_json(manifest_path, run)
        if args.target == "hdfs":
            # Cada ejecución tiene un manifiesto único; no se sobreescriben históricos.
            hdfs_command(
                binary, "-put", str(manifest_path), f"{hdfs_root}/metadata/{manifest_path.name}"
            )
            hdfs_command(binary, "-put", str(log_path), f"{hdfs_root}/logs/{log_path.name}")
        print(
            json.dumps(
                {
                    "status": run["status"],
                    "target": args.target,
                    "files_verified": run["files_verified"],
                    "bytes": run["total_bytes"],
                    "manifest": str(manifest_path),
                    "log": str(log_path),
                },
                ensure_ascii=False,
            )
        )
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
        atomic_json(log_path, run)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
