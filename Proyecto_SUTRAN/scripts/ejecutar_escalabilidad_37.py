"""Coordina las pruebas del pipeline y mide recursos de los contenedores desde el host."""

import json
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from uuid import uuid4

root = Path(__file__).resolve().parents[1]
envdir = root.parent / "Entorno_SUTRAN_HA"
docker = shutil.which("docker")
if not docker:
    raise RuntimeError("Docker no está en PATH. Abrir Docker Desktop y revisar la instalación.")
pointer = json.loads((root / "config/prueba_escalabilidad.json").read_text(encoding="utf-8-sig"))
prepared_out = root / pointer["report_directory"]
prep = json.loads((prepared_out / "preparacion.json").read_text(encoding="utf-8"))
execution = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
out = prepared_out / "executions" / execution
out.mkdir(parents=True, exist_ok=False)
output_relative = str(out.relative_to(root)).replace("\\", "/")
for name in [
    "preparacion.json",
    "manifest_1x.json",
    "manifest_10x.json",
    "manifest_100x.json",
    "limites_entorno.json",
]:
    if (prepared_out / name).exists():
        shutil.copy2(prepared_out / name, out / name)
assert prep["status"] == "prepared" and all(x["verified"] for x in prep["sources"].values())


def call(args, **kwargs):
    return subprocess.run([str(docker)] + args, cwd=envdir, **kwargs)


ids = call(
    ["compose", "ps", "-q", "--status", "running"], capture_output=True, text=True, check=True
).stdout.split()


def memory(v):
    m = re.match(r"([\d.]+)([a-zA-Z]+)", v.strip())
    return (
        float(m[1])
        * {
            "B": 1,
            "kB": 1000,
            "KB": 1000,
            "KiB": 1024,
            "MB": 1e6,
            "MiB": 1024**2,
            "GB": 1e9,
            "GiB": 1024**3,
        }[m[2]]
    )


def sample(factor):
    result = call(
        ["stats", "--no-stream", "--format", "{{json .}}"] + ids,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "factor": factor,
            "error": result.stderr,
        }
    data = [json.loads(x) for x in result.stdout.splitlines() if x.strip()]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "factor": factor,
        "containers": len(data),
        "memory_bytes_sum": sum(memory(x["MemUsage"].split("/")[0]) for x in data),
        "cpu_percent_sum": sum(float(x["CPUPerc"].rstrip("%")) for x in data),
        "per_container": data,
    }


summary = {
    "test_id": pointer["test_id"],
    "execution_id": execution,
    "status": "running",
    "scope": prep["scope"],
    "resource_method": "docker stats of all running lab containers every approximately 5 seconds; memory is sum of Docker-reported working sets, CPU 100% is one logical core; includes infrastructure, excludes host Windows and WSL overhead.",
    "results": [],
}
try:
    for factor in [1, 10, 100]:
        print(f"INICIANDO_PIPELINE {factor}x", flush=True)
        pattern = "part-000.csv" if factor == 1 else "part-00?.csv" if factor == 10 else "*.csv"
        paths = [
            prep["hdfs_root"] + "/raw/" + sid + "/" + pattern for sid in sorted(prep["sources"])
        ]
        du = call(
            [
                "compose",
                "exec",
                "-T",
                "jupyter",
                "hdfs",
                "--config",
                "/tmp/hadoop-conf",
                "dfs",
                "-du",
                "-s",
            ]
            + paths,
            capture_output=True,
            text=True,
            check=True,
        )
        input_bytes = sum(int(line.split()[0]) for line in du.stdout.splitlines() if line.strip())
        output_dir = "/project/" + output_relative + f"/factor_{factor}"
        manifest = (
            "/project/"
            + pointer["report_directory"].replace("\\", "/")
            + f"/manifest_{factor}x.json"
        )
        samples = []
        with (out / f"pipeline_{factor}x.log").open("w", encoding="utf-8") as log:
            start = time.perf_counter()
            proc = subprocess.Popen(
                [
                    str(docker),
                    "compose",
                    "exec",
                    "-T",
                    "jupyter",
                    "spark-submit",
                    "--master",
                    "yarn",
                    "/project/scripts/pipeline_escalabilidad_37.py",
                    "--manifest",
                    manifest,
                    "--output",
                    output_dir,
                ],
                cwd=envdir,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            finished = []

            def finish():
                proc.wait()
                finished.append(time.perf_counter())

            waiter = threading.Thread(target=finish)
            waiter.start()
            while not finished:
                samples.append(sample(factor))
                with (out / f"recursos_{factor}x.jsonl").open("a", encoding="utf-8") as file:
                    file.write(json.dumps(samples[-1]) + "\n")
                if proc.poll() is None:
                    time.sleep(3)
            waiter.join()
            elapsed = finished[0] - start
        assert proc.returncode == 0, f"Pipeline {factor}x falló; revisar su log"
        reports = list((out / f"factor_{factor}").glob("*/resultado_pipeline.json"))
        assert len(reports) == 1
        report = json.loads(reports[0].read_text(encoding="utf-8"))
        assert report["status"] == "success"
        assert report["replicas_verified"] == factor and len(report["scale_checks"]) == 28
        app = report["application_id"]
        yarn = call(
            ["compose", "exec", "-T", "jupyter", "yarn", "application", "-status", app],
            capture_output=True,
            text=True,
            check=True,
        )
        (out / f"yarn_{factor}x.log").write_text(yarn.stdout + "\n" + yarn.stderr, encoding="utf-8")
        assert "SUCCEEDED" in yarn.stdout
        resource = re.search(
            r"Aggregate Resource Allocation\s*:\s*(\d+) MB-seconds, (\d+) vcore-seconds",
            yarn.stdout,
        )
        du = call(
            [
                "compose",
                "exec",
                "-T",
                "jupyter",
                "hdfs",
                "--config",
                "/tmp/hadoop-conf",
                "dfs",
                "-du",
                "-s",
            ]
            + [x["path"] for x in report["outputs"].values()],
            capture_output=True,
            text=True,
            check=True,
        )
        output_bytes = sum(int(line.split()[0]) for line in du.stdout.splitlines() if line.strip())
        good_samples = [x for x in samples if "error" not in x]
        assert good_samples
        item = {
            "factor": factor,
            "input_rows": sum(x["raw_rows"] for x in report["sources"].values()),
            "input_bytes": input_bytes,
            "wall_seconds_including_startup": elapsed,
            "processing_seconds": report["elapsed_seconds"],
            "accepted_rows": sum(x["accepted"] for x in report["sources"].values()),
            "rejected_rows": sum(x["quarantined"] for x in report["sources"].values()),
            "output_bytes": output_bytes,
            "peak_container_memory_bytes": max(x["memory_bytes_sum"] for x in good_samples),
            "mean_cpu_percent_sum": mean(x["cpu_percent_sum"] for x in good_samples),
            "peak_cpu_percent_sum": max(x["cpu_percent_sum"] for x in good_samples),
            "sample_count": len(good_samples),
            "sample_errors": len(samples) - len(good_samples),
            "application_id": app,
            "yarn_mb_seconds": int(resource[1]) if resource else None,
            "yarn_vcore_seconds": int(resource[2]) if resource else None,
            "status": "success",
            "proportionality_verified": True,
        }
        summary["results"].append(item)
        (out / "resultado_escalabilidad.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("FACTOR_OK " + json.dumps(item), flush=True)
    base = summary["results"][0]
    for x in summary["results"]:
        x["time_growth_vs_1x"] = x["processing_seconds"] / base["processing_seconds"]
        x["time_growth_divided_by_volume_growth"] = x["time_growth_vs_1x"] / x["factor"]
    summary["status"] = "success"
except Exception as exc:
    summary["status"] = "failed"
    summary["error"] = str(exc)
    raise
finally:
    (out / "resultado_escalabilidad.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
if summary["status"] == "success":
    hdfs_summary = "/project/" + output_relative + "/resultado_escalabilidad.json"
    call(
        [
            "compose",
            "exec",
            "-T",
            "jupyter",
            "hdfs",
            "--config",
            "/tmp/hadoop-conf",
            "dfs",
            "-put",
            hdfs_summary,
            "hdfs://sutran/sutran/metadata/escalabilidad_" + execution + ".json",
        ],
        check=True,
    )
    (root / "config/ultima_escalabilidad.json").write_text(
        json.dumps(
            {
                "status": "success",
                "test_id": pointer["test_id"],
                "report_relative_path": str(
                    (out / "resultado_escalabilidad.json").relative_to(root)
                ).replace("\\", "/"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("ESCALABILIDAD_OK " + str(out), flush=True)
