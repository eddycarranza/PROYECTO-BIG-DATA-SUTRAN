"""Download an Apache archive with resumable retries and a pinned SHA-512."""

import hashlib
import subprocess
import sys
import time
from pathlib import Path


def valid(path, expected):
    if not path.is_file():
        return False
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha512").hexdigest() == expected


def download(url, filename, expected, attempts=20, delay=3):
    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, attempts + 1):
        if valid(path, expected):
            print(f"Descarga verificada: {path.name}", flush=True)
            return
        size = path.stat().st_size if path.exists() else 0
        print(f"{path.name}: intento {attempt}/{attempts}; reanudar desde {size} bytes", flush=True)
        result = subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--show-error",
                "--silent",
                "--connect-timeout",
                "30",
                "--max-time",
                "600",
                "--speed-limit",
                "1024",
                "--speed-time",
                "60",
                "--continue-at",
                "-",
                "--output",
                str(path),
                url,
            ]
        )
        if result.returncode == 0:
            if valid(path, expected):
                print(f"Descarga verificada: {path.name}", flush=True)
                return
            # Discard only this checksum-invalid cached archive, never project data.
            print(f"{path.name}: checksum incorrecto; reiniciar archivo cacheado", flush=True)
            path.unlink(missing_ok=True)
        elif result.returncode in (33, 36):
            raise RuntimeError(
                "El servidor no permite reanudar este archivo; se conserva el parcial."
            )
        else:
            print(f"{path.name}: curl devolvio {result.returncode}; parcial conservado", flush=True)
        if attempt < attempts:
            time.sleep(delay)
    raise RuntimeError(f"Descarga incompleta: {path}; conserva el cache y repite la construccion.")


if __name__ == "__main__":
    download(*sys.argv[1:])
