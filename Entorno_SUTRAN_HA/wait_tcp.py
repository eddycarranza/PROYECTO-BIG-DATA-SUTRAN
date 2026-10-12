import socket
import sys
import time

host, port = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"Disponible: {host}:{port}")
            break
    except OSError:
        time.sleep(2)
else:
    raise SystemExit(f"No respondió {host}:{port}; revisar logs del servicio.")
