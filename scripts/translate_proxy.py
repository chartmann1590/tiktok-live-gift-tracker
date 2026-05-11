import socket
import threading
import sys

REMOTE_HOST = sys.argv[1] if len(sys.argv) > 1 else "100.114.139.103"
REMOTE_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 3006
LOCAL_PORT = int(sys.argv[3]) if len(sys.argv) > 3 else 30006
BUFFER_SIZE = 65536


def forward(src, dst):
    try:
        while True:
            data = src.recv(BUFFER_SIZE)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


def handle(client):
    try:
        remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote.settimeout(10)
        remote.connect((REMOTE_HOST, REMOTE_PORT))
        remote.settimeout(None)
        threading.Thread(target=forward, args=(client, remote), daemon=True).start()
        threading.Thread(target=forward, args=(remote, client), daemon=True).start()
    except Exception:
        client.close()


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", LOCAL_PORT))
server.listen(50)
print(f"[translate-proxy] Forwarding 0.0.0.0:{LOCAL_PORT} -> {REMOTE_HOST}:{REMOTE_PORT}")
try:
    while True:
        client, addr = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()
except KeyboardInterrupt:
    server.close()
