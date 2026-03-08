###############################################################################
# server2.py
#
# Role:
#   Acts as SERVER2, the replica file server in the distributed system.
#
# Main responsibilities:
#   1. Receive the pathname request from SERVER1
#   2. Look for the requested file in SERVER2's replica directory
#   3. Return the file when it exists
#   4. Return NOT_FOUND when the file does not exist
#
# Note:
#   The logic below is the same working logic that was tested on the cloud
#   nodes. Only explanatory comments have been added for readability.
###############################################################################

import argparse
import os
import socket
import threading

from common import read_replica_file, recv_message, send_message


def handle_connection(conn, addr, replica_dir):
    """
    Handle one request coming from SERVER1.

    Each incoming connection is processed in a separate thread.
    """
    with conn:
        try:
            header, _ = recv_message(conn)

            # Only REQUEST messages are valid for this server.
            if header.get("type") != "REQUEST":
                send_message(conn, {
                    "status": "ERROR",
                    "message": "Invalid request type sent to SERVER2"
                })
                return

            requested_path = header.get("path", "")

            # Look for the requested file in SERVER2's replica directory.
            rel_path, content = read_replica_file(replica_dir, requested_path)

            if content is None:
                print(f"[SERVER2] NOT FOUND: {rel_path} from {addr}", flush=True)
                send_message(conn, {
                    "status": "NOT_FOUND",
                    "path": rel_path,
                    "message": f"File not found on SERVER2: {rel_path}"
                })
            else:
                print(f"[SERVER2] FOUND: {rel_path} ({len(content)} bytes) from {addr}", flush=True)
                send_message(conn, {
                    "status": "FOUND",
                    "path": rel_path,
                    "size": len(content)
                }, content)

        except ValueError as exc:
            print(f"[SERVER2] Bad request from {addr}: {exc}", flush=True)
            try:
                send_message(conn, {
                    "status": "ERROR",
                    "message": str(exc)
                })
            except Exception:
                pass

        except Exception as exc:
            print(f"[SERVER2] Internal error for {addr}: {exc}", flush=True)
            try:
                send_message(conn, {
                    "status": "ERROR",
                    "message": "Internal SERVER2 error"
                })
            except Exception:
                pass


def main():
    """
    Parse command-line arguments, bind the listening socket, and keep
    serving SERVER1 requests forever.
    """
    parser = argparse.ArgumentParser(description="Replica file server: SERVER2")
    parser.add_argument(
        "--bind-host",
        default=os.getenv("SERVER2_BIND_HOST", "0.0.0.0"),
        help="Host/IP to bind SERVER2 on"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SERVER2_PORT", "8081")),
        help="Port for SERVER2"
    )
    parser.add_argument(
        "--replica-dir",
        default=os.getenv("SERVER2_REPLICA_DIR", "./files2"),
        help="Replica directory for SERVER2"
    )
    args = parser.parse_args()

    os.makedirs(args.replica_dir, exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.bind_host, args.port))
        server.listen(50)

        print(
            f"[SERVER2] Listening on {args.bind_host}:{args.port} "
            f"using replica dir: {os.path.abspath(args.replica_dir)}",
            flush=True
        )

        while True:
            conn, addr = server.accept()
            threading.Thread(
                target=handle_connection,
                args=(conn, addr, args.replica_dir),
                daemon=True
            ).start()


if __name__ == "__main__":
    main()
