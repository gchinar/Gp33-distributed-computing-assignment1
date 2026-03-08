###############################################################################
# server1.py
#
# Role:
#   Acts as SERVER1, the coordinator server in the distributed system.
#
# Main responsibilities:
#   1. Receive the pathname request from CLIENT
#   2. Check whether the requested file exists in SERVER1's replica directory
#   3. Ask SERVER2 for the same file
#   4. Compare both copies when both are available
#   5. Return the correct result back to CLIENT
#
# Note:
#   The logic below is the same working logic that was tested on the cloud
#   nodes. Only explanatory comments have been added for readability.
###############################################################################

import argparse
import os
import socket
import threading

from common import (
    read_replica_file,
    recv_message,
    send_message,
    versioned_relpath,
)


def fetch_from_server2(server2_host, server2_port, timeout, requested_path):
    """
    Open a short-lived connection to SERVER2, forward the same pathname,
    and return SERVER2's response header and payload.
    """
    with socket.create_connection((server2_host, server2_port), timeout=timeout) as s2:
        send_message(s2, {
            "type": "REQUEST",
            "path": requested_path
        })
        return recv_message(s2)


def send_files_response(conn, request_path, message, files_meta, payload):
    """
    Send a FILES response back to CLIENT.

    files_meta tells the CLIENT how many files are being returned,
    their names, lengths, and the server source.
    """
    send_message(conn, {
        "status": "FILES",
        "request_path": request_path,
        "message": message,
        "files": files_meta
    }, payload)


def handle_client(conn, addr, replica_dir, server2_host, server2_port, timeout):
    """
    Handle one CLIENT connection.

    Each accepted connection is processed in its own thread so that SERVER1
    can serve more than one client request over time.
    """
    with conn:
        try:
            header, _ = recv_message(conn)

            # Only REQUEST messages are valid in this assignment.
            if header.get("type") != "REQUEST":
                send_message(conn, {
                    "status": "ERROR",
                    "message": "Invalid request type sent to SERVER1"
                })
                return

            requested_path = header.get("path", "")

            # Step 1: Check SERVER1's local replica directory.
            rel_path, file1_bytes = read_replica_file(replica_dir, requested_path)

            if file1_bytes is None:
                print(f"[SERVER1] Local NOT FOUND: {rel_path}", flush=True)
            else:
                print(f"[SERVER1] Local FOUND: {rel_path} ({len(file1_bytes)} bytes)", flush=True)

            # Step 2: Ask SERVER2 for the same normalized pathname.
            file2_bytes = None
            server2_note = None

            try:
                s2_header, s2_payload = fetch_from_server2(
                    server2_host,
                    server2_port,
                    timeout,
                    rel_path
                )

                s2_status = s2_header.get("status")

                if s2_status == "FOUND":
                    file2_bytes = s2_payload
                    print(f"[SERVER1] SERVER2 FOUND: {rel_path} ({len(file2_bytes)} bytes)", flush=True)
                elif s2_status == "NOT_FOUND":
                    print(f"[SERVER1] SERVER2 NOT FOUND: {rel_path}", flush=True)
                elif s2_status == "ERROR":
                    server2_note = s2_header.get("message", "SERVER2 returned an error")
                    print(f"[SERVER1] SERVER2 ERROR: {server2_note}", flush=True)
                else:
                    server2_note = f"Unexpected SERVER2 response: {s2_status}"
                    print(f"[SERVER1] {server2_note}", flush=True)

            except Exception as exc:
                # Even if SERVER2 is unavailable, SERVER1 may still be able
                # to serve its own local copy.
                server2_note = f"SERVER2 unavailable: {exc}"
                print(f"[SERVER1] {server2_note}", flush=True)

            # Step 3: Decide what result must be returned to CLIENT.

            # Case A: Both SERVER1 and SERVER2 have the file.
            if file1_bytes is not None and file2_bytes is not None:
                # If both copies are identical, return only one copy.
                if file1_bytes == file2_bytes:
                    message = "Identical file found on both servers. SERVER1 returned one copy."
                    files_meta = [{
                        "relative_path": rel_path,
                        "length": len(file1_bytes),
                        "source": "BOTH"
                    }]
                    send_files_response(conn, rel_path, message, files_meta, file1_bytes)
                    return

                # If both copies exist but differ, return both copies with
                # clear server-specific names.
                message = "Replicas differ. SERVER1 returned both files."
                rel1 = versioned_relpath(rel_path, "SERVER1")
                rel2 = versioned_relpath(rel_path, "SERVER2")

                files_meta = [
                    {
                        "relative_path": rel1,
                        "length": len(file1_bytes),
                        "source": "SERVER1"
                    },
                    {
                        "relative_path": rel2,
                        "length": len(file2_bytes),
                        "source": "SERVER2"
                    }
                ]

                payload = file1_bytes + file2_bytes
                send_files_response(conn, rel_path, message, files_meta, payload)
                return

            # Case B: Only SERVER1 has the file.
            if file1_bytes is not None:
                message = "File found only on SERVER1."
                if server2_note:
                    message += f" Note: {server2_note}"

                files_meta = [{
                    "relative_path": rel_path,
                    "length": len(file1_bytes),
                    "source": "SERVER1"
                }]
                send_files_response(conn, rel_path, message, files_meta, file1_bytes)
                return

            # Case C: Only SERVER2 has the file.
            if file2_bytes is not None:
                message = "File found only on SERVER2 and forwarded by SERVER1."
                files_meta = [{
                    "relative_path": rel_path,
                    "length": len(file2_bytes),
                    "source": "SERVER2"
                }]
                send_files_response(conn, rel_path, message, files_meta, file2_bytes)
                return

            # Case D: Neither server can provide the file.
            if server2_note:
                message = f"File not found on SERVER1, and SERVER2 could not provide it. {server2_note}"
            else:
                message = f"File not found on both servers: {rel_path}"

            send_message(conn, {
                "status": "NOT_FOUND",
                "request_path": rel_path,
                "message": message
            })

        except ValueError as exc:
            print(f"[SERVER1] Bad client request from {addr}: {exc}", flush=True)
            try:
                send_message(conn, {
                    "status": "ERROR",
                    "message": str(exc)
                })
            except Exception:
                pass

        except Exception as exc:
            print(f"[SERVER1] Internal error for {addr}: {exc}", flush=True)
            try:
                send_message(conn, {
                    "status": "ERROR",
                    "message": "Internal SERVER1 error"
                })
            except Exception:
                pass


def main():
    """
    Parse command-line arguments, bind the listening socket, and keep
    accepting CLIENT connections forever.
    """
    parser = argparse.ArgumentParser(description="Coordinator file server: SERVER1")
    parser.add_argument(
        "--bind-host",
        default=os.getenv("SERVER1_BIND_HOST", "0.0.0.0"),
        help="Host/IP to bind SERVER1 on"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SERVER1_PORT", "8081")),
        help="Port for SERVER1"
    )
    parser.add_argument(
        "--replica-dir",
        default=os.getenv("SERVER1_REPLICA_DIR", "./files1"),
        help="Replica directory for SERVER1"
    )
    parser.add_argument(
        "--server2-host",
        default=os.getenv("SERVER2_HOST", "127.0.0.1"),
        help="SERVER2 IP/hostname"
    )
    parser.add_argument(
        "--server2-port",
        type=int,
        default=int(os.getenv("SERVER2_PORT", "8081")),
        help="SERVER2 port"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.getenv("SERVER2_TIMEOUT", "5")),
        help="Timeout while contacting SERVER2"
    )
    args = parser.parse_args()

    os.makedirs(args.replica_dir, exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.bind_host, args.port))
        server.listen(50)

        print(
            f"[SERVER1] Listening on {args.bind_host}:{args.port} "
            f"using replica dir: {os.path.abspath(args.replica_dir)}",
            flush=True
        )
        print(
            f"[SERVER1] SERVER2 configured as {args.server2_host}:{args.server2_port}",
            flush=True
        )

        while True:
            conn, addr = server.accept()
            threading.Thread(
                target=handle_client,
                args=(
                    conn,
                    addr,
                    args.replica_dir,
                    args.server2_host,
                    args.server2_port,
                    args.timeout
                ),
                daemon=True
            ).start()


if __name__ == "__main__":
    main()
