###############################################################################
# client.py
#
# Role:
#   Acts as the CLIENT in the distributed system.
#
# Main responsibilities:
#   1. Send pathname request to SERVER1
#   2. Receive final result from SERVER1
#   3. Save returned file(s) into local output directory
#   4. Display useful output for examiner / user
#
# Assignment requirement met:
#   "CLIENT sends a file request with a pathname to SERVER1."
###############################################################################

import argparse
import socket
import sys

from common import recv_message, safe_output_path, send_message


def save_returned_files(out_dir, files_meta, payload):
    """
    Save one or more returned files from SERVER1.

    files_meta contains metadata such as:
    - relative file path
    - file length
    - file source (SERVER1 / SERVER2 / BOTH)

    payload contains the actual file bytes for all files together.
    This function splits the payload according to lengths in files_meta.
    """
    saved = []
    offset = 0

    for meta in files_meta:
        rel_path = meta["relative_path"]
        length = int(meta["length"])
        source = meta.get("source", "UNKNOWN")

        chunk = payload[offset:offset + length]
        if len(chunk) != length:
            raise ValueError("Response payload is shorter than declared file lengths")

        out_path = safe_output_path(out_dir, rel_path)
        out_path.write_bytes(chunk)

        saved.append((str(out_path), source, length))
        offset += length

    if offset != len(payload):
        raise ValueError("Response payload is longer than declared file lengths")

    return saved


def main():
    """
    Parse command-line arguments, connect to SERVER1, send request,
    receive response, and process the returned result.
    """
    parser = argparse.ArgumentParser(description="CLIENT for distributed file assignment")

    parser.add_argument(
        "pathname",
        help="Requested relative pathname, e.g. sample.txt or docs/a.txt"
    )

    parser.add_argument(
        "--server1-host",
        required=True,
        help="SERVER1 IP/hostname"
    )

    parser.add_argument(
        "--server1-port",
        type=int,
        default=8081,
        help="SERVER1 port"
    )

    parser.add_argument(
        "--out-dir",
        default="./downloads",
        help="Directory where returned file(s) will be saved"
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="Socket timeout in seconds"
    )

    args = parser.parse_args()

    try:
        # Connect to SERVER1
        with socket.create_connection((args.server1_host, args.server1_port), timeout=args.timeout) as client:
            # Send request header to SERVER1
            send_message(client, {
                "type": "REQUEST",
                "path": args.pathname
            })

            # Receive response from SERVER1
            header, payload = recv_message(client)

        status = header.get("status")

        # Case 1:
        # SERVER1 has returned one or more files
        if status == "FILES":
            files_meta = header.get("files", [])
            saved = save_returned_files(args.out_dir, files_meta, payload)

            print(header.get("message", "File(s) returned successfully."))
            for path, source, length in saved:
                print(f"Saved {source} copy ({length} bytes) to: {path}")
            return 0

        # Case 2:
        # File not found on both servers
        if status == "NOT_FOUND":
            print(header.get("message", "File not found."))
            return 1

        # Case 3:
        # Request or server error
        if status == "ERROR":
            print(f"Request failed: {header.get('message', 'Unknown error')}")
            return 2

        # Case 4:
        # Unexpected protocol response
        print(f"Unexpected response from SERVER1: {header}")
        return 3

    except Exception as exc:
        print(f"CLIENT error: {exc}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
