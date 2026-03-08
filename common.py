import json
import posixpath
import struct
from pathlib import Path

HEADER_LEN_STRUCT = struct.Struct("!I")
PAYLOAD_LEN_STRUCT = struct.Struct("!Q")


def recvall(sock, n):
    """
    Read exactly n bytes from the socket.
    This avoids incomplete reads, which can happen with TCP.
    """
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed unexpectedly")
        data.extend(chunk)
    return bytes(data)


def send_message(sock, header, payload=b""):
    """
    Send a message in two parts:
    1. JSON header
    2. Raw payload bytes

    Message format:
    - 4 bytes: header length
    - header bytes
    - 8 bytes: payload length
    - payload bytes
    """
    header_bytes = json.dumps(
        header,
        separators=(",", ":"),
        ensure_ascii=False
    ).encode("utf-8")

    sock.sendall(HEADER_LEN_STRUCT.pack(len(header_bytes)))
    sock.sendall(header_bytes)
    sock.sendall(PAYLOAD_LEN_STRUCT.pack(len(payload)))
    if payload:
        sock.sendall(payload)


def recv_message(sock):
    """
    Receive one complete message using the same framing format
    used by send_message().
    """
    header_len = HEADER_LEN_STRUCT.unpack(
        recvall(sock, HEADER_LEN_STRUCT.size)
    )[0]

    header = json.loads(recvall(sock, header_len).decode("utf-8"))

    payload_len = PAYLOAD_LEN_STRUCT.unpack(
        recvall(sock, PAYLOAD_LEN_STRUCT.size)
    )[0]

    payload = recvall(sock, payload_len) if payload_len else b""
    return header, payload


def normalize_requested_path(requested_path):
    """
    Validate the pathname sent by CLIENT.

    Rules:
    - must not be empty
    - must not be absolute
    - must not contain parent traversal like ..
    - must refer to a file path
    """
    raw = str(requested_path).strip().replace("\\", "/")

    if not raw:
        raise ValueError("Pathname cannot be empty")

    if raw.endswith("/"):
        raise ValueError("Pathname must refer to a file, not a directory")

    normalized = posixpath.normpath(raw)

    if normalized in ("", ".", ".."):
        raise ValueError("Invalid pathname")

    if normalized.startswith("/"):
        raise ValueError("Absolute paths are not allowed")

    rel = Path(normalized)

    if ".." in rel.parts:
        raise ValueError("Parent directory references are not allowed")

    return rel.as_posix()


def safe_file_path(base_dir, relative_path):
    """
    Build a safe full file path inside base_dir.
    Prevents escaping outside the intended directory.
    """
    base = Path(base_dir).resolve()
    target = (base / relative_path).resolve()

    try:
        target.relative_to(base)
    except ValueError as exc:
        raise ValueError("Resolved path escaped replica directory") from exc

    return target


def read_replica_file(base_dir, requested_path):
    """
    Read a file from the replica directory if it exists.
    Returns:
    - normalized relative path
    - file bytes or None
    """
    rel_path = normalize_requested_path(requested_path)
    full_path = safe_file_path(base_dir, rel_path)

    if full_path.is_file():
        return rel_path, full_path.read_bytes()

    return rel_path, None


def versioned_relpath(relative_path, label):
    """
    Add a label before the file extension.
    Example:
    sample.txt + SERVER1 -> sample_SERVER1.txt
    """
    p = Path(relative_path)
    return p.with_name(f"{p.stem}_{label}{p.suffix}").as_posix()


def safe_output_path(out_dir, relative_path):
    """
    Build a safe output path inside CLIENT's download directory.
    Also creates parent folders if needed.
    """
    rel_path = normalize_requested_path(relative_path)
    base = Path(out_dir).resolve()
    target = (base / rel_path).resolve()

    try:
        target.relative_to(base)
    except ValueError as exc:
        raise ValueError("Output path escaped output directory") from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    return target
