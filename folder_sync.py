#!/usr/bin/env python3
"""
folder_sync.py – LAN folder synchroniser (manual roles, verbose)
===============================================================
* **Explicit** `--mode receiver` (server) or `--mode sender` (client) — no
  auto‑election.
* Added **verbose status messages**:
  - Client prints when searching for a server.
  - Both sides show a **tqdm** progress bar while hashing/transferring files.
* Sync direction is **one‑way**: client → server.
* Exchanges file metadata (mtime + size) before hashing anything, and only
  hashes files whose mtime/size actually suggest a change — unchanged files
  are never re-hashed, which makes repeat passes (--interval) much cheaper.
* Both sides hash concurrently; `--parallel-hash` also multithreads hashing.
* Paths received from the peer are validated so they cannot escape the
  synced folder.
* `--exclude` skips noisy/irrelevant folders (.git, node_modules, etc).
* Prints a summary (files, bytes, time, throughput) at the end of each pass.
"""

import argparse
import fnmatch
import json
import os
import socket
import struct
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Set

from tqdm import tqdm
from blake3 import blake3      # pip install blake3

# ───────────────────────────── tunables ─────────────────────────────
CHUNK = 1_048_576
TCP_PORT_DEFAULT = 9000
DISC_PORT = 9001
HELLO_PREFIX = b"SYNC_HELLO "
LISTEN_BACKLOG = 5

# Folder/file names skipped during scanning by default (fnmatch patterns,
# matched against individual path components). Extend with --exclude.
DEFAULT_EXCLUDES = {
    ".git", "__pycache__", "node_modules", ".DS_Store",
    "$RECYCLE.BIN", "System Volume Information",
}
EXCLUDES: Set[str] = set(DEFAULT_EXCLUDES)

# ───────────────────────── util helpers ─────────────────────────────

def blake3_file(path: Path) -> str:
    """Return BLAKE3 hash of a file."""
    h = blake3()
    with path.open('rb') as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()

def _excluded(name: str) -> bool:
    return any(fnmatch.fnmatch(name, pat) for pat in EXCLUDES)


def iter_files(root: Path):
    """
    Yield every file under root, skipping directories/files that raise
    permission or other OS errors instead of aborting the whole walk, and
    anything matching an EXCLUDES pattern.
    """
    def on_walk_error(err: OSError):
        print(f"[warn] cannot list {getattr(err, 'filename', root)}: {err}")

    for dirpath, dirnames, filenames in os.walk(root, onerror=on_walk_error):
        dirnames[:] = [d for d in dirnames if not _excluded(d)]
        for name in filenames:
            if _excluded(name):
                continue
            p = Path(dirpath) / name
            try:
                if p.is_file():
                    yield p
            except OSError as e:
                print(f"[warn] skipping {p}: {e}")


def scan_meta(root: Path) -> Dict[str, Tuple[float, int]]:
    """
    Return mapping {relpath: (mtime, size)} for all files under root.
    Cheap metadata used to decide *which* files might need syncing, before
    any (expensive) hashing happens. Inaccessible files are skipped.
    """
    out: Dict[str, Tuple[float, int]] = {}
    for p in iter_files(root):
        rel = p.relative_to(root).as_posix()
        try:
            st = p.stat()
            out[rel] = (st.st_mtime, st.st_size)
        except OSError as e:
            print(f"[warn] skipping {rel}: {e}")
    return out


def hash_files(root: Path, rels: List[str], show_progress: bool = False,
               parallel: bool = False, max_workers: int = 8) -> Dict[str, str]:
    """
    Hash exactly the given relative paths under root (no tree scan) and
    return {relpath: blake3 hash}. Files that can't be read are skipped
    with a warning instead of aborting the batch.
    """
    bar = tqdm(total=len(rels), desc=f"Hashing {root}", unit="file") if show_progress else None

    def hash_one(rel: str):
        try:
            return rel, blake3_file(root / rel)
        except OSError as e:
            print(f"[warn] skipping {rel}: {e}")
            return rel, None

    out: Dict[str, str] = {}
    if parallel and rels:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(hash_one, rel) for rel in rels]
            for future in as_completed(futures):
                rel, h = future.result()
                if h is not None:
                    out[rel] = h
                if bar:
                    bar.update(1)
    else:
        for rel in rels:
            rel, h = hash_one(rel)
            if h is not None:
                out[rel] = h
            if bar:
                bar.update(1)
    if bar:
        bar.close()
    return out


def is_safe_rel(root: Path, rel) -> bool:
    """
    True if `rel` (a path received from the peer) is a relative path that
    stays inside `root`. Rejects absolute paths, drive letters and `..`.
    """
    if not isinstance(rel, str) or not rel or "\0" in rel:
        return False
    if Path(rel).is_absolute() or Path(rel).drive or rel.startswith(("/", "\\")):
        return False
    try:
        (root / rel).resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return False
    return True


def ensure_dir(path: Path):
    """Ensure a directory exists."""
    path.mkdir(parents=True, exist_ok=True)

# ───────────────────── discovery helper (server) ────────────────────

def hello_responder(server_id: int, tcp_port: int):
    """
    Respond to UDP discovery packets from clients, advertising server ID and port.
    """
    pkt = HELLO_PREFIX + json.dumps({"id": server_id, "port": tcp_port}).encode()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", DISC_PORT))
        while True:
            try:
                data, addr = s.recvfrom(1024)
            except Exception:
                continue
            if data.startswith(HELLO_PREFIX):
                s.sendto(pkt, addr)

# ───────────────────────── framing helpers ─────────────────────────

def send_json(sock: socket.socket, obj):
    """Send a JSON object over a socket, with length prefix."""
    blob = json.dumps(obj).encode()
    sock.sendall(struct.pack("!I", len(blob)) + blob)


def recv_json(sock: socket.socket):
    """Receive a JSON object from a socket, with length prefix."""
    hdr = recv_exact(sock, 4)
    if not hdr:
        raise ConnectionError("peer closed")
    (ln,) = struct.unpack("!I", hdr)
    return json.loads(recv_exact(sock, ln))


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from a socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed early")
        buf.extend(chunk)
    return bytes(buf)

# ────────────────────────── server side ────────────────────────────

def serve(root: Path, port: int, delete: bool):
    """
    Main server loop: listen for clients, spawn handler threads for each connection.
    """
    root = root.resolve()
    server_id = uuid.uuid4().int >> 64
    threading.Thread(target=hello_responder, args=(server_id, port), daemon=True).start()
    print(f"[server] UUID={server_id} dir={root} port={port}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", port))
        srv.listen(LISTEN_BACKLOG)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=handle_client, args=(conn, addr, root, delete), daemon=True).start()


def handle_client(conn: socket.socket, addr, root: Path, delete: bool):
    print(f"[server] ⇄ session with {addr[0]}:{addr[1]}")
    t0 = time.monotonic()
    stats = {"files": 0, "bytes": 0}
    with conn:
        try:
            # 1. Exchange cheap metadata (mtime + size) before hashing anything
            server_meta = scan_meta(root)
            send_json(conn, {"meta": server_meta})
            client_msg = recv_json(conn)
            client_meta = {}
            for rel, meta in client_msg.get("meta", {}).items():
                if is_safe_rel(root, rel):
                    client_meta[rel] = meta
                else:
                    print(f"[server] rejecting unsafe path from {addr[0]}: {rel!r}")

            # 2. From metadata alone, split candidates into files that are
            # definitely new/changed (direct transfer, no hash needed) and
            # files whose mtime moved but size didn't, which need a hash to
            # tell whether the content actually changed (verify).
            skip_date_check = getattr(handle_client, "skip_date_check", False)
            direct_transfer: Set[str] = set()
            verify: Set[str] = set()
            for rel, (client_mtime, client_size) in client_meta.items():
                if rel not in server_meta:
                    direct_transfer.add(rel)  # only exists on client
                    continue
                server_mtime, server_size = server_meta[rel]
                if not skip_date_check and client_mtime <= server_mtime:
                    continue  # server copy is same age or newer, keep it
                if client_size != server_size:
                    direct_transfer.add(rel)
                else:
                    verify.add(rel)

            # 3. Hash only the "verify" candidates, in parallel with the client
            use_parallel = getattr(handle_client, "use_parallel_hashing", False)
            srv_hashes_holder = {}
            def hash_server():
                nonlocal srv_hashes_holder
                srv_hashes_holder = hash_files(root, sorted(verify), show_progress=bool(verify), parallel=use_parallel)

            hash_thread = threading.Thread(target=hash_server)
            hash_thread.start()

            send_json(conn, {"verify": sorted(verify)})
            client_hashes: Dict[str, str] = recv_json(conn).get("hashes", {})

            hash_thread.join()
            srv_hashes = srv_hashes_holder

            need_from_client = sorted(direct_transfer) + [
                rel for rel in sorted(verify) if srv_hashes.get(rel) != client_hashes.get(rel)
            ]
            send_json(conn, {"need_from_client": need_from_client})
            if need_from_client:
                print(f"[server] receiving {len(need_from_client)} file(s) from {addr[0]} …")
            else:
                print(f"[server] {addr[0]} already up to date, nothing to receive.")
            # Receive and write files from client
            for rel in need_from_client:
                hdr = recv_json(conn)
                if hdr.get("error"):
                    print(f"[server] client could not read {rel}, skipping: {hdr['error']}")
                    continue
                size = hdr["size"]
                mtime = hdr.get("mtime")
                ctime = hdr.get("ctime")
                dst = root / rel
                # Try to open the destination; if denied, still drain the
                # incoming bytes below so the connection stays in sync.
                f = None
                try:
                    ensure_dir(dst.parent)
                    f = dst.open("wb")
                except OSError as e:
                    print(f"[server] cannot write {rel}, discarding incoming data: {e}")

                bar = tqdm(total=size, unit="B", unit_scale=True, desc=f"↧ {rel}")
                rem = size
                while rem:
                    chunk = conn.recv(min(CHUNK, rem))
                    if not chunk:
                        raise ConnectionError("client lost")
                    if f is not None:
                        f.write(chunk)
                    bar.update(len(chunk))
                    rem -= len(chunk)
                bar.close()
                if f is not None:
                    f.close()
                else:
                    continue
                stats["files"] += 1
                stats["bytes"] += size
                # Set file timestamps after writing
                try:
                    if mtime is not None:
                        import os
                        os.utime(dst, (mtime, mtime))
                    if ctime is not None:
                        # Set creation time on Windows
                        import platform
                        if platform.system() == "Windows":
                            try:
                                import ctypes
                                from ctypes import wintypes
                                FILE_WRITE_ATTRIBUTES = 0x100
                                handle = ctypes.windll.kernel32.CreateFileW(
                                    str(dst), FILE_WRITE_ATTRIBUTES, 0, None, 3, 0x80, None)
                                if handle != -1:
                                    ctime_int = int(ctime)
                                    ctime_filetime = int((ctime_int + 11644473600) * 10000000)
                                    class FILETIME(ctypes.Structure):
                                        _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]
                                    ft = FILETIME()
                                    ft.dwLowDateTime = ctime_filetime & 0xFFFFFFFF
                                    ft.dwHighDateTime = ctime_filetime >> 32
                                    ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ft), None, None)
                                    ctypes.windll.kernel32.CloseHandle(handle)
                            except Exception:
                                pass
                except Exception:
                    pass
            # Optionally delete server files not present on client
            if delete:
                client_set = set(client_meta)
                for rel in server_meta:
                    if rel not in client_set:
                        try:
                            (root / rel).unlink()
                        except Exception:
                            pass
            send_json(conn, {"done": True})
            elapsed = time.monotonic() - t0
            speed = stats["bytes"] / elapsed / 1_048_576 if elapsed > 0 else 0.0
            print(f"[server] session with {addr[0]} done: {stats['files']} file(s), "
                  f"{stats['bytes'] / 1_048_576:.1f} MB in {elapsed:.1f}s ({speed:.1f} MB/s)")
        except Exception as e:
            print(f"[server] error: {e}")

# ────────────────────────── client side ────────────────────────────

def client_loop(root: Path, host: str, port: int, interval: int):
    root = root.resolve()
    while True:
        try:
            client_pass(root, host, port)
        except Exception as e:
            print(f"[client] error: {e}")
        if interval <= 0:
            break
        time.sleep(interval)


def client_pass(root: Path, host: str, port: int):
    t0 = time.monotonic()
    stats = {"files": 0, "bytes": 0}
    # 1. Exchange cheap metadata (mtime + size) before hashing anything
    print(f"[client] Scanning {root} …")
    client_meta = scan_meta(root)
    print(f"[client] Connecting to {host}:{port} …")
    with socket.create_connection((host, port), timeout=None) as sock:
        recv_json(sock)  # server's metadata; server does the diffing, we don't need it
        send_json(sock, {"meta": client_meta})

        # 2. Hash only the candidate files the server asks us to verify
        use_parallel = getattr(client_pass, "use_parallel_hashing", False)
        # Only ever touch files we advertised ourselves, so a malicious
        # server can't make us read or upload anything outside root.
        verify_list = [r for r in recv_json(sock).get("verify", []) if r in client_meta]
        if verify_list:
            print(f"[client] Verifying {len(verify_list)} candidate file(s) …")
        hashes = hash_files(root, verify_list, show_progress=bool(verify_list), parallel=use_parallel)
        send_json(sock, {"hashes": hashes})

        reply = recv_json(sock)
        up_list = reply.get("need_from_client", [])
        if up_list:
            print(f"[client] Sending {len(up_list)} file(s) …")
        else:
            print("[client] Nothing to send, already up to date.")
        # Send files requested by server
        for rel in up_list:
            if rel not in client_meta:
                print(f"[client] server requested unknown path, refusing: {rel!r}")
                send_json(sock, {"path": rel, "error": "refused"})
                continue
            src = root / rel
            # Open before committing to a header so an access-denied/locked
            # file can be skipped without desyncing the wire protocol.
            try:
                st = src.stat()
                f = src.open("rb")
            except OSError as e:
                print(f"[client] cannot read {rel}, skipping: {e}")
                send_json(sock, {"path": rel, "error": str(e)})
                continue

            size = st.st_size
            # Send file metadata: path, size, modified, created
            send_json(sock, {
                "path": rel,
                "size": size,
                "mtime": st.st_mtime,
                "ctime": st.st_ctime
            })
            bar = tqdm(total=size, unit="B", unit_scale=True, desc=f"↥ {rel}")
            with f:
                while chunk := f.read(CHUNK):
                    sock.sendall(chunk)
                    bar.update(len(chunk))
            bar.close()
            stats["files"] += 1
            stats["bytes"] += size
        send_json(sock, {"done": True})
    elapsed = time.monotonic() - t0
    speed = stats["bytes"] / elapsed / 1_048_576 if elapsed > 0 else 0.0
    print(f"[client] pass done: {stats['files']} file(s), "
          f"{stats['bytes'] / 1_048_576:.1f} MB in {elapsed:.1f}s ({speed:.1f} MB/s)")

# ───────────────────────── discover (fixed client) ─────────────────────────

def discover_server(timeout: float = 3.0) -> str:
    """
    Broadcast UDP packets to discover a sync server on the LAN.
    Returns the IP address of the first server found.
    """
    print("[client] Looking for a server …")
    pkt = HELLO_PREFIX + b"DISC"
    infos = socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET)
    iface_ips = [info[-1][0] for info in infos]
    for attempt in range(5, 0, -1):
        for ip in iface_ips:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.settimeout(timeout)
                try:
                    s.bind((ip, 0))
                    s.sendto(pkt, ("255.255.255.255", DISC_PORT))
                    data, addr = s.recvfrom(1024)
                    if data.startswith(HELLO_PREFIX):
                        print(f"[client] Found server at {addr[0]}")
                        return addr[0]
                except (OSError, socket.timeout):
                    continue
        print(f"[client] No server found, retrying… ({attempt-1} left)")
    raise RuntimeError("no server found")

# ───────────────────────────────── CLI ─────────────────────────────────

def main():
    # Some status lines use non-ASCII arrows; on a legacy Windows console
    # (or when stdout is redirected/piped) the default codepage can't encode
    # them, which raises and kills the printing thread outright. Force UTF-8
    # with a safe fallback so a status message never crashes the sync.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description="Verbose manual-role folder sync")
    ap.add_argument("--dir", required=True, type=Path, help="Folder to sync")
    ap.add_argument("--port", type=int, default=TCP_PORT_DEFAULT, help="TCP port to use")
    ap.add_argument("--interval", type=int, default=0, help="Repeat interval (client mode)")
    ap.add_argument("--delete", action="store_true", help="Delete server files not present on client")
    ap.add_argument("--mode", required=True, choices=["receiver", "sender"], help="Fixed role: receiver/server or sender/client")
    ap.add_argument("--host", help="Server IP for client; omit to discover")
    ap.add_argument("--no-date-check", action="store_true", help="Skip last modified date conflict check")
    ap.add_argument("--parallel-hash", action="store_true", help="Use multithreaded hashing for speed")
    ap.add_argument("--exclude", nargs="*", default=[], metavar="PATTERN",
                     help=f"Extra fnmatch pattern(s) to skip, matched against file/dir names "
                          f"(always skipped: {', '.join(sorted(DEFAULT_EXCLUDES))})")
    args = ap.parse_args()

    # Pass skip_date_check flag to handler
    handle_client.skip_date_check = args.no_date_check
    # Pass parallel hashing flag
    handle_client.use_parallel_hashing = args.parallel_hash
    client_pass.use_parallel_hashing = args.parallel_hash
    EXCLUDES.update(args.exclude)

    if args.mode == "receiver":
        serve(args.dir, args.port, args.delete)
    else:
        target_host = args.host or discover_server()
        client_loop(args.dir, target_host, args.port, args.interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Stopped by user.")
        sys.exit(0)
