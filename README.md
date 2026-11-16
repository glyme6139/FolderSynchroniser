# FolderSynchroniser

One-way folder sync between two machines on the same local network. It is a
single Python script with no server setup: start a **receiver** on one
machine and a **sender** on the other, and files flow from sender to receiver.

## Features

- **Auto-discovery**: the sender finds the receiver over UDP broadcast, or you can pass `--host`.
- **Incremental**: file mtime and size are compared first. Only files whose
  metadata suggests a change are hashed (BLAKE3), and only files whose
  content actually differs are transferred.
- **Preserves timestamps**: modification time on all platforms, creation time on Windows.
- **Repeat mode**: `--interval N` re-syncs every N seconds.
- **Exclusions**: `.git`, `node_modules`, `__pycache__` and similar are skipped by default; add more with `--exclude`.
- **Progress bars** and a per-pass summary (files, MB, throughput).

## Install

Requires Python 3.8+.

```sh
pip install -r requirements.txt
```

## Usage

On the machine that should **receive** files:

```sh
python folder_sync.py --mode receiver --dir /path/to/destination
```

On the machine that should **send** files:

```sh
python folder_sync.py --mode sender --dir /path/to/source
# or, skipping discovery:
python folder_sync.py --mode sender --dir /path/to/source --host 192.168.1.20
```

### Options

| Option | Description |
| --- | --- |
| `--mode {receiver,sender}` | Role of this machine (required). |
| `--dir PATH` | Folder to sync (required). |
| `--host IP` | Receiver address (sender only). If omitted, the sender uses discovery. |
| `--port N` | TCP port (default `9000`). Discovery always uses UDP `9001`. |
| `--interval N` | Sender: repeat the sync every N seconds (default: run once). |
| `--delete` | Receiver: delete files that no longer exist on the sender. |
| `--no-date-check` | Transfer differing files even if the receiver's copy is newer. |
| `--parallel-hash` | Hash files with multiple threads. |
| `--exclude PATTERN ...` | Extra file/folder name patterns to skip (fnmatch syntax). |

## How it works

1. The sender scans its folder and sends `{path: (mtime, size)}` to the receiver.
2. The receiver marks files as *new/changed* (missing locally, or newer with a
   different size) or as *to verify* (newer, same size).
3. Both sides hash the *to verify* files and compare the hashes.
4. The sender streams every file that differs, and the receiver writes it and restores its timestamps.

By default a file is only overwritten when the sender's copy is newer than
the receiver's. Use `--no-date-check` to turn this off.

## Security

This tool is meant for **trusted local networks**:

- There is **no authentication and no encryption**. Anyone who can reach the
  receiver's port can write files into the synced folder, and with `--delete`
  they can remove files from it.
- Paths received from the other machine are validated, so neither side can
  read or write files outside its `--dir` folder.

Do not expose the receiver port to the internet.

## License

[MIT](LICENSE)
