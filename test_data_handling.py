"""Data handling and replication test harness.

Launches a small cluster, waits for mocked sensor data to be generated and
replicated, then verifies per-peer CSV files.
"""

import csv
import multiprocessing
import shutil
import sys
import time
from pathlib import Path

import Peer
import Peer_utils


def peer_process(peer_label, address, port):
    """Peer process entry point for data handling test.

    Redirects stdout/stderr to a per-peer log file and starts the peer.

    Args:
        peer_label (str): Human-readable label (e.g., "peer1").
        address (str): Local IP address for the peer.
        port (int): TCP port for the peer's server socket.
    """
    Path("logs").mkdir(exist_ok=True)
    log_path = Path("logs") / f"{peer_label}_log.txt"
    sys.stdout = open(log_path, 'w', buffering=1, encoding='utf-8')
    sys.stderr = sys.stdout
    p = Peer.Peer(address=address, port=port)
    p.start()


def read_csv_rows(path: Path):
    """Read all rows from a CSV file.

    Args:
        path (Path): Path to the CSV file.

    Returns:
        list[list[str]]: Parsed rows including header; empty list on error or missing file.
    """
    if not path.exists():
        return []
    try:
        with path.open(newline='', encoding='utf-8') as f:
            return list(csv.reader(f))
    except Exception as e:
        print(f"Failed to read CSV file '{path}': {e}", file=sys.stderr)
        return []


def read_log(path: Path) -> str:
    """Read an entire log file as a string.

    Args:
        path (Path): Path to the log file.

    Returns:
        str: File contents or empty string on error/missing file.
    """
    try:
        if path.exists():
            with path.open('r', encoding='utf-8', errors='ignore') as f:
                return f.read()
    except Exception as exc:
        print(f"Warning: failed to read log file '{path}': {exc}", file=sys.stderr)
    return ""


def extract_internal_id(content: str) -> str | None:
    """Parse a peer UUID from log content.

    Args:
        content (str): Entire log file content.

    Returns:
        str | None: Extracted UUID string, if present.
    """
    for line in content.splitlines():
        if line.startswith("Peer ID:"):
            return line.split("Peer ID:", 1)[1].strip()
    return None


def run_test():
    """Run the data handling test.

    Launches three peers, waits for data generation, and verifies that each
    peer has replicated CSVs for all sensors.
    """
    print("--- Starting Data Handling Test (mocked sensors) ---")

    peer_labels = ["peer1", "peer2", "peer3"]
    peers: dict[str, dict] = {}

    # Reset base data directory before launching peers
    base_data = Path.cwd() / "data"
    if base_data.is_dir():
        try:
            shutil.rmtree(base_data)
            print(f"Cleared base data directory: {base_data}")
        except Exception as e:
            print(f"Failed to clear base data directory {base_data}: {e}")

    # Start peers (staggered)
    for pid in peer_labels:
        address = "127.0.0.1"
        port = Peer_utils.get_free_port()
        proc = multiprocessing.Process(target=peer_process, args=(pid, address, port))
        proc.daemon = True
        proc.start()
        peers[pid] = {
            "proc": proc,
            "port": port,
            "log": Path("logs") / f"{pid}_log.txt",
        }
        print(f"Started {pid} with PID {proc.pid} on port {port}")
        time.sleep(2)

    # Wait for network to stabilize and for 2-3 measurement intervals
    stabilize = 20
    extra = 40  # covers ~2 samples at 15s
    print(f"--- Waiting for stabilization ({stabilize}s) + sampling ({extra}s) ---")
    time.sleep(stabilize + extra)

    # Parse internal UUIDs from logs
    internal_ids: list[str] = []
    label_to_internal: dict[str, str] = {}
    for lbl in peer_labels:
        content = read_log(peers[lbl]["log"])
        internal = extract_internal_id(content)
        if internal:
            internal_ids.append(internal)
            label_to_internal[lbl] = internal

    if not internal_ids:
        print("--- FAIL: Could not parse internal peer IDs from logs ---")

    # Check per-peer directories and per-sensor CSVs
    base_missing: list[str] = []
    peer_dirs: dict[str, Path] = {}
    for internal in internal_ids:
        pdir = Path("data") / internal
        peer_dirs[internal] = pdir
        if not pdir.is_dir():
            base_missing.append(internal)

    if base_missing:
        print("--- FAIL: Missing data directories for:", ", ".join(base_missing))
    else:
        print("Data directories present for:", ", ".join(internal_ids))

    # Verify each peer directory contains CSV for every sensor
    per_peer_missing: dict[str, dict[str, list[str]]] = {}
    for holder in internal_ids:  # directory owner (UUID dir name)
        missing_or_empty_csvs: list[str] = []
        header_only_csvs: list[str] = []
        directory = peer_dirs.get(holder, Path("data") / holder)
        for sensor in internal_ids:
            path = directory / f"{sensor}.csv"
            rows = read_csv_rows(path)
            if not rows:
                missing_or_empty_csvs.append(sensor)
            elif len(rows) == 1:
                header_only_csvs.append(sensor)
            else:
                print(f"{holder}/{sensor}.csv -> {max(0, len(rows)-1)} rows; last: {rows[-1]}")
        if missing_or_empty_csvs or header_only_csvs:
            per_peer_missing[holder] = {
                "missing_or_empty": missing_or_empty_csvs,
                "header_only": header_only_csvs,
            }

    if per_peer_missing:
        print("--- FAIL: Missing, unreadable, or header-only CSVs (no data rows):")
        for holder, details in per_peer_missing.items():
            parts: list[str] = []
            if details["missing_or_empty"]:
                parts.append(f"missing/empty: {', '.join(details['missing_or_empty'])}")
            if details["header_only"]:
                parts.append(f"header only (no data rows): {', '.join(details['header_only'])}")
            print(f"  {holder}: " + "; ".join(parts))
    else:
        print("--- PASS: All peer directories contain CSVs for all sensors ---")

    # Cleanup
    for info in peers.values():
        proc = info["proc"]
        if proc.is_alive():
            proc.terminate()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    run_test()
