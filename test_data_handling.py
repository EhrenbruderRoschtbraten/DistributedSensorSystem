import multiprocessing
import time
import os
import csv
import sys
import Peer
import Peer_utils


def peer_process(peer_id, address, port):
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/{peer_id}_log.txt"
    sys.stdout = open(log_path, 'w', buffering=1, encoding='utf-8')
    sys.stderr = sys.stdout
    p = Peer.Peer(peer_id=peer_id, address=address, port=port)
    p.start()


def read_csv_rows(path):
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            r = csv.reader(f)
            rows = list(r)
    except Exception:
        pass
    return rows


def run_test():
    print("--- Starting Data Handling Test (mocked sensors) ---")

    peer_ids = ["peer1", "peer2", "peer3"]
    peers = {}

    # Start peers (staggered)
    for pid in peer_ids:
        address = Peer_utils.get_local_ip()
        port = Peer_utils.get_free_port()
        proc = multiprocessing.Process(target=peer_process, args=(pid, address, port))
        proc.daemon = True
        proc.start()
        peers[pid] = {
            "proc": proc,
            "port": port,
            "log": f"logs/{pid}_log.txt",
        }
        print(f"Started {pid} with PID {proc.pid} on port {port}")
        time.sleep(2)

    # Wait for network to stabilize and for 2-3 measurement intervals
    stabilize = 20
    extra = 40  # covers ~2 samples at 15s
    print(f"--- Waiting for stabilization ({stabilize}s) + sampling ({extra}s) ---")
    time.sleep(stabilize + extra)

    # Check per-peer directories and per-sensor CSVs
    base_missing = []
    peer_dirs = {}
    for pid in peer_ids:
        pdir = os.path.join("data", pid)
        peer_dirs[pid] = pdir
        if not os.path.isdir(pdir):
            base_missing.append(pid)

    if base_missing:
        print("--- FAIL: Missing data directories for:", ", ".join(base_missing))
    else:
        print("Data directories present for:", ", ".join(peer_ids))

    # Verify each peer directory contains CSV for every sensor
    per_peer_missing = {}
    for holder in peer_ids:  # directory owner
        missing_csvs = []
        for sensor in peer_ids:
            path = os.path.join(peer_dirs.get(holder, os.path.join("data", holder)), f"{sensor}.csv")
            rows = read_csv_rows(path)
            if len(rows) <= 1:
                missing_csvs.append(sensor)
            else:
                print(f"{holder}/{sensor}.csv -> {max(0, len(rows)-1)} rows; last: {rows[-1]}")
        if missing_csvs:
            per_peer_missing[holder] = missing_csvs

    if per_peer_missing:
        print("--- FAIL: Missing or empty CSVs:")
        for holder, sensors in per_peer_missing.items():
            print(f"  {holder}: {', '.join(sensors)}")
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
