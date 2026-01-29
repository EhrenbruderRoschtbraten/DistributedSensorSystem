import multiprocessing
import time
import os
import sys
import Peer
import Peer_utils


def peer_process(peer_id, address, port):
    # Redirect output to per-peer log for verification
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/{peer_id}_log.txt"
    sys.stdout = open(log_path, 'w', buffering=1, encoding='utf-8')
    sys.stderr = sys.stdout
    p = Peer.Peer(peer_id=peer_id, address=address, port=port)
    p.start()


def run_test():
    print("--- Starting Bully Leader Election Test (kill leader) ---")

    peer_ids = ["peer1", "peer2", "peer3", "peer4"]
    peers = {}

    # Start peers
    for pid in peer_ids:
        address = Peer_utils.get_local_ip()
        port = Peer_utils.get_free_port()
        p = multiprocessing.Process(target=peer_process, args=(pid, address, port))
        p.daemon = True
        p.start()
        peers[pid] = {
            "proc": p,
            "port": port,
            "log": f"logs/{pid}_log.txt",
        }
        print(f"Started {pid} with PID {p.pid} on port {port}")
        time.sleep(2)

    print("--- Waiting for network to stabilize (15s) ---")
    time.sleep(15)

    # Determine current leader by parsing logs
    def read_log(path):
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
        except Exception:
            pass
        return ""

    leader_id = None
    for pid in peer_ids:
        content = read_log(peers[pid]["log"])
        if "Is Group Leader: True" in content:
            leader_id = pid
            break

    # Fallback if not found yet: wait a bit more and retry
    if leader_id is None:
        time.sleep(5)
        for pid in peer_ids:
            content = read_log(peers[pid]["log"])
            if "Is Group Leader: True" in content:
                leader_id = pid
                break

    if leader_id is None:
        # As a last resort assume peer1 is leader
        leader_id = "peer1"
    print(f"Detected leader: {leader_id}")

    # Crash the detected leader
    leader_proc = peers[leader_id]["proc"]
    print(f"--- CRASHING leader {leader_id} (PID {leader_proc.pid}) ---")
    leader_proc.terminate()

    # Wait for election
    wait_secs = 30
    print(f"--- Waiting for bully election to complete ({wait_secs}s) ---")
    time.sleep(wait_secs)

    # Compute expected leader among remaining peers using bully priority
    def priority(pid):
        return (str(pid), int(peers[pid]["port"]))

    remaining = [pid for pid in peer_ids if pid != leader_id]
    expected_leader = max(remaining, key=priority)
    p2 = read_log(peers["peer2"]["log"]) if "peer2" in peers else ""
    p3 = read_log(peers["peer3"]["log"]) if "peer3" in peers else ""

    success = False
    if f"Received COORDINATOR announcement: {expected_leader}" in p2 or f"Received COORDINATOR announcement: {expected_leader}" in p3:
        success = True

    if success:
        print(f"--- PASS: {expected_leader} elected as leader (bully algorithm) ---")
    else:
        print("--- FAIL: Did not detect expected COORDINATOR announcement ---")
        print("peer2_log excerpt:\n", p2[-1000:])
        print("peer3_log excerpt:\n", p3[-1000:])

    # Cleanup remaining processes
    for pid, info in peers.items():
        if pid == leader_id:
            continue
        proc = info["proc"]
        if proc.is_alive():
            proc.terminate()


if __name__ == "__main__":
    run_test()
