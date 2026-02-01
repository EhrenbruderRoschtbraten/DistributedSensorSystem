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
            # Intentionally ignore any log read errors (e.g., log not yet created)
            # and fall through to return an empty string so the test can retry.
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
    remaining_logs = {pid: read_log(peers[pid]["log"]) for pid in remaining}
    expected_leader_log = remaining_logs.get(expected_leader, "")

    success = False
    # Any remaining peer acknowledging the coordinator is a success
    if any(f"Received COORDINATOR announcement: {expected_leader}" in log for log in remaining_logs.values()):
        success = True
    # Or the expected leader confirming becoming leader
    if "became the Group Leader via election" in expected_leader_log and expected_leader in expected_leader_log:
        success = True

    if success:
        print(f"--- PASS: {expected_leader} elected as leader (bully algorithm) ---")
    else:
        print("--- FAIL: Did not detect expected COORDINATOR announcement ---")
        for pid, log in remaining_logs.items():
            print(f"{pid}_log excerpt:\n", log[-1000:])

    # Cleanup remaining processes
    for pid, info in peers.items():
        if pid == leader_id:
            continue
        proc = info["proc"]
        if proc.is_alive():
            proc.terminate()


if __name__ == "__main__":
    run_test()
