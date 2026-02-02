import multiprocessing
import time
import os
import sys
from pathlib import Path
import Peer
import Peer_utils


def peer_process(peer_label, address, port):
    """Peer process entry point for election test.

    Redirects stdout/stderr to a per-peer log file and starts the peer.

    Args:
        peer_label (str): Human-readable label (e.g., "peer1").
        address (str): Local IP address for the peer.
        port (int): TCP port for the peer's server socket.
    """
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/{peer_label}_log.txt"
    sys.stdout = open(log_path, 'w', buffering=1, encoding='utf-8')
    sys.stderr = sys.stdout
    p = Peer.Peer(address=address, port=port)
    p.start()


def read_log(path):
    """Read log file safely, returning full content."""
    try:
        p = Path(path)
        if p.exists():
            with p.open('r', encoding='utf-8', errors='ignore') as f:
                return f.read()
    except Exception as e:
        print(f"Warning: Failed to read log {path}: {e}")
    return ""


def extract_internal_id(content):
    """Extract peer UUID from log."""
    for line in content.splitlines():
        if line.startswith("Peer ID:"):
            return line.split("Peer ID:", 1)[1].strip()
    return None


def run_test():
    """Run the bully leader election test (crash current leader).

    Starts four peers, detects current leader, terminates it, waits for a new
    election, and verifies success via logs.
    """
    print("=" * 80)
    print("Starting Bully Leader Election Test (kill leader)")
    print("=" * 80)

    peer_labels = ["peer1", "peer2", "peer3", "peer4"]
    peers = {}

    # Start peers with staggered timing
    print("\n[SETUP] Starting peers...")
    for pid in peer_labels:
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
        print(f"  Started {pid} with PID {p.pid} on port {port}")
        time.sleep(2)

    print("\n[STABILIZATION] Waiting for network to stabilize (25s)...")
    time.sleep(25)

    # Parse peer UUIDs from logs
    print("\n[DISCOVERY] Extracting peer UUIDs from logs...")
    label_to_internal = {}
    for pid in peer_labels:
        content = read_log(peers[pid]["log"])
        internal = extract_internal_id(content)
        if internal:
            label_to_internal[pid] = internal
            print(f"  {pid:6} -> {internal}")
        else:
            print(f"  {pid:6} -> NOT FOUND")

    # Detect current leader
    print("\n[LEADER DETECTION] Identifying current leader...")
    leader_label = None
    for pid in peer_labels:
        content = read_log(peers[pid]["log"])
        if "Is Group Leader: True" in content:
            leader_label = pid
            break

    if leader_label is None:
        print("  Leader not detected in first check, waiting 10s...")
        time.sleep(10)
        for pid in peer_labels:
            content = read_log(peers[pid]["log"])
            if "Is Group Leader: True" in content:
                leader_label = pid
                break

    if leader_label is None:
        leader_label = "peer1"
        print(f"  Leader not found, defaulting to {leader_label}")
    else:
        leader_uuid = label_to_internal.get(leader_label, "unknown")
        print(f"  Current leader: {leader_label} (UUID: {leader_uuid})")

    # Crash the detected leader
    print(f"\n[CRASH] Terminating leader {leader_label}...")
    leader_proc = peers[leader_label]["proc"]
    leader_proc.terminate()
    time.sleep(2)

    # Wait for election to complete
    wait_secs = 40
    print(f"\n[ELECTION] Waiting for bully election to complete ({wait_secs}s)...")
    time.sleep(wait_secs)

    # Calculate expected leader among remaining peers
    print("\n[VERIFICATION] Computing expected leader by priority...")
    remaining = [pid for pid in peer_labels if pid != leader_label]

    def priority_key(internal_id):
        """Priority is UUID string lexicographic ordering (UUID-only)."""
        return str(internal_id)

    # Determine expected leader if all UUIDs are known; otherwise, we'll detect actual leader from logs
    expected_leader_label = None
    expected_leader_uuid = None
    if all(label_to_internal.get(lab) for lab in remaining):
        print("  All peer UUIDs available, calculating priority (UUID-only):")
        for lab in remaining:
            pri = priority_key(label_to_internal[lab])
            print(f"    {lab:6}: {pri}")
        expected_leader_label = max(remaining, key=lambda lab: priority_key(label_to_internal[lab]))
        expected_leader_uuid = label_to_internal.get(expected_leader_label, "unknown")
    else:
        print("  Warning: Some peer UUIDs missing; will detect leader from logs.")

    print(f"  Expected new leader: {expected_leader_label} (UUID: {expected_leader_uuid})")

    # Read logs from remaining peers
    remaining_logs = {pid: read_log(peers[pid]["log"]) for pid in remaining}
    expected_leader_log = remaining_logs.get(expected_leader_label, "") if expected_leader_label else ""

    # Check for successful election
    print("\n[RESULT] Checking for election success...")
    success = False
    success_reasons = []

    if expected_leader_label and expected_leader_uuid:
        # Check 1: Any remaining peer acknowledging the new coordinator
        for pid, log in remaining_logs.items():
            if f"Received COORDINATOR announcement: {expected_leader_uuid}" in log:
                success = True
                success_reasons.append(f"✓ {pid} received COORDINATOR announcement for {expected_leader_uuid}")

        # Check 2: Expected leader confirming it became leader
        if "became the Group Leader via election" in expected_leader_log:
            success = True
            success_reasons.append(f"✓ {expected_leader_label} announced it became Group Leader via election")

        # Check 3: Expected leader has "Is Group Leader: True"
        if "Is Group Leader: True" in expected_leader_log:
            success = True
            success_reasons.append(f"✓ {expected_leader_label} confirmed in logs as Group Leader")
    else:
        # Generic success detection when expectation can't be computed
        # Check A: Any COORDINATOR announcement seen
        for pid, log in remaining_logs.items():
            if "Received COORDINATOR announcement:" in log:
                success = True
                success_reasons.append(f"✓ {pid} logged a COORDINATOR announcement")
                break
        # Check B: Any remaining peer logs 'Is Group Leader: True' or 'became the Group Leader via election'
        if not success:
            for pid, log in remaining_logs.items():
                if ("Is Group Leader: True" in log) or ("became the Group Leader via election" in log):
                    success = True
                    success_reasons.append(f"✓ {pid} became or confirmed Group Leader")
                    break

    print()
    if success:
        print("=" * 80)
        if expected_leader_label and expected_leader_uuid:
            print(f"PASS: Leader election successful! Expected leader: {expected_leader_label} ({expected_leader_uuid})")
        else:
            print("PASS: Leader election successful!")
        print("=" * 80)
        for reason in success_reasons:
            print(reason)
    else:
        print("=" * 80)
        print("FAIL: Leader election did not complete as expected")
        print("=" * 80)
        print("\nRemaining peer logs (last 1500 chars):")
        for pid in remaining:
            log = remaining_logs.get(pid, "")
            print(f"\n--- {pid}_log.txt ---")
            print(log[-1500:] if log else "(empty)")
            print()

    # Cleanup remaining processes
    print("\n[CLEANUP] Terminating remaining peers...")
    for pid, info in peers.items():
        if pid == leader_label:
            continue
        proc = info["proc"]
        if proc.is_alive():
            proc.terminate()
    
    print("[CLEANUP] Done\n")
    return success


if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
