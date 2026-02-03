import threading
import time
import os
import shutil
from Peer import Peer

class NetworkValidator:
    def __init__(self, peers):
        self.peers = peers

    def assert_single_leader(self):
        """Verify that exactly one leader exists in the network."""
        leaders = [p for p in self.peers if p.isGroupLeader]
        count = len(leaders)
        if count == 1:
            print(f"[ASSERT PASS] Unique leader found: {leaders[0].peer_id}")
        else:
            print(f"[ASSERT FAIL] Found {count} leaders. Split-brain detected!")
        return count == 1

    def assert_discovery(self):
        """Verify all peers have found each other in their groupView."""
        total_peers = len(self.peers)
        for p in self.peers:
            # groupView should have (total_peers - 1) entries + self
            view_size = len(p.groupView)
            if view_size < total_peers:
                print(f"[ASSERT FAIL] Peer {p.peer_id} only sees {view_size}/{total_peers} peers.")
                return False
        print(f"[ASSERT PASS] All {total_peers} peers discovered each other.")
        return True

def cleanup():
    if os.path.exists("data"):
        shutil.rmtree("data")
    print("[Test] Environment cleaned.")

# --- Test Case 1: Sequential Join ---
def run_sequential_test():
    print("\n" + "="*20 + " SEQUENTIAL TEST " + "="*20)
    cleanup()
    peers = []
    
    # Start Leader
    p1 = Peer("127.0.0.1", 5001)
    peers.append(p1)
    threading.Thread(target=p1.start, daemon=True).start()
    time.sleep(7) # Wait for it to claim leadership

    # Start Followers
    for i in range(2):
        p = Peer("127.0.0.1", 5002 + i)
        peers.append(p)
        threading.Thread(target=p.start, daemon=True).start()
        time.sleep(2)

    time.sleep(5) # Let network settle
    validator = NetworkValidator(peers)
    validator.assert_single_leader()
    validator.assert_discovery()

# --- Test Case 2: Simultaneous Join ---
def run_simultaneous_test():
    print("\n" + "="*20 + " SIMULTANEOUS TEST " + "="*20)
    cleanup()
    peers = []
    
    for i in range(3):
        peers.append(Peer("127.0.0.1", 6001 + i))

    print("[Test] Launching all peers at once...")
    for p in peers:
        threading.Thread(target=p.start, daemon=True).start()

    print("[Test] Waiting for jitter and election to resolve (15s)...")
    time.sleep(15)

    validator = NetworkValidator(peers)
    # This is the real test of your jitter/step-down logic
    validator.assert_single_leader()
    validator.assert_discovery()

if __name__ == "__main__":
    run_sequential_test()
    # run_simultaneous_test()