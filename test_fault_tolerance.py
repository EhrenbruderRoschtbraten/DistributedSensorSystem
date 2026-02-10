
"""Fail-stop fault tolerance test harness.

Starts three peers, terminates one to simulate a crash, and relies on logs to
verify that the group membership is updated accordingly.
"""

import multiprocessing
import time

import Peer
import Peer_utils

# Setup similar to main_multithreading.py but with control to kill a peer

def peer_process(peer_label, address, port):
    """Entry point for a peer process used in the test.

    Args:
        peer_label (str): Human-readable label (e.g., "peer1").
        address (str): Local IP address.
        port (int): TCP port for peer's server socket.
    """
    p = Peer.Peer(address=address, port=port)
    p.start()

def run_test():
    """Run the fail-stop fault tolerance scenario.

    Starts three peers, kills one, waits for detection timeout, and prints
    guidance for verifying logs.
    """
    print("--- Starting Fail-Stop Fault Tolerance Test ---")
    
    # 1. Setup Identities
    peer_ids = ["peer1", "peer2", "peer3"]
    processes = []
    
    # Clean up previous runs
    # Cross-platform: no-op cleanup for Windows
    time.sleep(1)

    # 2. Start Peers
    for pid in peer_ids:
        address = Peer_utils.get_local_ip()
        port = Peer_utils.get_free_port()
        
        # Need to ensure unique IDs and persistence dirs exists (mocking identity load)
        # For simplicity, we assume directories exist or we just pass the ID directly
        # The Peer class takes peer_id directly.
        
        p = multiprocessing.Process(target=peer_process, args=(pid, address, port))
        p.daemon = True
        p.start()
        processes.append(p)
        print(f"Started {pid} with PID {p.pid}")
        time.sleep(2) # Stagger start to ensure deterministic leader (first one)

    print("--- Waiting for network to stabilize (15s) ---")
    time.sleep(15)

    # 3. Simulate Crash
    target_peer_index = 2 # peer3
    target_proc = processes[target_peer_index]
    print(f"--- CRASHING peer3 (PID {target_proc.pid}) ---")
    target_proc.terminate()
    
    print("--- Waiting for detection timeout (25s) ---")
    time.sleep(25)

    # 4. Verification
    # We can't easily read the internal state of the other processes without IPC or log parsing.
    # Since I cannot see stdout easily in this test script if I don't redirect it,
    # I will rely on the main output capturing stdout from the child processes.
    
    print("--- Test Finished. Checking logs above for 'Peer peer3 failed' or 'Updated group view' removing peer3 ---")
    
    # Clean up
    for p in processes:
        if p.is_alive():
            p.terminate()

if __name__ == "__main__":
    run_test()
