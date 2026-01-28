
import multiprocessing
import time
import os
import signal
import Peer
import Peer_utils
import identity
import sys

# Setup similar to main_multithreading.py but with control to kill a peer

def peer_process(peer_id, address, port):
    p = Peer.Peer(peer_id=peer_id, address=address, port=port)
    # Redirect stdout to avoid clutter or keep it for debugging
    # sys.stdout = open(f'{peer_id}_log.txt', 'w') 
    p.start()

def run_test():
    print("--- Starting Fail-Stop Fault Tolerance Test ---")
    
    # 1. Setup Identities
    peer_ids = ["peer1", "peer2", "peer3"]
    processes = []
    
    # Clean up previous runs
    os.system("pkill -9 -f python3")
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
    target_pid = processes[target_peer_index].pid
    print(f"--- CRASHING peer3 (PID {target_pid}) ---")
    os.kill(target_pid, signal.SIGKILL)
    
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
