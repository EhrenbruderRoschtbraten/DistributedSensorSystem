import multiprocessing
import time
import os
import shutil
import pandas as pd
from pathlib import Path
from Peer import Peer

def run_peer_worker(addr, port):
    """
    Initialize and start the Peer entirely inside the child process.
    This avoids pickling locks/sockets.
    """
    try:
        peer = Peer(addr, port)
        peer.start() # This blocks until the process is terminated
    except Exception as e:
        print(f"Peer at {port} crashed: {e}")

def cleanup_data():
    data_path = Path.cwd() / "data"
    if data_path.exists():
        shutil.rmtree(data_path)
    print("Cleaned up old test data.")

def test_distributed_system():
    # Windows fix: Ensure cleanup happens in the main entry point
    cleanup_data()
    
    # Use 'spawn' context explicitly for Windows consistency
    ctx = multiprocessing.get_context('spawn')

    # 1. Start Peer 1
    p1 = ctx.Process(target=run_peer_worker, args=('127.0.0.1', 5001))
    p1.start()
    print("Started Peer 1 (Expected Leader)...")
    time.sleep(10) 

    # 2. Start Peer 2 and Peer 3
    p2 = ctx.Process(target=run_peer_worker, args=('127.0.0.1', 5002))
    p3 = ctx.Process(target=run_peer_worker, args=('127.0.0.1', 5003))
    p2.start()
    p3.start()
    print("Started Peer 2 and Peer 3...")

    print("Waiting 20 seconds for data generation...")
    time.sleep(20)

    # ... rest of your verification logic ...

    # Clean up processes
    for p in [p1, p2, p3]:
        if p.is_alive():
            p.terminate()
            p.join()
    print("Test complete.")

if __name__ == "__main__":
    # CRITICAL: This line is mandatory on Windows to prevent recursion errors
    multiprocessing.freeze_support()
    test_distributed_system()