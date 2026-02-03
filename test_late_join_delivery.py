"""Integration Test: Late joiner receives only new ordered messages.

Scenario:
1. Start a peer (late joiner candidate).
2. Simulate leader acknowledgement with starting sequence = 4.
3. Multicast two ordered messages (#4, #5).
4. Verify peer delivers only those new messages for the test sensor.
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from Peer_utils import get_free_port

LOGDIR = "test_logs_late_join"
os.makedirs(LOGDIR, exist_ok=True)
MCAST_GRP = "224.1.1.1"
MCAST_PORT = 5007


def cleanup_data():
    data_path = Path.cwd() / "data"
    if data_path.exists():
        shutil.rmtree(data_path)


def wait_for_log_contains(log_path, needle, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        if log_path.exists():
            try:
                text = log_path.read_text(errors="replace")
            except Exception:
                text = ""
            if needle in text:
                return True
        time.sleep(0.25)
    return False


def send_added_to_network(host, port, starting_seq):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    driver_id = f"driver-{int(time.time() * 1000)}"
    handshake = f"HELLO from {host}:0 and peer id:{driver_id}"
    sock.sendall(handshake.encode())
    time.sleep(0.2)
    msg = f"ADDED TO NETWORK:{starting_seq}\n"
    sock.sendall(msg.encode())
    sock.close()
    return driver_id


def send_multicast_messages(messages, sender_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    for seq_id, payload in messages:
        body = {
            "type": "MULTICAST_MESSAGE_ORDER",
            "message": [seq_id, payload],
            "from": sender_id,
        }
        sock.sendto(json.dumps(body).encode(), (MCAST_GRP, MCAST_PORT))
        time.sleep(0.1)
    sock.close()


def read_sensor_temperatures(csv_path):
    if not csv_path.exists():
        return []
    lines = csv_path.read_text(errors="replace").splitlines()
    temps = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) >= 4:
            temps.append(parts[3])
    return temps


def main():
    cleanup_data()
    host = "127.0.0.1"
    port = get_free_port()

    p_log = Path(LOGDIR) / "peer.log"

    print("=" * 70)
    print("LATE JOIN DELIVERY TEST")
    print("=" * 70)
    print(f"Port: {port}, Address: {host}")
    print()

    processes = []
    try:
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        out_f = open(p_log, "wb")
        err_f = open(Path(LOGDIR) / "peer.err", "wb")
        p = subprocess.Popen([sys.executable, "-u", "peer_entry.py", str(port)],
                             stdout=out_f, stderr=err_f, env=env)
        processes.append((p, out_f, err_f))

        driver_id = send_added_to_network(host, port, starting_seq=4)

        if not wait_for_log_contains(p_log, "Synchronized! Now expecting message #4", timeout=20):
            print("FAIL: Late joiner did not synchronize in time.")
            return 1

        sensor_id = "test-sensor"
        payloads = [
            f"DATA|{sensor_id}|2026-02-03T12:00:04Z|100.4|40.0|1000.0",
            f"DATA|{sensor_id}|2026-02-03T12:00:05Z|100.5|40.0|1000.0",
        ]

        time.sleep(1)
        multicast_msgs = [(4, payloads[0]), (5, payloads[1])]
        send_multicast_messages(multicast_msgs, sender_id=driver_id)

        time.sleep(3)

        data_root = Path.cwd() / "data"
        peer_dirs = [p for p in data_root.glob("*") if p.is_dir()]
        if not peer_dirs:
            print("FAIL: No peer data directory created.")
            return 1
        csv_path = peer_dirs[0] / f"{sensor_id}.csv"
        temps = read_sensor_temperatures(csv_path)

        expected = {"100.4", "100.5"}
        received = set(temps)

        print("\n" + "=" * 70)
        print("ANALYSIS")
        print("=" * 70)
        print(f"Peer test-sensor temps: {temps}")

        if not expected.issubset(received):
            print("FAIL: Late joiner did not receive the last two messages.")
            return 1
        if "100.1" in received or "100.2" in received or "100.3" in received:
            print("FAIL: Late joiner received messages sent before it joined.")
            return 1

        print("PASS: Late joiner received only the new ordered messages.")
        return 0
    finally:
        for p, out_f, err_f in processes:
            try:
                if p.poll() is None:
                    p.terminate()
                    p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
            try:
                out_f.close()
                err_f.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
