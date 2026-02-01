# Distributed Sensor System

A lightweight, dynamic distributed sensor network supporting automatic leader election via the bully algorithm and ordered group messaging using a simple sequencer.

## Overview
- Dynamic host discovery via UDP broadcast over `BROADCAST_PORT` (default: 9999).
- Peers maintain a `groupView` and `orderedPeerList` for membership and ordering.
- The leader acts as both group coordinator and sequencer for ordered multicast.
- Failure detection uses leader heartbeats and member ACKs.

## Leader Election (Bully Algorithm)
- Priority: peers compare by `(peer_id, port)` lexicographically. With `peer1`, `peer2`, `peer3`, the order is `peer3 > peer2 > peer1`.
- Trigger: election starts when members detect leader heartbeat timeout or when a leader failure is processed.
- Flow:
  1. A peer sends `ELECTION:<sender_id>` to all peers with higher priority.
  2. Higher peers respond `OK` and start their own election.
  3. If no `OK` is received within `election_timeout`, the initiator announces `COORDINATOR:<peer_id>` and becomes leader.
  4. All peers update `leader_id`; the leader starts heartbeats.

## Message Types
- Discovery: `NEW_PEER_REQUEST` (UDP), `ADDED TO NETWORK` (TCP).
- Membership change: `VIEW_CHANGE:<orderedPeerList>-<groupView>-<view_id>` (TCP).
- Heartbeats: `HEARTBEAT` / `HEARTBEAT_ACK` (TCP).
- Election: `ELECTION:<peer_id>`, `OK`, `COORDINATOR:<peer_id>` (TCP).
- Sequencer: `SEQUENCER_REQUEST:<payload>` (TCP).

## Failure Detection
- Leader sends `HEARTBEAT` to members every `heartbeat_interval`; expects `HEARTBEAT_ACK` within `heartbeat_timeout`.
- On timeout for a member, the leader removes the member from the view and multicasts `VIEW_CHANGE`.
- Members run `leader_check_thread` and start bully election if the leader heartbeat is not observed within `heartbeat_timeout`.

## Configuration Knobs
- `heartbeat_interval` (default: 5s)
- `heartbeat_timeout` (default: 10s)
- `election_timeout` (default: 5s)

## Run
From the project root:

```powershell
# Start three peers
python .\main_multithreading.py

# Fail-stop test (kills a non-leader and checks membership updates)
python .\test_fault_tolerance.py

# Bully leader election test (detects current leader, kills it, verifies new leader)
python .\test_leader_election.py
```

## Logs
- Tests redirect per-peer output to `logs/{peer_id}_log.txt` (e.g., `logs/peer2_log.txt`).
- Look for `Received COORDINATOR announcement: <peer>` or `became the Group Leader via election` in these log files to verify election.

## Windows Notes
- You may see warnings about `SO_REUSEPORT` not being supported.
- Multiple broadcast listeners across processes can cause bind conflicts on the same port. The system still functions using whichever listener successfully binds.

## Extending Priority
- To change election priority, adjust `get_priority()` in `Peer.py` to use other criteria (e.g., numeric IDs or timestamps).