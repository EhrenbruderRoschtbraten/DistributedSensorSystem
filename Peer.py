from email import message
from pydoc import text
import socket
import threading
import uuid
import time
import queue
import json
import struct
import os
import csv
import random
from datetime import datetime


BROADCAST_PORT = 9999  # Dedicated port for UDP broadcasts
BROADCAST_IP = '127.0.0.1'

MCAST_GRP = '224.1.1.1'
MCAST_PORT = 5007

class Peer():
    def __init__(self, peer_id, address, port):
        self.peer_id = peer_id
        self.address = address
        self.partOfNetwork = False
        self.port = port
        self.connection_dict = {}
        self.isGroupLeader = False
        self.tcp_thread = None
        self.udp_thread = None
        self.groupView = {}
        self.orderedPeerList = []
        self.successor = None  # Changed to dict: {'peer_id':, 'address':, 'port':}
        self.election_active = False
        self.leader_id = None
        self.last_leader_heartbeat = time.time()
        self.heartbeat_interval = 5  # seconds
        self.heartbeat_timeout = 10  # seconds
        self.heartbeat_thread = None
        self.leader_check_thread = None
        self.pending_acks = {}
        self.sequencer_peer_id = None  # To be set when known
        self.sequencer_sequence_number = None  
        self.local_sequence_number = 0
        self.lock = threading.Lock()
        self.multicast_socket = None
        self.incoming_queue = queue.PriorityQueue()
        self.expected_seq = 1
        self.delivered_messages = []
        self.multicast_thread_active = False
        self.view_id = 0  # To track changes in group view
        self.peer_last_heartbeat = {} # Map peer_id -> last_ack_time
        self.election_timeout = 5
        self.received_ok = False
        # Sensor data config
        self.sensor_interval_seconds = 15
        self.sensor_thread = None
        # Each peer writes into its own data directory for replication
        self.data_dir = os.path.join(os.getcwd(), "data", self.peer_id)


    def __str__(self):
        return f"Peer ID: {self.peer_id}, Address: {self.address}, Port: {self.port}"
    
    def start(self):
        self.start_listening_threads()
        self.broadcast_new_peer_request('127.0.0.1', 9999)
        time_after_broadcast = time.time()
        #how does a peer know it has been added to the group?
        #maybe once it receives successor information from leader?
        # if no reply is received within 5 seconds, assume leader
        time.sleep(5)  # wait for potential responses
        while not self.partOfNetwork:
            if time.time() - time_after_broadcast > 5:
                print("No response received. Assuming role of Group Leader.")
                self.isGroupLeader = True
                self.leader_id = self.peer_id
                self.sequencer_peer_id = self.peer_id
                self.sequencer_sequence_number = 0
                self.partOfNetwork = False
                self.groupView[self.peer_id] = {
                    'address': self.address,
                    'port': self.port,
                    'role': 'leader'
                }
                self.orderedPeerList.append(self.peer_id)
                break

        #CONTINUE BROADCASTING UNTIL PART OF NETWORK OR DISCOVER ANOTHER PEER
        while not self.partOfNetwork:
            print("Waiting to join the network...")
            self.broadcast_new_peer_request('127.0.0.1', 9999)
            time.sleep(5)
        

        # Start fault tolerance threads
        self.start_leader_check_thread()
        if self.isGroupLeader:
            self.start_heartbeat_thread()

        time.sleep(5)  # WAIT FOR THE OTHER PEERS TO SETTLE

        if(self.peer_id == "peer1"):
            self.send_message_to_sequencer("Hello from peer1")

        if(self.peer_id == "peer3"):
            self.send_message_to_sequencer("Hello from peer3")

        if(self.peer_id == "peer2"):
            self.send_message_to_sequencer("Hello from peer2")

        time.sleep(5)  # WAIT FOR MESSAGES TO BE PROCESSED

        self.print_status()
        # Start mocked sensor thread once part of network and multicast ready
        try:
            if self.sensor_thread is None:
                self.start_sensor_thread()
        except Exception as e:
            print(f"Failed to start sensor thread: {e}")
        # Keep the peer running by joining the threads
        if self.tcp_thread:
            self.tcp_thread.join()
        if self.udp_thread:
            self.udp_thread.join()

    def send_message_to_sequencer(self, message):
        if self.sequencer_peer_id == self.peer_id:
            print(f"[Node {self.peer_id}] I am the sequencer, processing message directly: '{message}'")
            with self.lock:
                self.sequencer_sequence_number += 1
                ordered_msg = (self.sequencer_sequence_number, message)
                self.multicast_message_to_group(ordered_msg, from_peer_id=self.peer_id)
            return
        else:
            print(f"[Node {self.peer_id}] Sending: '{message}'")
            # In a real system, this goes to the sequencer first
            #wraps message with sequence number
            #send message to sequencer peer
            peer_info = self.groupView.get(self.sequencer_peer_id)
            message = f"SEQUENCER_REQUEST:{message}"
            if self.sequencer_peer_id:
                try:
                    if self.sequencer_peer_id not in self.connection_dict:
                        self.send_connection_request(peer_info['address'], peer_info['port'], self.sequencer_peer_id)
                    self.send_message(self.sequencer_peer_id, message)
                    print(f"Sent sequencer request to {self.sequencer_peer_id}: {message}")
                except Exception as e:
                        print(f"Failed to send updated successor information to {self.sequencer_peer_id}: {e}")
            else:
                print("Sequencer peer ID unknown, cannot send message.")
        
    def process_buffer(self):
        """ The 'Hold-back Buffer' Logic """
        while not self.incoming_queue.empty():
            # Peek at the priority queue (lowest seq number first)
            seq_id, msg = self.incoming_queue.queue[0]
            
            if seq_id == self.expected_seq:
                # Correct order! Deliver it.
                self.incoming_queue.get() 
                self.delivered_messages.append(msg)
                print(f"  ==> [Node {self.peer_id}] Delivered #{seq_id}: {msg}")
                try:
                    self.handle_ordered_payload(msg, seq_id)
                except Exception as e:
                    print(f"Error handling ordered payload: {e}")
                self.expected_seq += 1
            else:
                # Out of order! Gap detected. Wait for the missing message.
                break

    def connect(self):
        print(f"Connecting to Peer {self.peer_id} at {self.address}:{self.port}")   

    def create_listening_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', self.port))
        s.listen()
        return s
    
    def accept_connection(self, listening_socket):
        while True:
            conn, addr = listening_socket.accept()
            msg = conn.recv(1024)  # Optional handshake message
            decoded_msg = msg.decode()
            # Parse: "HELLO from 127.0.0.1:5001 and peer id:peer2"
            if " and peer id:" in decoded_msg:
                parts = decoded_msg.split(" and peer id:")
                address_port = parts[0].split(" from ")[1]  # "127.0.0.1:5001"
                peer_id = parts[1]
                peer_ip, peer_port_str = address_port.split(":")
                peer_port = int(peer_port_str)
            else:
                # Fallback, but should not happen
                peer_port = 0
                peer_id = "unknown"
            print(f"Accepted connection from {addr} with port {peer_port} and peer id:{peer_id}")
            # self.connection_dict[f"peer_{peer_port}"] = conn
            self.connection_dict[peer_id] = conn

            threading.Thread(target=self.receive_message,
                             args=(peer_id,),
                             daemon=True).start()
        
    
    def send_message(self, peer_key, message):
        if peer_key in self.connection_dict:
            conn = self.connection_dict[peer_key]
            conn.sendall(message.encode())
            print(f"Sent to {peer_key}: {message}")
        else:
            print(f"No connection found for {peer_key} while sending message only {self.connection_dict.keys()} available")

    def receive_message(self, peer_key, buffer_size=1024):
        print(f"Receive thread started for {peer_key}")
        if peer_key in self.connection_dict:
            conn = self.connection_dict[peer_key]
            while True:
                try:
                    data = conn.recv(buffer_size)
                    if not data:
                        print(f"Connection closed by {peer_key}")
                        break
                except Exception as e:
                    print(f"Connection error with {peer_key}: {e}")
                    break
                message = data.decode()
                print(f"Received from {peer_key}: {message}")
                if message.startswith("SUCCESSOR_INFORMATION:"):
                    parts = message.split("SUCCESSOR_INFORMATION:")[1].split(":", 1)
                    successor_list_str = parts[0]
                    group_view_str = parts[1]
                    successor_list = eval(successor_list_str)
                    group_view = eval(group_view_str)
                    self.handle_successor_information(successor_list, group_view)
                    self.partOfNetwork = True   #ONLY AFTER RECEIVING SUCCESSOR INFO DO WE CONSIDER OURSELVES PART OF THE NETWORK
                    print(f"Peer {self.peer_id} is now part of the network.")
                    print("successor information processed. ", self.successor)
                elif message == "HEARTBEAT":
                    self.last_leader_heartbeat = time.time()
                    self.send_message(peer_key, "HEARTBEAT_ACK")
                elif message == "HEARTBEAT_ACK":
                    # Leader received ack, update timestamp
                    print(f"Received HEARTBEAT_ACK from {peer_key}")
                    self.peer_last_heartbeat[peer_key] = time.time()
                elif message.startswith("VIEW_CHANGE:"):
                    print(f"Hurrah Received VIEW_CHANGE message: {message} my peer id is {self.peer_id}")
                    parts = message.split("VIEW_CHANGE:")[1].split("-", 2)
                    new_ordered_list_str = parts[0]
                    new_group_view_str = parts[1]
                    new_view_id_str = parts[2]
                    new_ordered_list = eval(new_ordered_list_str)
                    new_group_view = eval(new_group_view_str)
                    new_view_id = int(new_view_id_str)
                    print(f"Parsed VIEW_CHANGE - Ordered List: {new_ordered_list}, Group View: {new_group_view}, View ID: {new_view_id}")
                    self.handle_view_change(new_ordered_list, new_group_view, new_view_id)
                elif message.startswith("ADDED TO NETWORK"):
                    print(f"Peer {self.peer_id} received acknowledgement of being added to the network.")
                    self.partOfNetwork = True
                    self.sequencer_peer_id = peer_key
                    self.leader_id = peer_key
                    if(self.multicast_thread_active==False):
                        self.create_multicast_threads()
                    # Start leader heartbeat monitoring as a member
                    if not self.isGroupLeader and self.leader_check_thread is None:
                        self.start_leader_check_thread()
                elif message.startswith("SEQUENCER_REQUEST:"):
                    if self.sequencer_peer_id != self.peer_id:
                        print(f"Error: Peer {self.peer_id} received sequencer request but is not the sequencer.")
                        continue
                    else:
                        # Handle sequencer request
                        original_msg = message.split("SEQUENCER_REQUEST:")[1]
                        print(f"Sequencer request received: {original_msg}")
                        with self.lock:
                            self.sequencer_sequence_number += 1
                            ordered_msg = (self.sequencer_sequence_number, original_msg)
                            self.multicast_message_to_group(ordered_msg, from_peer_id=peer_key)
                elif message.startswith("ELECTION:"):
                    sender_id = message.split(":")[1]
                    self.handle_election_message(sender_id)
                elif message.startswith("OK"):
                    # Received OK from higher node
                    self.received_ok = True
                elif message.startswith("COORDINATOR:"):
                    new_leader = message.split(":")[1]
                    print(f"Received COORDINATOR announcement: {new_leader}")
                    self.leader_id = new_leader
                    self.sequencer_peer_id = new_leader
                    self.isGroupLeader = (new_leader == self.peer_id)
                    if self.isGroupLeader:
                        print(f"Peer {self.peer_id} became the Group Leader via election.")
                        self.start_heartbeat_thread()
                    else:
                        print(f"Peer {self.peer_id} acknowledges new leader {new_leader}.")

        else:
            print(f"No connection found for {peer_key} while receiving message")

    def create_multicast_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.multicast_socket = sock

    def multicast_message_to_group(self, ordered_msg , from_peer_id):
        if not self.multicast_socket:
            self.create_multicast_socket()


        #construct message with metadata
        whole_message = {
            "type": "MULTICAST_MESSAGE_ORDER",
            "message": ordered_msg,
            "from": from_peer_id
        }

        print(f"Multicasting message to group chat: {whole_message}")
        # multicast the message with metadata
        #ip multicast syntax

        try:
            self.multicast_socket.sendto(json.dumps(whole_message).encode(), (MCAST_GRP, MCAST_PORT))
            print(f"Multicast message sent: {whole_message}")
        except Exception as e:
            print(f"Failed to send multicast message: {e}")
        

    def handle_view_change(self, new_ordered_list, new_group_view, new_view_id):
        #if self.view_id < new_view_id:
        print("Entering handle_view_change")
        if self.view_id < new_view_id:
            print(f"Received view change: {new_ordered_list}, {new_group_view}")
            self.orderedPeerList = new_ordered_list
            self.groupView = new_group_view
            print(f"Updated ordered peer list after VIEW_CHANGE: {self.orderedPeerList} and group view: {self.groupView}")
            self.view_id = new_view_id
        else:
            print(f"Ignoring older view change with view ID {new_view_id}, current view ID is {self.view_id}")

    def create_multicast_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        else:
            print("Warning: SO_REUSEPORT not supported on this platform.")
        sock.bind(('', MCAST_PORT))
        mreq = struct.pack(
            "4sl",
            socket.inet_aton(MCAST_GRP),
            socket.INADDR_ANY
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return sock
    
    def listen_for_multicast_messages(self):
        sock = self.create_multicast_listener()
        while True:
            data, addr = sock.recvfrom(4096)
            message = json.loads(data.decode())
            print(f"Received multicast message at {self.peer_id} from {addr}: {message}")
            #process message here
            #how to extract metadata from message?
            message_content = message["message"]
            from_peer = message["from"]

            self.incoming_queue.put(message_content)
            self.process_buffer()

    # -------------------- Sensor Data (Mocked) --------------------
    def start_sensor_thread(self):
        # Only start once we are part of the network and sequencer known
        if not self.partOfNetwork or not self.sequencer_peer_id:
            print("Sensor thread deferred until network ready.")
            # Retry shortly to avoid missing initialization race
            threading.Timer(3.0, self.start_sensor_thread).start()
            return
        if self.sensor_thread and self.sensor_thread.is_alive():
            return
        os.makedirs(self.data_dir, exist_ok=True)
        self.sensor_thread = threading.Thread(target=self._sensor_loop, daemon=True)
        self.sensor_thread.start()
        print(f"Sensor thread started for {self.peer_id} with interval {self.sensor_interval_seconds}s")

    def _sensor_loop(self):
        # Small random initial delay to de-sync sends across peers
        time.sleep(random.uniform(0, 2))
        while True:
            try:
                measurement = self.generate_mock_measurement()
                payload = self.encode_measurement(measurement)
                self.send_message_to_sequencer(payload)
            except Exception as e:
                print(f"Sensor loop error: {e}")
            time.sleep(self.sensor_interval_seconds)

    def generate_mock_measurement(self):
        # Simple random walk around typical indoor values
        temp = round(random.uniform(19.0, 24.0), 2)        # Celsius
        humidity = round(random.uniform(30.0, 60.0), 2)    # %
        pressure = round(random.uniform(990.0, 1030.0), 2) # hPa
        ts = datetime.utcnow().isoformat() + "Z"
        return {
            "sensor_id": self.peer_id,
            "timestamp": ts,
            "temperature": temp,
            "humidity": humidity,
            "pressure": pressure,
        }

    def encode_measurement(self, m):
        # Totally ordered payload format
        # DATA|sensor_id|timestamp|temperature|humidity|pressure
        return f"DATA|{m['sensor_id']}|{m['timestamp']}|{m['temperature']}|{m['humidity']}|{m['pressure']}"

    def handle_ordered_payload(self, payload, seq_id):
        # Handle only our data messages; ignore other demo strings
        if isinstance(payload, str) and payload.startswith("DATA|"):
            parts = payload.split("|")
            if len(parts) != 6:
                print(f"Malformed DATA payload: {payload}")
                return
            _, sensor_id, ts, temp, hum, pres = parts
            self.write_measurement_csv(sensor_id, ts, temp, hum, pres, seq_id)

    def write_measurement_csv(self, sensor_id, ts, temp, hum, pres, seq_id):
        # One CSV per sensor in this peer's directory (replicated across peers)
        file_path = os.path.join(self.data_dir, f"{sensor_id}.csv")
        file_exists = os.path.exists(file_path)
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            with open(file_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["sequence", "timestamp", "sensor_id", "temperature_c", "humidity_pct", "pressure_hpa"]) 
                writer.writerow([seq_id, ts, sensor_id, temp, hum, pres])
        except Exception as e:
            print(f"Failed to write CSV for {sensor_id}: {e}")


    def create_multicast_threads(self):
        self.create_multicast_listener()
        multicast_thread = threading.Thread(target=self.listen_for_multicast_messages, daemon=True)
        multicast_thread.start()
        self.multicast_thread_active = True

    def send_connection_request(self, peer_ip, peer_port, peer_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((peer_ip, peer_port))  # TCP handshake
            # Optional handshake message
            sock.send(b"HELLO from " + f"{self.address}:{self.port} and peer id:{self.peer_id}".encode())
            # Store the connection
            if peer_id not in self.connection_dict:
                self.connection_dict[peer_id] = sock
            print(f"Connected to id : {peer_id} and {peer_ip}:{peer_port}")
            # Start receive thread for this connection
            threading.Thread(target=self.receive_message, args=(peer_id,), daemon=True).start()
        except socket.error as e:
            print("Connection failed:", e)
            raise

    #SEND TO NEW NODE BROADCAST REQUESTS FOR NEW PEERS: CREATE NEW UDP SOCKET 
    #IN ORDER TO GAIN ACCESS TO THE NETWORK
    
    def broadcast_new_peer_request(self, broadcast_ip, broadcast_port, message="NEW_PEER_REQUEST"):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Don't set SO_BROADCAST for localhost - send directly to 127.0.0.1
        message = f"{message}:{self.peer_id}:{self.port}"
        udp_sock.sendto(message.encode(), (broadcast_ip, broadcast_port))
        print(f"Broadcasted new peer request to {broadcast_ip}:{broadcast_port}")
        udp_sock.close()

    def listen_for_broadcasts(self, buffer_size=1024):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if hasattr(socket, 'SO_REUSEPORT'):
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        else:
            print("Warning: SO_REUSEPORT not supported on this platform.")
        udp_sock.bind(('0.0.0.0', BROADCAST_PORT))
        print(f"Listening for broadcasts on port {BROADCAST_PORT}")
        while True:
            data, addr = udp_sock.recvfrom(buffer_size)
            decoded_msg = data.decode()
            parts = decoded_msg.split(":")
            new_peer_id = parts[1]
            peer_port = int(parts[2])
            print(f"Received broadcast from {addr}: {parts[0]} with Peer ID {new_peer_id} on port {peer_port}")
            # Process the broadcast message as needed
            # For example, add the new peer to the group view

            if self.isGroupLeader:
                if decoded_msg.split(':')[0] == "NEW_PEER_REQUEST":
                    if new_peer_id not in self.groupView:
                        self.groupView[new_peer_id] = {
                            'address': addr[0],
                            'port': peer_port,
                            'role': 'member'   
                        }
                        self.peer_last_heartbeat[new_peer_id] = time.time() # Initialize heartbeat tracking
                        print(f"Updated group view: {self.groupView}")

                        #add to peer successor list
                        self.orderedPeerList.append(new_peer_id)
                        #reorder peer list if group leader
                        self.orderedPeerList = sorted(self.orderedPeerList)
                        print(f"Updated ordered peer list: {self.orderedPeerList}")
                        #broadcast_successor_information()
                        self.send_added_to_network_acknowledgement(new_peer_id)
                        


                        self.view_id += 1
                        self.multicast_groupviewandorderedlist_update()

                        # self.send_successor_information(new_peer_id) 
                        #HOW TO FIGURE OUT WHICH PEERS TO SEND NEW SUCCESSOR INFO TO?

                    else:
                        print(f"Peer ID {new_peer_id} already in group view.")
            
    def send_added_to_network_acknowledgement(self, peer_id_to_send_info):
        if self.isGroupLeader:
            message = f"ADDED TO NETWORK"
            peer_info = self.groupView.get(peer_id_to_send_info)
            print(f"Sending acknowledgement to {peer_id_to_send_info} at {peer_info}")
            time.sleep(1)  # Give the peer time to start listening
            try:
                self.send_connection_request(peer_info['address'], peer_info['port'], peer_id_to_send_info)
                self.send_message(peer_id_to_send_info, message)
                self.partOfNetwork = True  # Now part of the network after sending successor info
                if(self.multicast_thread_active==False):
                    self.create_multicast_threads()
                print(f"Peer {self.peer_id} itself is now part of the network.")
            except Exception as e:
                print(f"Failed to send successor information to {peer_id_to_send_info}: {e}")


    def multicast_groupviewandorderedlist_update(self):
        if self.isGroupLeader:
            message = f"VIEW_CHANGE:{self.orderedPeerList}-{self.groupView}-{self.view_id}"
            print(f"Multicasting updated successor information to all peers.")
            for peer_id, peer_info in self.groupView.items():
                if peer_id != self.peer_id:
                    try:
                        if peer_id not in self.connection_dict:
                            self.send_connection_request(peer_info['address'], peer_info['port'], peer_id)
                        self.send_message(peer_id, message)
                        print(f"Sent updated group view change to {peer_id}: {message}")
                    except Exception as e:
                        print(f"Failed to send updated successor information to {peer_id}: {e}")



                                               



    def start_listening_threads(self):
        tcp_listening_socket = self.create_listening_socket()
        self.tcp_thread = threading.Thread(target=self.accept_connection,args=(tcp_listening_socket,), daemon=True)
        self.tcp_thread.start()
        
        #IF group leader not known and partOfNetwork is false, start UDP listener also
        if not self.isGroupLeader and not self.partOfNetwork:
            print("UDP THREAD STARTED FOR NON-GROUP LEADER")
            udp_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
            udp_thread.start()

        if self.isGroupLeader:
            print("UDP THREAD STARTED FOR GROUP LEADER")
            udp_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
            udp_thread.start()

        print("TCP and UDP listeners started.")


    def start_heartbeat_thread(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        self.heartbeat_thread.start()

    def send_heartbeats(self):
        while True:
            time.sleep(self.heartbeat_interval)
            if self.isGroupLeader:
                current_time = time.time()
                # Create a copy of keys to avoid runtime error during modification if a peer fails
                for peer_id in list(self.groupView.keys()):
                    if peer_id != self.peer_id:
                        # Check for timeout
                        last_ack = self.peer_last_heartbeat.get(peer_id, current_time)
                        if current_time - last_ack > self.heartbeat_timeout:
                            print(f"Peer {peer_id} timed out! Last ACK: {last_ack}, Current: {current_time}")
                            self.handle_peer_failure(peer_id)
                            continue

                        try:
                            self.send_message(peer_id, "HEARTBEAT")
                        except Exception as e:
                            print(f"Failed to send heartbeat to {peer_id}: {e}")
                            # logic handled by timeout now, but immediate failure can also be handled
                            self.handle_peer_failure(peer_id)

    def handle_peer_failure(self, peer_id):
        if peer_id in self.groupView:
            del self.groupView[peer_id]
            if peer_id in self.peer_last_heartbeat:
                del self.peer_last_heartbeat[peer_id]
            if peer_id in self.orderedPeerList:
                self.orderedPeerList.remove(peer_id)
            print(f"Peer {peer_id} failed, updated group: {self.groupView}, ordered: {self.orderedPeerList}")
            print(f"Peer {peer_id} failed, updated group: {self.groupView}, ordered: {self.orderedPeerList}")
            
            # Update view ID
            self.view_id += 1
            
            # Send updated successor info to remaining peers
            self.multicast_groupviewandorderedlist_update()

            # If the failed peer was the leader, start election
            if self.leader_id == peer_id:
                print("Leader failure detected, initiating bully election.")
                self.start_bully_election()

    def start_leader_check_thread(self):
        self.leader_check_thread = threading.Thread(target=self.check_leader_heartbeat, daemon=True)
        self.leader_check_thread.start()

    def check_leader_heartbeat(self):
        while True:
            time.sleep(1)
            if not self.isGroupLeader and time.time() - self.last_leader_heartbeat > self.heartbeat_timeout:
                print("Leader heartbeat timeout, starting bully election")
                self.start_bully_election()
                break  # Prevent multiple elections

    def print_status(self):
        print(f"Peer ID: {self.peer_id}")
        print(f"Address: {self.address}")
        print(f"Port: {self.port}")
        print(f"Is Group Leader: {self.isGroupLeader}")
        print(f"Part of Network: {self.partOfNetwork}")
        print(f"Leader ID: {self.leader_id}")
        print(f"Group View: {self.groupView}")
        print(f"View ID: {self.view_id}")
        print(f"Ordered Peer List: {self.orderedPeerList}")
        print(f"Delivered Messages: {self.delivered_messages}")

    # -------------------- Bully Algorithm --------------------
    def get_priority(self, pid):
        # Lexicographic priority on peer_id; tie-breaker on port
        port = self.groupView.get(pid, {}).get('port', 0)
        return (str(pid), int(port))

    def ensure_connection(self, pid):
        if pid == self.peer_id:
            return
        if pid not in self.connection_dict:
            info = self.groupView.get(pid)
            if info:
                try:
                    self.send_connection_request(info['address'], info['port'], pid)
                except Exception as e:
                    print(f"Failed to ensure connection to {pid}: {e}")

    def start_bully_election(self):
        if self.election_active:
            return
        self.election_active = True
        self.received_ok = False
        my_pri = self.get_priority(self.peer_id)
        higher_peers = [pid for pid in self.groupView.keys() if pid != self.peer_id and self.get_priority(pid) > my_pri]
        print(f"Bully election from {self.peer_id}. Higher peers: {higher_peers}")
        # Notify higher peers
        for pid in higher_peers:
            self.ensure_connection(pid)
            try:
                self.send_message(pid, f"ELECTION:{self.peer_id}")
            except Exception as e:
                print(f"Failed to send ELECTION to {pid}: {e}")

        # Wait for OK only if there are higher-priority peers to respond
        if higher_peers:
            start_wait = time.time()
            while time.time() - start_wait < self.election_timeout:
                if self.received_ok:
                    print("Received OK from higher peer, waiting for COORDINATOR.")
                    break
                time.sleep(0.2)

        if not self.received_ok:
            # Become leader
            print(f"No OK received. {self.peer_id} becomes leader.")
            self.announce_coordinator()
        # else: a higher-priority peer responded; this peer now passively waits for a COORDINATOR message handled elsewhere (no retry logic here)

        # Reset election flag after some grace
        def reset_flag():
            time.sleep(self.election_timeout)
            self.election_active = False
        threading.Thread(target=reset_flag, daemon=True).start()

    def handle_election_message(self, sender_id):
        # If we have higher priority, send OK and start own election
        if self.get_priority(self.peer_id) > self.get_priority(sender_id):
            print(f"{self.peer_id} received ELECTION from {sender_id}. Responding OK and starting election.")
            try:
                self.ensure_connection(sender_id)
                self.send_message(sender_id, "OK")
            except Exception as e:
                print(f"Failed to send OK to {sender_id}: {e}")
            # Start own election if not already
            self.start_bully_election()
        else:
            print(f"{self.peer_id} received ELECTION from {sender_id}. Lower priority; acknowledging OK.")
            try:
                self.ensure_connection(sender_id)
                self.send_message(sender_id, "OK")
            except Exception as e:
                print(f"Failed to send OK to {sender_id}: {e}")

    def _initialize_sequencer_sequence_number(self):
        """
        Initialize the sequencer sequence number when this peer becomes leader.

        Instead of resetting to 0 (which can cause collisions with sequence
        numbers that may have already been used by a previous leader), we
        advance the counter to follow the highest sequence number we know
        about locally.
        """
        try:
            # If delivered_messages is a sequence of previously ordered messages,
            # using its length as the starting point helps avoid reusing
            # sequence numbers that have already been observed.
            if hasattr(self, "delivered_messages") and isinstance(self.delivered_messages, (list, tuple)):
                self.sequencer_sequence_number = len(self.delivered_messages)
            else:
                # Fallback: preserve any existing value, or start from 0 if unset.
                self.sequencer_sequence_number = getattr(self, "sequencer_sequence_number", 0)
        except Exception:
            # Defensive fallback in case of unexpected state.
            self.sequencer_sequence_number = getattr(self, "sequencer_sequence_number", 0)

    def announce_coordinator(self):
        self.isGroupLeader = True
        self.leader_id = self.peer_id
        self.sequencer_peer_id = self.peer_id
        # Initialize sequencer sequence number when this peer becomes leader
        self._initialize_sequencer_sequence_number()
        self.start_heartbeat_thread()
        message = f"COORDINATOR:{self.peer_id}"
        for pid, info in self.groupView.items():
            if pid == self.peer_id:
                continue
            self.ensure_connection(pid)
            try:
                self.send_message(pid, message)
            except Exception as e:
                print(f"Failed to send COORDINATOR to {pid}: {e}")
