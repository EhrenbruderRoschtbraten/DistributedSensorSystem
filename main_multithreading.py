import Peer
import multiprocessing
import socket
import time
import identity


#initialize two peers
#start peer1 listening socket in a separate process
def peer1stuff(peer1):
        # peer1.send_message(f"peer_5002","Hello from Peer 1")
        # peer1.listen_for_broadcasts(6000)

        # peer1.start_listening_threads()
        # time.sleep(2)  # Give peer2 time to start listening
        # peer1.broadcast_new_peer_request('127.0.0.1', 9999, f"NEW_PEER_REQUEST:{peer1.peer_id}")
        # time.sleep(5)
        # peer1.broadcast_new_peer_request('127.0.0.1', 9999, "Another broadcast")

        peer1.start()

        



#start peer2 listening socket in a separate process
def peer2stuff(peer2):
        # peer2.start_listening_threads()
        # time.sleep(10)  # Keep process alive to receive broadcasts

        # print("now sending connection request from peer2 to peer1")
        #RESOURCE DISCOVERY FIRST
        #HEARING FOR BROADCAST REQUESTS

        # peer2.send_connection_request('127.0.0.1', 5001)
        # peer2.send_message(f"peer_5001","Hello from Peer 2")
        # peer2.receive_message(f"peer_5001")
        # peer2.broadcast_new_peer_request('255.255.255.255', 6000, "New peer joined the network")

        peer2.start()

if __name__ == '__main__':
    
    path1 = "peer1_identity/peer1_identity.txt"
    peer_id1 = identity.load_or_generate_peer_id(PEER_ID_FILE=path1)
    peer1 = Peer.Peer(peer_id=peer_id1, address='127.0.0.1', port=5001)

    path2 = "peer2_identity/peer2_identity.txt"
    peer_id2 = identity.load_or_generate_peer_id(PEER_ID_FILE=path2)
    peer2 = Peer.Peer(peer_id=peer_id2, address='127.0.0.1', port=5002)


    process1 = multiprocessing.Process(target=peer1stuff,args=(peer1,))
    process2 = multiprocessing.Process(target=peer2stuff,args=(peer2,))

    process2.start()
    process1.start()


    process2.join()
    process1.join()


    


    

    
    