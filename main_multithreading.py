import Peer
import multiprocessing
import socket
import time
import Peer_utils


#initialize two peers
#start peer1 listening socket in a separate process
def peer1stuff(peer1address, peer1port):
    # Create peer instance in this process (auto-generated UUID)
    peer1 = Peer.Peer(address=peer1address, port=peer1port)
    peer1.start()


        



#start peer2 listening socket in a separate process
def peer2stuff(peer2address, peer2port):
    # Create peer instance in this process (auto-generated UUID)
    peer2 = Peer.Peer(address=peer2address, port=peer2port)
    peer2.start()

#start peer2 listening socket in a separate process
def peer3stuff(peer3address, peer3port):
    # Create peer instance in this process (auto-generated UUID)
    peer3 = Peer.Peer(address=peer3address, port=peer3port)
    peer3.start()


if __name__ == '__main__':

    peer1address = Peer_utils.get_local_ip()
    peer1port = Peer_utils.get_free_port()
     
    peer2address = Peer_utils.get_local_ip()
    peer2port = Peer_utils.get_free_port()

    peer3address = Peer_utils.get_local_ip()
    peer3port = Peer_utils.get_free_port()
        
    process1 = multiprocessing.Process(
        target=peer1stuff,
        args=(peer1address, peer1port)
    )
    process1.daemon = True

    process2 = multiprocessing.Process(
        target=peer2stuff,
        args=(peer2address, peer2port)
    )
    process2.daemon = True

    process3 = multiprocessing.Process(
        target=peer3stuff,
        args=(peer3address, peer3port)
    )
    process3.daemon = True

    process2.start()
    time.sleep(5)  # Ensure peer2 starts before peer1
    process1.start()
    time.sleep(5)  # Ensure peer1 starts before peer3
    process3.start()



    process2.join()
    process1.join()
    process3.join()

    


    

    
    