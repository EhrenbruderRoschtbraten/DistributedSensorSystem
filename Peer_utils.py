import socket

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))   # 0 = ask OS for free port
    port = s.getsockname()[1]
    s.close()
    return port

def get_broadcast_address(ip):
    parts = ip.split('.')
    parts[-1] = '255'
    return '.'.join(parts)

# print("Local IP Address:", get_local_ip())
# print("Free Port:", get_free_port())