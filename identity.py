import uuid
from pathlib import Path

PEER_ID_FILE = "peer_id.txt"


def load_or_generate_peer_id(PEER_ID_FILE=PEER_ID_FILE):
    path = Path(PEER_ID_FILE)

    if path.exists():
        return path.read_text().strip()
    else:
        new_id = str(uuid.uuid4())
        path.write_text(new_id)
        return new_id