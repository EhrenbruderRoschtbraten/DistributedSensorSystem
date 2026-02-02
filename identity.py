"""Simple peer identity management.

Generates or loads a persistent UUID for a peer from a local file.
"""

import uuid
from pathlib import Path

PEER_ID_FILE = "peer_id.txt"


def load_or_generate_peer_id(PEER_ID_FILE=PEER_ID_FILE):
    """Load a peer UUID from disk or generate a new one.

    Args:
        PEER_ID_FILE (str): Path to the file storing the peer UUID.

    Returns:
        str: The peer UUID, either loaded from disk or newly generated.
    """
    path = Path(PEER_ID_FILE)

    if path.exists():
        return path.read_text().strip()
    else:
        new_id = str(uuid.uuid4())
        path.write_text(new_id)
        return new_id