import base64
from typing import Union, Optional

import numpy as np


def array_to_base64url(array: Union[np.ndarray | bytes]) -> str:
    if isinstance(array, bytes):
        embeddings_bytes = array

    else:
        if array.dtype == bool:
            array = np.packbits(array)

        embeddings_bytes = array.tobytes()

    embeddings_base64 = base64.urlsafe_b64encode(embeddings_bytes).decode('utf-8')
    return embeddings_base64


def array_from_base64url(filename: str, dtype: type, *, length: int = None) -> np.ndarray:
    embeddings_base64 = filename
    embeddings_bytes = base64.urlsafe_b64decode(embeddings_base64)
    if dtype == np.dtype(bool):
        embeddings = np.unpackbits(np.frombuffer(embeddings_bytes, dtype=np.uint8)).astype(bool)
    else:
        embeddings = np.frombuffer(embeddings_bytes, dtype)

    if length is not None:
        embeddings = embeddings[:length]

    return embeddings
