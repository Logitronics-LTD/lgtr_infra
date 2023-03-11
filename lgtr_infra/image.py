from typing import Optional, Sequence

# Note: These modules are not a mandatory dependency of the package,
#   It is the user's responsibility to install them if they want to use this module

# noinspection PyUnresolvedReferences
import cv2

# noinspection PyUnresolvedReferences
import numpy as np


def calc_resize_dims(
    image_shape: Sequence[int], width: Optional[int] = None, height: Optional[int] = None, downsample_only=True
):
    h0, w0 = image_shape[:2]

    if width is not None and height is not None:
        r_h = height / float(h0)
        r_w = width / float(w0)

        if r_h < r_w:
            w1, h1 = (int(w0 * r_h), height)
        else:
            w1, h1 = (width, int(h0 * r_w))

    elif width is None:
        r = height / float(h0)
        w1, h1 = (int(w0 * r), height)
    elif height is None:
        r = width / float(w0)
        w1, h1 = (width, int(h0 * r))
    else:
        w1, h1 = (w0, h0)

    if downsample_only and (w1 >= w0 or h1 >= h0):
        w1, h1 = w0, h0

    return h1, w1


def image_resize(
    image: np.ndarray, width: int = None, height: int = None, *, interpolation=cv2.INTER_AREA, downsample_only=True
) -> np.ndarray:
    h1, w1 = calc_resize_dims(image.shape, width, height, downsample_only)
    return cv2.resize(image, (w1, h1), interpolation=interpolation)
