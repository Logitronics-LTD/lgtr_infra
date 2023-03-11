import datetime
import json
import uuid
from typing import Optional, TypeVar, Type, Iterable, Any, Callable

from simple_parsing import ArgumentParser

T = TypeVar('T')

IMAGE_SUFFIXES = {'.jpg', '.jpeg', '.jfif', '.png', '.bmp', '.gif', '.tiff', '.tif', '.webp'}


def is_image_extension(suffix: str):
    ext = suffix.lower()
    return ext in IMAGE_SUFFIXES


def parse_dataclass_args(parse_type: Type[T], args: list[str] = None) -> T:
    parser = ArgumentParser()
    parser.add_arguments(parse_type, dest='dataclass')

    return parser.parse_args(args).dataclass


def dedup_with_serialization(
    list_objects: Iterable[Any], *, dumps: Callable = None, loads: Callable = None
) -> list[Any]:
    if dumps is None:
        dumps = lambda x: json.dumps(x, sort_keys=True)

    if loads is None:
        loads = json.loads

    set_serialized = set(dumps(obj) for obj in list_objects)
    return [
        loads(serialized) for serialized in set_serialized
    ]


def htimestamp(time=None, *, with_ms=True):
    if time is None:
        time = datetime.datetime.utcnow()

    if with_ms:
        return time.strftime('%Y%m%d_%H%M%S_%f')[:-3]
    else:
        return time.strftime('%Y%m%d_%H%M%S')


def htimestamp_parse(str_datetime: Optional[str], default=None):
    if str_datetime:
        try:
            return datetime.datetime.strptime(str_datetime, '%Y%m%d_%H%M%S.%f')

        except ValueError:
            return datetime.datetime.strptime(str_datetime, '%Y%m%d_%H%M%S_%f')
    else:
        return default


def short_uuid4():
    return str(uuid.uuid4())[:8]


def htimestamp_uuid():
    return htimestamp() + '_' + short_uuid4()
