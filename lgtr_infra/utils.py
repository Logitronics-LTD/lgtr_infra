import datetime
import inspect
import json
import logging
import pkgutil
import types
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


def discover_sub_classes(base_class: Type, package: str | types.ModuleType):
    """
        Auto traverse a package and its sub-packages to find all classes that are
        descendants of a given base class.
    """
    descendants: dict[str, Type] = {}

    # Import the package
    if isinstance(package, str):
        package = __import__(package_name, fromlist=[""])

    # Traverse the package and its sub-packages
    packages_to_traverse = [package]
    for package_current in packages_to_traverse:
        for _module_loader, name, ispkg in pkgutil.walk_packages(package_current.__path__):
            try:
                package_or_module = __import__(f"{package_current.__name__}.{name}", fromlist=[""])
            except (ModuleNotFoundError, NameError):
                logging.warning(f"Skipping module {name} in package {package_current.__name__} (Unable to import)")
                continue

            if ispkg:
                packages_to_traverse.append(package_or_module)
            else:
                # Import the module and get its classes
                for cls_name, cls in inspect.getmembers(package_or_module, inspect.isclass):
                    # Check if the class is a descendant of the base class
                    if issubclass(cls, base_class):
                        descendants[cls_name] = cls

    return descendants
