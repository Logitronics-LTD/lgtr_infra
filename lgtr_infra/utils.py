import datetime
import functools
import inspect
import json
import logging
import pkgutil
import subprocess
import types
import uuid
import warnings
from typing import TypeVar, Type, Iterable, Any, Callable

import simple_parsing

T = TypeVar('T')

IMAGE_SUFFIXES = {
    '.jpg', '.jpeg', '.jfif', '.png', '.bmp', '.gif', '.tiff', '.tif', '.webp'
}


def shell(cmd, **kwargs):
    return subprocess.check_call(cmd, shell=True, **kwargs)


def is_image_extension(suffix: str):
    ext = suffix.lower()
    return ext in IMAGE_SUFFIXES


def parse_dataclass_args(parse_type: Type[T], args: list[str] = None) -> T:
    warnings.warn(
        "parse_dataclass_args is deprecated, use `simple_parsing.parse()` directly instead", DeprecationWarning
    )
    return simple_parsing.parse(parse_type, args=args)


def dedup_with_serialization(
    list_objects: Iterable[Any], *, dumps: Callable = None, loads: Callable = None
) -> list[Any]:
    dumps = dumps or functools.partial(json.dumps, sort_keys=True)
    loads = loads or json.loads

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


def htimestamp_parse(str_datetime: str):
    try:
        dt = datetime.datetime.strptime(str_datetime, '%Y%m%d_%H%M%S.%f')

    except ValueError:
        dt = datetime.datetime.strptime(str_datetime, '%Y%m%d_%H%M%S_%f')

    return dt


def short_uuid4():
    return str(uuid.uuid4())[:8]


def htimestamp_uuid():
    return htimestamp() + '_' + short_uuid4()


def traverse_package(
    package: str | types.ModuleType,
    callback_module: Callable[[types.ModuleType], Any] = None,
    callback_package: Callable[[types.ModuleType], Any] = None
):
    """
        Auto traverse a package and its sub-packages and modules, and call a callback
    """
    # Import the package
    if isinstance(package, str):
        package = __import__(package, fromlist=[""])

    # Traverse the package and its sub-packages
    packages_to_traverse = [package]
    for package_current in packages_to_traverse:
        for _module_loader, name, is_pkg in pkgutil.walk_packages(package_current.__path__):
            try:
                package_or_module = __import__(
                    f"{package_current.__name__}.{name}", fromlist=[""])
            except (ModuleNotFoundError, NameError):
                logging.warning(f"Skipping module {name} in package {
                                package_current.__name__} (Unable to import)")
                continue

            if is_pkg:
                packages_to_traverse.append(package_or_module)
                if callback_package is not None:
                    callback_package(package_or_module)
            else:
                if callback_module is not None:
                    callback_module(package_or_module)


def discover_package_classes(package: str | types.ModuleType, criteria: Callable[[Type], bool] = None):
    discovered_classes = []

    def _callback_module(module: types.ModuleType):
        for _cls_name, cls in inspect.getmembers(module, inspect.isclass):
            if criteria is None or criteria(cls):
                discovered_classes.append(cls)

    traverse_package(package, callback_module=_callback_module)
    return discovered_classes


def discover_package_methods(package: str | types.ModuleType, criteria: Callable[[Any], bool] = None):
    discovered_methods = []

    def _callback_module(module: types.ModuleType):
        for _func_name, func in inspect.getmembers(module, inspect.isfunction):
            if criteria is None or criteria(func):
                discovered_methods.append(func)

    traverse_package(package, callback_module=_callback_module)
    return discovered_methods


def transform_recursive(obj: Any, func: Callable) -> Any:
    """
        Recursively transforms an object using the provided function.

        :param: obj: The object to transform. Supports primitives such as dict, list, etc.
        :param: func: The transformation function to apply to each primitive element.
    """
    if isinstance(obj, dict):
        obj = {k: transform_recursive(v, func) for k, v in obj.items()}
    elif isinstance(obj, list):
        obj = [transform_recursive(item, func) for item in obj]
    elif isinstance(obj, tuple):
        obj = tuple(transform_recursive(item, func) for item in obj)
    elif isinstance(obj, set):
        obj = {transform_recursive(item, func) for item in obj}

    return func(obj)
