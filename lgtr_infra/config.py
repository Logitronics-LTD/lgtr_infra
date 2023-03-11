import importlib.util
import json
import os
from pathlib import Path
from typing import TypeVar, Type, Optional, Union

import dacite

T = TypeVar('T')


def search_file_in_paths(filename: str, paths: Optional[list[Path]] = None):
    paths = paths or [os.getcwd()] + os.environ["PATH"].split(os.pathsep)

    for path in paths:
        full_path = Path(path) / filename
        if full_path.exists():
            return full_path

    return None


def import_module(path: Union[Path, str]):
    path = Path(path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def chain_module_if_exists(config: T, path: Union[Path, str] = None):
    path = Path(path)
    if path is not None and path.exists():
        return import_module(path).update(config)


def load_config(config_class: Type[T], path_config: Path) -> T:
    """
        Load a structured config file, using given class

        Note:
            - Supports JSON, YAML, HOCON, and Python files
            - Python files must have a `main` function that returns an instance of the given config class
            - Imports are local to prevent polluting the global dependency tree
    """

    suffix = path_config.suffix.lower()

    match suffix:
        case '.json':
            obj = json.loads(path_config.read_text())

        case '.yaml' | '.yml':
            # noinspection PyUnresolvedReferences
            import yaml
            obj = yaml.safe_load(path_config.read_text())

        case '.conf':
            # noinspection PyUnresolvedReferences
            import pyhocon
            obj = pyhocon.ConfigFactory.parse_file(path_config).as_plain_ordered_dict()

        case '.py':
            config_module = import_module(path_config)
            config = config_class()
            config_module.update(config)
            return config

        case _:
            raise ValueError(f'Unknown config file type: {suffix}')

    if isinstance(obj, dict):
        config = dacite.from_dict(config_class, obj)
    else:
        config = obj

    return config
