import importlib.util
import json
import logging
import logging.config
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar, Type, Optional, Union

import dacite

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class GlobalConfigBase:
    env_vars: dict = field(default_factory=dict)
    logging: Optional[dict] = None

    def setup(self):
        os.environ.update(self.env_vars)

        if self.logging is not None:
            logging.config.dictConfig(self.logging)


def search_file_in_paths(filename: Union[Path, str], paths: Optional[list[Path]] = None, *, include_cwd=True):
    """
        Search for a filename in a list of paths,
        - If filename is absolute, return it
        - If filename is relative, search for it in the given paths
        - If filename is not found, return None
        - Use system PATH if paths is None
    """

    filename = Path(filename)
    if filename.is_absolute():
        return filename

    if paths is None:
        paths = os.environ["PATH"].split(os.pathsep)

    if include_cwd:
        paths = [os.getcwd()] + paths

    for path in paths:
        full_path = Path(path) / filename
        if full_path.exists():
            return full_path

    return None


def import_module(path: Union[Path, str]):
    """
        Import a module from a given path
        Module will be imported using the current PYTHONPATH even if it isn't on it
    """

    path = Path(path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def update_config_py(config: T, path_config: Union[Path, str, None], *, ignore_non_existing_file=False) -> T:
    """
        Update a config object using a python file, optionally ignore the file if it doesn't exist
        Given config will be passed to `update()` function of the loaded module
        Python file can be completely standalone, and not part of the PYTHONPATH
    """

    # Ignore if path is None
    if path_config is None:
        return config

    # Handle optional path or file doesn't exist
    path_config = Path(path_config)
    if ignore_non_existing_file and not path_config.exists():
        logger.warning(f'Skipped loading file; Config file not found: {path_config}')
        return config

    # Path exists, load the module
    config_module = import_module(path_config)
    config_module.update(config)
    return config


def load_config(config_class: Type[T], path_config: Union[Path, str]) -> T:
    """
        Load a structured config file, using given class

        Note:
            - Supports JSON, YAML, HOCON, and Python files
            - Python files must have an `update` function that returns an instance of the given config class
    """

    path_config = Path(path_config)
    suffix = path_config.suffix.lower()

    match suffix.lower():
        case '.json':
            obj = json.loads(path_config.read_text())

        case '.yaml' | '.yml':
            obj = _load_yaml(path_config)

        case '.conf':
            obj = _load_hocon(path_config)

        case '.py':
            config = config_class()
            return update_config_py(config, path_config)

        case _:
            raise ValueError(f'Unsupported config file type: `{suffix}`')

    config = dacite.from_dict(config_class, obj)
    return config


def _load_hocon(path_config):
    try:
        # noinspection PyUnresolvedReferences
        import pyhocon

    except ImportError as e:
        raise ImportError('pyhocon is required to load HOCON files') from e
    obj = pyhocon.ConfigFactory.parse_file(path_config).as_plain_ordered_dict()
    return obj


def _load_yaml(path_config):
    # # noinspection PyUnresolvedReferences
    # import yaml
    try:
        # noinspection PyUnresolvedReferences
        import yaml
    except ImportError as e:
        raise ImportError('pyyaml is required to load YAML files') from e

    obj = yaml.safe_load(path_config.read_text())
    return obj
