"""A module containing helper functions used by various parts of the tgfserver application."""
import json
import os
import pathlib
import sys
from collections import deque
from typing import Any, Dict


def expand_path(path: str, make_dir: bool = True) -> pathlib.Path:
    """A function that expands the given path with user information and returns it as a pathlib.Path instance.

    Parameters
    ----------
    path : str
        The path to be expanded.
    make_dir : bool, optional
        An optional flag that, if True, will create the given path if it does not already exist.

    Returns
    -------
    pathlib.Path
        A pathlib.Path instance containing the expanded path.

    """

    expanded_path = pathlib.Path(path.replace('<uid>', str(os.geteuid()))).expanduser()
    if make_dir and not expanded_path.is_dir():
        expanded_path.mkdir(parents=True)

    return expanded_path


def get_obj_size(obj: Any) -> int:
    """A function that returns the size of the passed object (including subobjects) in bytes.

    Note: objects that aren't pure Python might not yield accurate measurements (so pass things like numpy arrays and
    pandas dataframes, which have components written in other languages, at your own risk).

    Parameters
    ----------
    obj : Any
        The object to get the size of.

    Returns
    -------
    int
        The size of the passed object in bytes.

    """

    size = 0
    visited = set()
    stack = deque()
    stack.append(obj)
    while len(stack) > 0:
        item = stack.pop()
        item_id = id(item)
        if item_id in visited:
            continue

        size += sys.getsizeof(item)
        visited.add(item_id)
        if isinstance(item, dict):
            for key in item.keys():
                stack.append(key)

            for value in item.values():
                stack.append(value)

        elif hasattr(item, '__dict__'):
            # Iterating through all the object's members
            stack.append(item.__dict__)
        elif hasattr(item, '__iter__') and not isinstance(item, (str, bytes, bytearray)):
            for subitem in item:
                stack.append(subitem)

    return size


def read_json_file(file: str) -> Dict[Any, Any]:
    """A function that reads the given JSON file and returns it as a dictionary.

    Parameters
    ----------
    file : str
        The file to be read.

    Returns
    -------
    Dict[Any, Any]
        The json file's contents as a dictionary.

    """

    with open(file, 'r') as f:
        result = json.load(f)

    return result


def write_json_file(dictionary: Dict[Any, Any], file: str) -> None:
        """A function that writes the given dictionary as JSON to the given file.

        Parameters
        ----------
        dictionary : Dict[Any, Any]
            The dictionary to be written.

        file : str
            The name of the file to write the dictionary to as JSON.

        """

        with open(file, 'w') as f:
            json.dump(dictionary, f)
