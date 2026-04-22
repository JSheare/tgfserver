"""A module containing helper functions used by various parts of the tgfserver application."""
import json
import os
import pathlib
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
