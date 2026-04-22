"""A module containing the pydantic models used to validate and transform input from the TGF Group Info spreadsheet."""
import pydantic
import re
from typing import Annotated


def is_valid_directory_path(value: str) -> str:
    value.replace('\\', '/')
    if value != '' and value[-1] == '/':
        value = value[:-1]

    if value != '' and not re.match(r'^((/[a-zA-Z0-9_-]+)+|/)$', value):
        raise ValueError('input should be a valid absolute directory path.')

    return value


def is_valid_instrument(value: str) -> str:
    if not value.isalnum():
        raise ValueError('input should consist of only letters and numbers.')

    value = value.upper()
    return value


def is_valid_subdirectory_path(value: str) -> str:
    value.replace('\\', '/')
    if value != '' and value[-1] == '/':
        value = value[:-1]

    if value != '' and not re.match(r'^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$', value):
        raise ValueError('input should be a valid subdirectory path.')

    return value


class GeneralModel(pydantic.BaseModel):
    """A model for validating rows coming from the General sheet."""
    data_root : Annotated[str, pydantic.AfterValidator(is_valid_directory_path)]
    format_name : str


class InstrumentsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Instruments sheet."""
    instrument_name : Annotated[str, pydantic.AfterValidator(is_valid_instrument)]
    subdir : Annotated[str, pydantic.AfterValidator(is_valid_subdirectory_path)]
