"""A module containing the pydantic models used to validate and transform input from the TGF Group Info spreadsheet."""
import datetime
import pydantic
import re
from typing import Any, Annotated


def is_valid_directory_path(value: str) -> str:
    value.replace('\\', '/')
    if value != '' and value[-1] == '/':
        value = value[:-1]

    if value != '' and not re.match(r'^((/[a-zA-Z0-9_-]+)+|/)$', value):
        raise ValueError('input should be a valid absolute directory path.')

    return value


def is_valid_instrument_name(value: str) -> str:
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


def is_valid_yymmdd(value: Any) -> datetime.datetime:
    try:
        if not (isinstance(value, str) and len(value) == 6):
            raise ValueError

        if value == '000000':
            return datetime.datetime.fromtimestamp(0, datetime.UTC)
        elif value == '999999':
            # Maximum possible value for yymmdd in this century
            return datetime.datetime.fromtimestamp(4102387200, datetime.UTC)

        return datetime.datetime.strptime(value, '%y%m%d').replace(tzinfo=datetime.UTC)
    except ValueError:
        raise ValueError('input should be a valid date in YYMMDD format.')


class GeneralModel(pydantic.BaseModel):
    """A model for validating rows coming from the General sheet."""
    data_root: Annotated[str, pydantic.AfterValidator(is_valid_directory_path)]
    format_name: str


class InstrumentsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Instruments sheet."""
    instrument_name: Annotated[str, pydantic.AfterValidator(is_valid_instrument_name)]
    subdir: Annotated[str, pydantic.AfterValidator(is_valid_subdirectory_path)]


class ScintillatorsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Scintillators sheet."""
    scint_name: str
    scint_priority: pydantic.PositiveInt
    plot_color: str


class ConfigurationsModel(pydantic.BaseModel):
    instrument_name: Annotated[str, pydantic.AfterValidator(is_valid_instrument_name)]
    after_date: Annotated[datetime.datetime, pydantic.BeforeValidator(is_valid_yymmdd)]
    scint_name: str
    erc: str
    format_name: str
    long_event_search: bool


class DeploymentsModel(pydantic.BaseModel):
    location: str
    instrument_name: Annotated[str, pydantic.AfterValidator(is_valid_instrument_name)]
    start_date: Annotated[datetime.datetime, pydantic.BeforeValidator(is_valid_yymmdd)]
    end_date: Annotated[datetime.datetime, pydantic.BeforeValidator(is_valid_yymmdd)]
    tz_identifier: str
    weather_station: str
    sounding_station: str
    latitude: pydantic.PositiveFloat
    longitude: pydantic.PositiveFloat
    altitude: float
    notes: str
