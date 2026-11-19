"""A module containing the pydantic models used to validate and transform input from the TGF Group Info spreadsheet."""
import datetime
import pydantic
from typing import Annotated

import tgfserver.validation.validator_funcs as v


class GeneralModel(pydantic.BaseModel):
    """A model for validating rows coming from the General sheet."""
    data_root: Annotated[str, pydantic.AfterValidator(v.is_valid_directory_path)]
    format_name: str


class InstrumentsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Instruments sheet."""
    instrument_name: Annotated[str, pydantic.AfterValidator(v.is_valid_instrument_name)]
    subdir: Annotated[str, pydantic.AfterValidator(v.is_valid_subdirectory_path)]


class ScintillatorsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Scintillators sheet."""
    scint_name: str
    scint_priority: pydantic.PositiveInt
    plot_color: str


class ConfigurationsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Configurations sheet."""
    instrument_name: Annotated[str, pydantic.AfterValidator(v.is_valid_instrument_name)]
    after_date: Annotated[datetime.datetime, pydantic.BeforeValidator(v.is_valid_yymmdd)]
    scint_name: str
    erc: str
    format_name: str
    long_event_search: bool


class DeploymentsModel(pydantic.BaseModel):
    """A model for validating rows coming from the Deployments sheet."""
    location: str
    instrument_name: Annotated[str, pydantic.AfterValidator(v.is_valid_instrument_name)]
    start_date: Annotated[datetime.datetime, pydantic.BeforeValidator(v.is_valid_yymmdd)]
    end_date: Annotated[datetime.datetime, pydantic.BeforeValidator(v.is_valid_yymmdd)]
    tz_identifier: str
    weather_station: str
    sounding_station: str
    latitude: pydantic.PositiveFloat
    longitude: pydantic.PositiveFloat
    altitude: float
    notes: str
