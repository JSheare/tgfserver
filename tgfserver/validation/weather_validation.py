"""A module containing the pydantic models used to validate and transform input from the weather website."""
import pydantic
from pydantic import BaseModel
from typing import Annotated

import tgfserver.validation.validator_funcs as v


class WeatherModel(BaseModel):
    """A model for validating rows coming from the weather website."""
    measurement_time: Annotated[int, pydantic.BeforeValidator(v.is_valid_clock)]
    condition: str
