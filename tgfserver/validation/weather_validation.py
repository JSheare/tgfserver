"""A module containing the pydantic models used to validate and transform input from the weather website."""
import pydantic
from pydantic import BaseModel
from typing import Annotated


def is_valid_clock(value):
    try:
        if not isinstance(value, str):
            raise Exception

        meridiem = value.split()[1]
        nums = value.split()[0].split(':')
        hour = int(nums[0])
        minute = int(nums[1])
        if not (meridiem == 'AM' or meridiem == 'PM'):
            raise Exception

        if not 0 < hour <= 12:
            raise Exception

        if not minute <= 60:
            raise Exception

    except Exception:
        raise ValueError('input must be a clock string of the form HH:MM AM/PM.')

    # Converting from 12 hour time to 24 hour time
    if meridiem == 'AM' and hour == 12:  # midnight
        hour = 0
    elif meridiem == 'PM' and hour == 12:  # noon
        pass
    elif meridiem == 'PM':  # PM conversion
        hour += 12

    return hour * 60**2 + minute * 60


class WeatherModel(BaseModel):
    """A model for validating rows coming from the weather website."""
    measurement_time: Annotated[int, pydantic.BeforeValidator(is_valid_clock)]
    condition: str
