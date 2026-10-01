"""A module containing the pydantic models used to validate and transform messages to the instrument dispatcher."""
import datetime
import pydantic
from annotated_types import Len
from typing import Any, Annotated, List


class MessageModel(pydantic.BaseModel):
    """A model for validating messages from the client"""
    type: Annotated[str, pydantic.StringConstraints(max_length=100)]
    payload: dict[str, Any]


class AuthenticationModel(pydantic.BaseModel):
    """A model for validating message payloads during an authentication operation."""
    instrument: Annotated[str, pydantic.StringConstraints(max_length=100)]
    password: Annotated[str, pydantic.StringConstraints(max_length=100)]


class CheckInModel(pydantic.BaseModel):
    """A model for validating message payloads during a check in operation."""
    storage_frac: pydantic.PositiveFloat
    gps: bool


class NegotiationModel(pydantic.BaseModel):
    """A model for validating message payloads during a file transfer negotiation operation."""
    measured_rates: Annotated[List[pydantic.PositiveFloat], Len(max_length=50)]
    timestamps: Annotated[List[datetime.datetime], Len(max_length=50)]
    total_bytes: pydantic.PositiveInt

    @pydantic.model_validator(mode='after')
    def check_lens(self):
        if len(self.measured_rates) != len(self.timestamps):
            raise ValueError('the number of measured rates must match the number of timestamps.')

        return self
