"""A module containing the pydantic models used to validate and transform information from the config file."""
import logging
import pydantic
import re
from cron_converter import Cron
from typing import Any, Annotated
from typing_extensions import Self


def is_valid_day_sec(value: int) -> int:
    if not 0 <= value <= 86400:
        raise ValueError('input should be a valid second of day.')

    return value


def is_valid_day_of_week(value: int) -> int:
    if not 1 <= value <= 7:
        raise ValueError('input should be a number 1-7 corresponding to a day of the week.')

    return value

def is_valid_ipv4(value: str) -> str:
    if not re.match(r'^(((?!25?[6-9])[12]\d|[1-9])?\d\.?\b){4}$', value):
        raise ValueError('input should be a valid IPv4 address.')

    return value


def is_valid_cron(value: Any) -> Cron:
    if not isinstance(value, str):
        raise ValueError('input should be a valid crontab-style string.')

    return Cron(value)


def is_valid_log_level(value: Any) -> int:
    if not isinstance(value, str):
        raise ValueError('input should be a string corresponding to a log level.')

    level_mappings = logging.getLevelNamesMapping()
    value = value.upper()
    if value not in level_mappings:
        raise ValueError('input should be a string corresponding to a log level.')

    return level_mappings[value]


def is_valid_port(value: int) -> int:
    if not 0 <= value <= 65535:
        raise ValueError('input should be a valid TCP port.')

    return value


def is_valid_gmail(value: str) -> str:
    if not re.match(r'^[a-z0-9](\.?[a-z0-9]){5,}@g(oogle)?mail\.com$', value):
        raise ValueError('input must be a gmail address.')

    return value


class ManagerModel(pydantic.BaseModel):
    """A model for validating the manager service section of the config file."""
    log_level: Annotated[int, pydantic.BeforeValidator(is_valid_log_level)]
    start_limit_interval_sec: pydantic.PositiveFloat
    start_limit_burst: pydantic.PositiveInt
    restart_sec: pydantic.PositiveFloat
    shutdown_timeout_sec: pydantic.PositiveFloat
    db_host: Annotated[str, pydantic.AfterValidator(is_valid_ipv4)]
    db_port: Annotated[int, pydantic.AfterValidator(is_valid_port)]
    db_connect_timeout_sec: pydantic.PositiveInt
    db_name: str
    db_user: str
    db_password: str
    spreadsheet_id: str
    sheets_api_key: str
    db_update_time: Annotated[Cron, pydantic.BeforeValidator(is_valid_cron)]
    scrape_weather: bool
    scrape_timeout_sec: pydantic.PositiveInt

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class DispatcherModel(pydantic.BaseModel):
    """A model for validating the dispatcher service section of the config file."""
    log_level: Annotated[int, pydantic.BeforeValidator(is_valid_log_level)]
    service_host: Annotated[str, pydantic.AfterValidator(is_valid_ipv4)]
    service_port: Annotated[int, pydantic.AfterValidator(is_valid_port)]
    receive_timeout_sec: pydantic.PositiveFloat
    max_msg_size_bytes: pydantic.PositiveInt
    ip_cache_size_bytes: pydantic.PositiveInt
    ip_period_sec: pydantic.PositiveInt
    max_auth_attempts: pydantic.PositiveInt
    db_pool_size: pydantic.PositiveInt
    db_host: Annotated[str, pydantic.AfterValidator(is_valid_ipv4)]
    db_port: Annotated[int, pydantic.AfterValidator(is_valid_port)]
    db_connect_timeout_sec: pydantic.PositiveInt
    db_name: str
    db_user: str
    db_password: str
    start_day: Annotated[int, pydantic.AfterValidator(is_valid_day_of_week)]
    end_day: Annotated[int, pydantic.AfterValidator(is_valid_day_of_week)]
    start_time_sec: Annotated[int, pydantic.AfterValidator(is_valid_day_sec)]
    end_time_sec: Annotated[int, pydantic.AfterValidator(is_valid_day_sec)]
    max_slot_size_sec: pydantic.PositiveInt
    gap_size_sec: pydantic.PositiveInt
    stale_stats_thresh_sec: pydantic.PositiveInt
    scheduling_deadline: Annotated[Cron, pydantic.BeforeValidator(is_valid_cron)]
    stale_check_in_thresh_sec: pydantic.PositiveInt
    storage_warning_thresh: pydantic.PositiveFloat
    gmail_address: Annotated[str, pydantic.AfterValidator(is_valid_gmail)]
    gmail_api_credentials: pydantic.Json[dict]
    digest_time: Annotated[Cron, pydantic.BeforeValidator(is_valid_cron)]

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    @pydantic.model_validator(mode='after')
    def check_scheduler_times(self) -> Self:
        if self.start_time_sec > self.end_time_sec:
            raise ValueError('scheduler end time is greater than start time.')

        return self
