"""A module containing the pydantic models used to validate and transform information from the config file."""
import pydantic
from cron_converter import Cron
from typing import Annotated
from typing_extensions import Self

import tgfserver.validation.validator_funcs as v


class ManagerModel(pydantic.BaseModel):
    """A model for validating the manager service section of the config file."""
    log_level: Annotated[int, pydantic.BeforeValidator(v.is_valid_log_level)]
    start_limit_interval_sec: pydantic.PositiveFloat
    start_limit_burst: pydantic.PositiveInt
    restart_sec: pydantic.PositiveFloat
    shutdown_timeout_sec: pydantic.PositiveFloat
    do_db_update: bool
    db_host: Annotated[str, pydantic.AfterValidator(v.is_valid_ipv4)]
    db_port: Annotated[int, pydantic.AfterValidator(v.is_valid_port)]
    db_connect_timeout_sec: pydantic.PositiveInt
    db_name: str
    db_user: str
    db_password: str
    spreadsheet_id: str
    sheets_api_key: str
    db_update_time: Annotated[Cron, pydantic.BeforeValidator(v.is_valid_cron)]
    scrape_weather: bool
    scrape_timeout_sec: pydantic.PositiveInt

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class DispatcherModel(pydantic.BaseModel):
    """A model for validating the dispatcher service section of the config file."""
    log_level: Annotated[int, pydantic.BeforeValidator(v.is_valid_log_level)]
    service_host: Annotated[str, pydantic.AfterValidator(v.is_valid_ipv4)]
    service_port: Annotated[int, pydantic.AfterValidator(v.is_valid_port)]
    receive_timeout_sec: pydantic.PositiveFloat
    max_msg_size_bytes: pydantic.PositiveInt
    ip_cache_size_bytes: pydantic.PositiveInt
    ip_period_sec: pydantic.PositiveInt
    max_auth_attempts: pydantic.PositiveInt
    db_pool_size: pydantic.PositiveInt
    db_host: Annotated[str, pydantic.AfterValidator(v.is_valid_ipv4)]
    db_port: Annotated[int, pydantic.AfterValidator(v.is_valid_port)]
    db_connect_timeout_sec: pydantic.PositiveInt
    db_name: str
    db_user: str
    db_password: str
    start_day: Annotated[int, pydantic.AfterValidator(v.is_valid_day_of_week)]
    end_day: Annotated[int, pydantic.AfterValidator(v.is_valid_day_of_week)]
    start_time_sec: Annotated[int, pydantic.AfterValidator(v.is_valid_day_sec)]
    end_time_sec: Annotated[int, pydantic.AfterValidator(v.is_valid_day_sec)]
    max_slot_size_sec: pydantic.PositiveInt
    gap_size_sec: pydantic.PositiveInt
    long_measurement_gap_thresh_sec: pydantic.PositiveInt
    scheduling_deadline: Annotated[Cron, pydantic.BeforeValidator(v.is_valid_cron)]
    stale_check_in_thresh_sec: pydantic.PositiveInt
    storage_warning_thresh: pydantic.PositiveFloat
    gmail_address: Annotated[str, pydantic.AfterValidator(v.is_valid_gmail)]
    gmail_api_credentials: pydantic.Json[dict]
    digest_time: Annotated[Cron, pydantic.BeforeValidator(v.is_valid_cron)]

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    @pydantic.model_validator(mode='after')
    def check_scheduler_times(self) -> Self:
        if self.start_time_sec > self.end_time_sec:
            raise ValueError('scheduler end time is greater than start time.')

        return self


class APIModel(pydantic.BaseModel):
    """A model for validating the API section of the config file."""
    log_level: Annotated[int, pydantic.BeforeValidator(v.is_valid_log_level)]
    service_host: Annotated[str, pydantic.AfterValidator(v.is_valid_ipv4)]
    service_port: Annotated[int, pydantic.AfterValidator(v.is_valid_port)]
    db_pool_size: pydantic.PositiveInt
    db_host: Annotated[str, pydantic.AfterValidator(v.is_valid_ipv4)]
    db_port: Annotated[int, pydantic.AfterValidator(v.is_valid_port)]
    db_connect_timeout_sec: pydantic.PositiveInt
    db_name: str
    db_user: str
    db_password: str
