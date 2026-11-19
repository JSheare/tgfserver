"""A module containing the validator functions used by the tgfserver application's pydantic validation models."""
import datetime
import logging
import re
from cron_converter import Cron
from typing import Any

# Before validators:
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


def is_valid_clock(value: Any) -> int:
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


# After validators:
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


def is_valid_port(value: int) -> int:
    if not 0 <= value <= 65535:
        raise ValueError('input should be a valid TCP port.')

    return value


def is_valid_gmail(value: str) -> str:
    if not re.match(r'^[a-z0-9](\.?[a-z0-9]){5,}@g(oogle)?mail\.com$', value):
        raise ValueError('input must be a gmail address.')

    return value


def is_valid_directory_path(value: str) -> str:
    value.replace('\\', '/')
    if value != '' and value[-1] == '/':
        value = value[:-1]

    if value != '' and not re.match(r'^((/[a-zA-Z0-9_-]+)+|/)$', value):
        raise ValueError('input should be a valid absolute directory path.')

    return value


def is_valid_subdirectory_path(value: str) -> str:
    value.replace('\\', '/')
    if value != '' and value[-1] == '/':
        value = value[:-1]

    if value != '' and not re.match(r'^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$', value):
        raise ValueError('input should be a valid subdirectory path.')

    return value


def is_valid_instrument_name(value: str) -> str:
    if not value.isalnum():
        raise ValueError('input should consist of only letters and numbers.')

    value = value.upper()
    return value
