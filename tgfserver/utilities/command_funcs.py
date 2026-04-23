"""A module containing functions for commands that are independent of a running tgfserver application instance."""
import configparser
import datetime
import psycopg
import pydantic
from typing import Type

import tgfserver.config.parameters as params
import tgfserver.validation.config_validation as v
from tgfserver.startup.config_funcs import read_config


def report_check_ins() -> None:
    """A function that reports the most recent instrument dispatcher check ins for each instrument."""
    config = v.ManagerModel(**dict(read_config().items(params.MANAGER_NAME)))
    try:
        with psycopg.connect(f'host={config.db_host} '
                             f'port={config.db_port} '
                             f'connect_timeout={config.db_connect_timeout_sec} '
                             f'dbname={config.db_name} '
                             f'user={config.db_user} '
                             f'password={config.db_password}',
                             autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""SET TIMEZONE TO UTC;""")
                cur.execute("""SELECT * FROM tgfserver.get_check_ins();""")
                check_ins = cur.fetchall()

    except pydantic.ValidationError:
        print(f'Error: failed to access database, config file failed validation. Use the --test_config flag for '
              f'details.')
    except psycopg.Error as ex:
        print(f'Error: encountered a database exception: {ex}.')
        return

    print('Most recent status:')
    for instrument, timestamp, storage_frac, gps in check_ins:
        days_since = (datetime.datetime.now(datetime.UTC) - timestamp).total_seconds() / 86400
        print(f'{instrument} - last check in at {timestamp.isoformat()} '
              f'({"<=1 day" if days_since <= 1 else format(days_since, ".1f") + " days"} ago):')
        print(f'\tStorage usage: {format(storage_frac * 100, ".2f")}%; GPS status: {gps}')


def validate_service_config(config: configparser.ConfigParser, service_name: str,
                            config_model: Type[pydantic.BaseModel]) -> None:
    """A helper function that validates the config file section for the specified service."""
    try:
        config_model(**dict(config.items(service_name)))
        print(f"Options for service '{service_name}' validated successfully.")
    except pydantic.ValidationError as ex:
        print(f"Encountered error(s) when validating config file for service '{service_name}':")
        missing_fields = 0
        for error in ex.errors():
            if error['type'] == '':
                missing_fields += 1
            else:
                print(f"Invalid input '{error['input']}'. {error['msg']}")

        if missing_fields > 0:
            print(f'{missing_fields} missing fields.')

        return
    except configparser.NoSectionError:
        print(f"Encountered error when parsing config file: no section for service '{service_name}'.")
        return


def validate_config() -> None:
    """A function that validates the user config file."""
    print('Validating config file:')
    config = read_config()
    validate_service_config(config, params.MANAGER_NAME, v.ManagerModel)
    validate_service_config(config, params.DISPATCHER_NAME, v.DispatcherModel)
