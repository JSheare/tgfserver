"""A module containing the top level main function of the tgfserver application and its support functions."""
import argon2
import argparse
import configparser
import datetime
import os
import pathlib
import psutil
import psycopg
import pydantic
import re
import socket
import sys
from contextlib import contextmanager
from google_auth_oauthlib.flow import InstalledAppFlow
from typing import Any, Generator, List, Type

# Adds parent directory to sys.path. Necessary to make the imports below work when running this file as a script
if __name__ == '__main__':
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import tgfserver.config.parameters as params
import tgfserver.validation.config_validation as v
from tgfserver.helpers.helper_funcs import expand_path
from tgfserver.helpers.string_tcp import StringTCP
from tgfserver.services.manager_service import ManagerService
from tgfserver.startup.startup_funcs import default_startup
from tgfserver.startup.config_funcs import read_config, write_config


def update_instrument_password(config: configparser.ConfigParser) -> None:
    """A function that updates the stored password for an instrument."""
    instrument = input('Enter the name of the instrument: ').upper()
    try:
        with psycopg.connect(f'host={config[params.MANAGER_NAME]["db_host"]} '
                             f'port={config[params.MANAGER_NAME]["db_port"]} '
                             f'connect_timeout={config[params.MANAGER_NAME]["db_connect_timeout_sec"]} '
                             f'dbname={config[params.MANAGER_NAME]["db_name"]} '
                             f'user={config[params.MANAGER_NAME]["db_user"]} '
                             f'password={config[params.MANAGER_NAME]["db_password"]}',
                             autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT tgfserver.get_instrument_id(%s)""", (instrument,))
                result = cur.fetchone()[0]
                if result is None:
                    print(f'Error: instrument {instrument} does not exist.')
                    return

                ph = argon2.PasswordHasher()
                password_hash = ph.hash(input(f'Enter the new password for {instrument}: '))
                cur.execute("""
                CALL tgfserver.update_instrument_password(%s, %s);
                """, (instrument, password_hash))
                print(f'Successfully updated password for instrument {instrument}.')

    except configparser.NoOptionError:
        print('Error: config file is missing necessary options. Use the --test_config flag for details.')
    except psycopg.Error as ex:
        print(f'Error: encountered a database exception: {ex}.')


def get_gmail_token(config: configparser.ConfigParser) -> None:
    """A function that takes the user through the authorization process required to get a Gmail API token and then
    stores it in the config file."""
    print('Before continuing, please obtain a Google API client secret file.')
    print('These can be obtained from a Google Cloud Console project (https://console.cloud.google.com/).')
    print('To obtain a secret that will work with the application, the following must be true of the project:')
    print('1 - the account to be used must be registered as a test user.')
    print('2 - the Gmail API must be enabled.')
    print('3 - the project must have a "Desktop Application" client set up.')
    print('Once these are true, download the client secret as a file.')
    print('Then, rename the file to "client_secret.json" and place it in the same directory as the config file.')
    gmail_address = input('When finished, enter the gmail address to be used: ')
    # Checking to see if input is actually a valid gmail address
    if not re.match(r'^[a-z0-9](\.?[a-z0-9]){5,}@g(oogle)?mail\.com$', gmail_address):
        print('Error: not a valid Gmail address.')
        return

    # Checking for the client secret file
    client_secret_file = f'{expand_path(params.USER_CONFIG_PATH)}/client_secret.json'
    if not pathlib.Path(client_secret_file).exists():
        print('Error: no client secret file found.')
        return

    # Getting the API credentials
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, params.GMAIL_SCOPES)
    creds = flow.run_local_server(port=0)

    # Updating the config file
    config.set(params.DISPATCHER_NAME, 'gmail_address', gmail_address)
    config.set(params.DISPATCHER_NAME, 'gmail_api_credentials', creds.to_json())
    write_config(config)

    print('Successfully wrote Gmail address and API credentials to the config file.')


def user_setup() -> None:
    """A function that implements the user setup command."""
    try:
        # Ensuring that the config file exists and is up to date and getting config info
        config = read_config()

        operations = [(update_instrument_password, 'update instrument password'),
                      (get_gmail_token, 'get Gmail API token')]
        while True:
            print("Please choose one of the following setup options or enter 'q' to quit:")
            for i in range(len(operations)):
                print(f'{i + 1} - {operations[i][1]}')

            option = input('Option: ')
            if option.isdigit() and 1 <= int(option) <= len(operations):
                operations[int(option) - 1][0](config)
                print('')
            elif option == 'q':
                return
            else:
                print('Invalid option.\n')

    except KeyboardInterrupt:
        pass


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
        print(f'Error: failed to access database, config file failed validation. Use the --test-config flag for '
              f'details.')
        return
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
    except configparser.NoSectionError:
        print(f"Encountered error when parsing config file: no section for service '{service_name}'.")
    except pydantic.ValidationError as ex:
        print(f"Encountered error(s) when validating config file for service '{service_name}':")
        for error in ex.errors():
            loc = '.'.join([s for s in error['loc'] if isinstance(s, str)])
            if error['type'] == 'missing':
                print(f"Missing required field: {loc}.")
            else:
                print(f"Invalid input '{error['input']}' at {loc}. {error['msg']}")


def validate_config() -> None:
    """A function that validates the user config file."""
    print('Validating config file:')
    config = read_config()
    validate_service_config(config, params.MANAGER_NAME, v.ManagerModel)
    validate_service_config(config, params.DISPATCHER_NAME, v.DispatcherModel)
    validate_service_config(config, params.API_NAME, v.APIModel)


def running_pid() -> int:
    """A function that returns the pid of the tgfserver application, or -1 if it isn't currently running."""
    try:
        with open(f'{expand_path(params.RUNTIME_PATH, make_dir=False)}/{params.PID_FILE}', 'r') as file:
            # Checking to see if the pid in the file is still active
            pid = int(file.readline())
            if psutil.pid_exists(pid):
                return pid

        return -1
    except (OSError, FileNotFoundError, ValueError):
        return -1


def send_commands(commands: List[str]) -> None:
    """A function that sends commands to the tgfserver application (if possible) and prints its replies."""
    try:
        pid = running_pid()
        if pid != -1:
            # Getting the port number of the application's localhost listener
            port = None
            for connection in psutil.net_connections():
                if connection.pid == pid:
                    port = connection.laddr.port

            if port is None:
                print(f'Unable to process '
                      f'{commands[0] + ' command' if len(commands) == 1 else (
                              ", ".join(commands[:-1]) + ' and ' + commands[-1] + ' commands')}: '
                      f'failed to contact application.')
                return

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Waiting for a max of ten seconds to connect to the application
                sock.settimeout(10)
                try:
                    sock.connect(('127.0.0.1', port))
                except TimeoutError:
                    print(f'Unable to process '
                          f'{commands[0] + ' command' if len(commands) == 1 else (
                                  ", ".join(commands[:-1]) + ' and ' + commands[-1] + ' commands')}: '
                          f'application unresponsive.')
                    return
                except ConnectionRefusedError:
                    print(f'Unable to process '
                          f'{commands[0] + ' command' if len(commands) == 1 else (
                                  ", ".join(commands[:-1]) + ' and ' + commands[-1] + ' commands')}: '
                          f'connection refused. Please try again later.')

                sock.settimeout(None)

                protocol = StringTCP

                # Sending the commands to the application
                commands_sent = 0
                for command in commands:
                    try:
                        protocol.send_message(sock, command)
                        commands_sent += 1
                    except Exception:
                        print(f'Encountered an exception while sending {command} command.')

                # Printing responses from the application and stopping after all sent commands have been processed
                if commands_sent > 0:
                    command_count = 0
                    try:
                        for message in protocol.get_messages(sock):
                            if message == 'SENTINEL':
                                # Sentinel value indicates that a command is done processing
                                command_count += 1
                                # Stopping once all commands have been processed
                                if command_count == commands_sent:
                                    break

                            else:
                                print(message)

                    except Exception:
                        print(f'Encountered an exception while getting responses from application.')

    except KeyboardInterrupt:
        pass


@contextmanager
def pid_manager() -> Generator[None, Any, None]:
    """A generator responsible for creating and deleting the application's pid file."""
    # Making the file
    directory = expand_path(params.RUNTIME_PATH)
    with open(f'{directory}/{params.PID_FILE}', 'w') as file:
        file.write(str(os.getpid()))

    yield

    # Deleting the file
    try:
        os.remove(f'{directory}/{params.PID_FILE}')
    except Exception:
        pass


def main() -> None:
    """A function that serves as the top level call for the tgfserver application. Also handles command execution."""
    parser = argparse.ArgumentParser(prog='tgfserver',
                                     description='A linux backend application for the operations of the UCSC TGF group')
    parser.add_argument('--setup', help='run the tgfserver user setup', action='store_true')
    parser.add_argument('--test-config', help='test that the config file contains valid options',
                        action='store_true')
    parser.add_argument('--status', help='display the current status of the tgfserver application', action='store_true')
    parser.add_argument('--instruments', help='report the most recent check in info for each instrument',
                        action='store_true')
    parser.add_argument('--update-db', help='update the backend database from the TGF Group Info spreadsheet',
                        action='store_true')
    parser.add_argument('--reload', help='reload the current tgfserver application instance', action='store_true')

    args = parser.parse_args()

    # Handling any arguments
    has_args = False
    commands = []
    if args.setup:
        user_setup()
        has_args = True

    if args.test_config:
        validate_config()
        has_args = True

    if args.status:
        commands.append('--status')
        has_args = True

    if args.instruments:
        report_check_ins()
        has_args = True

    if args.update_db:
        commands.append('--update-db')
        has_args = True

    if args.reload:
        commands.append('--reload')
        has_args = True

    if len(commands) > 0:
        if running_pid() != -1:
            send_commands(commands)
        else:
            print(f'Unable to process '
                  f'{commands[0] + ' command' if len(commands) == 1 else (
                          ", ".join(commands[:-1]) + ' and ' + commands[-1] + ' commands')}: '
                  f'application not running.')

    if not has_args and running_pid() == -1:
        # Running the application
        with pid_manager():
            default_startup(ManagerService)


if __name__ == '__main__':
    main()
