"""A module containing functions that facilitate user setup of the tgfserver application."""
import argon2
import configparser
import pathlib
import psycopg
import re
from google_auth_oauthlib.flow import InstalledAppFlow

import tgfserver.config.parameters as params
from tgfserver.helpers.helper_funcs import expand_path
from tgfserver.startup.config_funcs import read_config, write_config


def update_instrument_password(config: configparser.ConfigParser) -> None:
    """A function that updates the stored password for an instrument."""
    instrument = input('Enter the name of the instrument: ').upper()
    try:
        with psycopg.connect(f'host={config[params.manager_name]["db_host"]} '
                             f'port={config[params.manager_name]["db_port"]} '
                             f'connect_timeout={config[params.manager_name]["db_connect_timeout_sec"]} '
                             f'dbname={config[params.manager_name]["db_name"]} '
                             f'user={config[params.manager_name]["db_user"]} '
                             f'password={config[params.manager_name]["db_password"]}',
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
    client_secret_file = f'{expand_path(params.user_config_path)}/client_secret.json'
    if not pathlib.Path(client_secret_file).exists():
        print('Error: no client secret file found.')
        return

    # Getting the API credentials
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, params.gmail_scopes)
    creds = flow.run_local_server(port=0)

    # Updating the config file
    config.set(params.dispatcher_name, 'gmail_address', gmail_address)
    config.set(params.dispatcher_name, 'gmail_api_credentials', creds.to_json())
    write_config(config)

    print('Successfully wrote Gmail address and API credentials to the config file.')


def user_setup() -> None:
    """A function that implements the user setup command."""
    try:
        # Ensuring that the config file exists and is up to date and getting config info
        config = read_config()

        operations = [(update_instrument_password, 'update instrument password'),
                      (get_gmail_token, 'Get Gmail API token')]
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
