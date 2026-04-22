"""A module containing the top level main function of the tgfserver application and its support functions."""
import argparse
import os
import psutil
import socket
import sys
from typing import List

# Adds parent directory to sys.path. Necessary to make the imports below work when running this file as a script
if __name__ == '__main__':
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import tgfserver.config.parameters as params
from tgfserver.utilities.command_funcs import report_check_ins, validate_config
from tgfserver.helpers.helper_funcs import expand_path
from tgfserver.helpers.string_tcp import StringTCP
from tgfserver.services.manager_service import ManagerService
from tgfserver.utilities.user_setup import user_setup
from tgfserver.startup.startup_funcs import default_startup


def running_pid() -> int:
    """A function that returns the pid of the tgfserver application, or -1 if it isn't currently running."""
    try:
        with open(f'{expand_path(params.runtime_path, make_dir=False)}/{params.pid_file}', 'r') as file:
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

        else:
            print(f'Unable to process '
                  f'{commands[0] + ' command' if len(commands) == 1 else (
                          ", ".join(commands[:-1]) + ' and ' + commands[-1] + ' commands')}: '
                  f'application not running.')

    except KeyboardInterrupt:
        pass


def main() -> None:
    """A function that serves as the top level call for the tgfserver application. Also handles command execution."""
    has_args = False
    commands = []
    parser = argparse.ArgumentParser(prog='tgfserver',
                                     description='A backend application for the operations of the UCSC TGF group')
    parser.add_argument('--setup', help='run the tgfserver user setup', action='store_true')
    parser.add_argument('--test_config', help='test that the user config file contains valid entries',
                        action='store_true')
    parser.add_argument('--status', help='display the current status of the tgfserver application', action='store_true')
    parser.add_argument('--instruments', help='report the most recent check in info for each instrument',
                        action='store_true')
    parser.add_argument('--update_db', help='update the backend database from the TGF Group Info spreadsheet',
                        action='store_true')
    parser.add_argument('--reload', help='reload the current tgfserver application instance', action='store_true')
    args = parser.parse_args()
    if args.setup:
        has_args = True
        user_setup()

    if args.test_config:
        has_args = True
        validate_config()

    if args.status:
        has_args = True
        commands.append('--status')

    if args.instruments:
        has_args = True
        report_check_ins()

    if args.update_db:
        has_args = True
        commands.append('--update_db')

    if args.reload:
        has_args = True
        commands.append('--reload')

    if len(commands) > 0:
        send_commands(commands)

    if not has_args and running_pid() == -1:
        default_startup(ManagerService)


if __name__ == '__main__':
    main()
