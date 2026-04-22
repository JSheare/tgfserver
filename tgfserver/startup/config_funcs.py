"""A module containing functions for working with the application's config file."""
import configparser
import pathlib

import tgfserver.config.parameters as params
from tgfserver.helpers.helper_funcs import expand_path


def get_config_template() -> configparser.ConfigParser:
    """A helper function that returns the application's config file template as a ConfigParser instance."""
    template_file = f'{pathlib.Path(__file__).parent.parent}/config/{params.config_file}'
    template_config = configparser.ConfigParser()
    template_config.read(template_file)
    return template_config


def read_config(update_user_config: bool = True) -> configparser.ConfigParser:
    """A function that creates and reads the application's config file and returns it as a ConfigParser instance.

    Parameters
    ----------
    update_user_config : bool, optional
        An optional flag that updates the user config file from the template. True by default.

    Returns
    -------
    configparser.ConfigParser
        A ConfigParser instance containing the contents of the config file.

    """

    # Reading the user config file, or making one if it doesn't exist
    user_file = f'{expand_path(params.user_config_path)}/{params.config_file}'
    if not pathlib.Path(user_file).is_file():
        template_config = get_config_template()
        with open(user_file, 'w') as user_file:
            template_config.write(user_file)

        return template_config

    else:
        user_config = configparser.ConfigParser()
        user_config.read(user_file)
        if update_user_config:
            template_config = get_config_template()
            needs_update = False
            # Checking to see if the user config is missing necessary sections or options
            for section in template_config.sections():
                if user_config.has_section(section):
                    for option in template_config.options(section):
                        if not user_config.has_option(section, option):
                            needs_update = True
                            break

                    if needs_update:
                        break

                else:
                    needs_update = True
                    break

            # Checking to see if the user config has old sections or options
            if not needs_update:  # No sense in doing this check if the file already needs an update
                for section in user_config.sections():
                    if template_config.has_section(section):
                        for option in user_config.options(section):
                            if not template_config.has_option(section, option):
                                needs_update = True
                                break

                        if needs_update:
                            break

                    else:
                        needs_update = True
                        break

            if needs_update:
                # Copying out user-modified data before writing
                for section in user_config.sections():
                    if template_config.has_section(section):
                        for option in user_config.options(section):
                            if template_config.has_option(section, option):
                                template_config.set(section, option, user_config.get(section, option))

                with open(user_file, 'w') as user_file:
                    template_config.write(user_file)

                return template_config

        return user_config


def write_config(config: configparser.ConfigParser) -> None:
    """A function that writes the contents of the given ConfigParser instance to the application's config file.

    Parameters
    ----------
    config : configparser.ConfigParser
        The config parser to be written to the config file.

    """

    user_file = f'{expand_path(params.user_config_path)}/{params.config_file}'
    with open(user_file, 'w') as user_file:
        config.write(user_file)
