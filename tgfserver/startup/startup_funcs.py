"""A module containing functions for starting up tgfserver services."""
import asyncio
import configparser
import pydantic
import warnings
from typing import Any, Type

from tgfserver.services.service_base import ServiceBase


def default_startup(service: Type[ServiceBase], *args: Any, **kwargs: Any) -> None:
    """A function that performs the default service startup.

    Parameters
    ----------
    service: ServiceBase child class types
        The class of the service to start.

    """

    warnings.filterwarnings('always')
    try:
        service_instance = service(*args, **kwargs)
    except pydantic.ValidationError as ex:
        print(f"Encountered error(s) when validating config file for service '{service.service_name}':")
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
        print(f"Encountered error when parsing config file: no section for service '{service.service_name}'.")
        return
    except Exception as ex:
        print('Fatal exception encountered during startup.')
        print(f'{type(ex).__name__}: {ex}')
        return

    if 'resource_timeout' in kwargs:
        asyncio.run(service_instance.main(kwargs['resource_timeout']))
    else:
        asyncio.run(service_instance.main())
