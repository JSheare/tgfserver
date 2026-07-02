"""A module containing functions for starting up tgfserver services."""
import asyncio
import configparser
import pydantic
import warnings
from typing import Any, Type

from tgfserver.services.api_service import APIService
from tgfserver.services.service_base import ServiceBase


def default_startup(service: Type[ServiceBase] | Type[APIService], *args: Any, **kwargs: Any) -> None:
    """A function that performs the default service startup.

    Parameters
    ----------
    service: ServiceBase child class types
        The class of the service to start.

    """

    warnings.filterwarnings('always')
    try:
        service_instance = service(*args, **kwargs)
    except pydantic.ValidationError:
        print(f"Encountered error(s) when validating config file for service '{service.service_name}'. Use the "
              f"--test_config flag for details.")
        return
    except configparser.NoSectionError:
        print(f"Encountered error when parsing config file: no section for service '{service.service_name}'.")
        return
    except Exception as ex:
        print(f"Fatal exception encountered during startup of service '{service.service_name}'.")
        print(f'{type(ex).__name__}: {ex}')
        return

    if 'resource_timeout' in kwargs:
        asyncio.run(service_instance.main(kwargs['resource_timeout']))
    else:
        asyncio.run(service_instance.main())
