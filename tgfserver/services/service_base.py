"""A module containing an abstract base class for tgfserver services."""
import asyncio
import datetime
import inspect
import logging
import pydantic
import signal
from abc import ABC
from abc import abstractmethod
from cron_converter import Cron
from importlib.metadata import version, PackageNotFoundError
from typing import Any, Callable, Set

import tgfserver.config.parameters as params
from tgfserver.startup.config_funcs import read_config
from tgfserver.startup.configure_logging import configure_logging


class ServiceBase(ABC):
    """An abstract base class for tgfserver services. Implements methods for registering and handling resource lifetime,
    primary task execution, and periodic task execution.

    Attributes
    ----------
    _config : pydantic.BaseModel subclasses
        A pydantic model containing the service's config options.
    _logger : logging.Logger
        The service's logger.
    __registered_resource_managers : List[coroutine function]
        A list of registered resource coroutine functions.
    __registered_primary_tasks : List[Tuple[coroutine function, list[Any], Dict[str, Any]]]
        A list of registered primary task coroutine functions and any args and keyword args that should be passed
        to them.
    __registered_interval_tasks : List[Tuple[coroutine function, float, bool]]
        A list of registered interval task coroutine functions, the intervals to execute them at, and whether execution
        should be started immediately on service startup.
    __registered_cron_tasks : List[Tuple[coroutine function, cron_converter.Cron]]
        A list of registered cron task coroutine functions and the Cron object for determining their execution
        schedules.
    __shutdown : asyncio.Event
        An event used to signal that the service should shut down.

    """

    @property
    @abstractmethod
    def service_name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def _config_model(self):
        raise NotImplementedError

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Getting and validating the values in the service's config file section

        self._config = self._config_model(
            **dict(read_config(update_user_config=kwargs['update_user_config']
            if 'update_user_config' in kwargs
            else True).items(self.service_name)))

        self._logger = logging.getLogger(self.service_name)
        self._logger.setLevel(logging.getLevelName(self._config.log_level))

        self.__registered_resource_managers = []
        self.__registered_primary_tasks = []
        self.__registered_interval_tasks = []
        self.__registered_cron_tasks = []

        self.__shutdown = asyncio.Event()

    def _configure_module_loggers(self) -> None:
        """A helper function that sets the log level of the various modules used by the service. It is recommended
        that this method be overridden in child classes to include a call to the parent method via super() and calls
        to setLevel() for each of the module loggers used by the child class."""
        if self._config.log_level == logging.DEBUG:
            asyncio.get_event_loop().set_debug(True)

        logging.getLogger('asyncio').setLevel(self._config.log_level)

    async def _reload_callback(self, diff: Set[str]) -> None:
        """A function that is called after a service reload. Used to implement custom post-reload behavior via
        overriding in subclasses."""
        pass

    async def reload(self) -> None:
        """A function that initiates a service reload when called."""
        self._logger.info('Reloading service.')
        # Reading the config file in another thread
        parser = await asyncio.to_thread(read_config, update_user_config=False)
        try:
            new_config = self._config_model(**dict(parser.items(self.service_name)))
        except pydantic.ValidationError:
            self._logger.error('Failed to reload service. Config validation failed.')
            return

        # Noting where the config differences are
        diff = set()
        for field, value in new_config:
            if hasattr(self._config, field):
                if getattr(self._config, field) != value:
                    diff.add(field)

            else:
                self._logger.error('Failed to reload service. Updated config contains unknown options.')
                return

        self._config = new_config

        # Changing the log level if necessary
        if 'log_level' in diff:
            # Changing the log level on the root logger
            logging.getLogger().setLevel(self._config.log_level)

            # Changing the log level on the service logger
            self._logger.setLevel(self._config.log_level)

            # Changing the log level on the module level loggers
            self._configure_module_loggers()

            self._logger.info(f'Changed service log level to {logging.getLevelName(self._config.log_level)}.')

        # Running the reload callback
        await self._reload_callback(diff)

    def shutdown(self) -> None:
        """A function that initiates service shutdown when called."""
        self.__shutdown.set()

    async def __reload_handler(self, os_signal: signal.Signals) -> None:
        """A signal handler that initiates service reload when the passed OS signal is received."""
        self._logger.debug(f'OS signal received: {os_signal.name}')
        await self.reload()

    async def __shutdown_handler(self, os_signal: signal.Signals) -> None:
        """A signal handler that initiates service shutdown when the passed OS signal is received."""
        self._logger.debug(f'OS signal received: {os_signal.name}.')
        self.shutdown()

    def __add_signal_handlers(self) -> None:
        """A helper function that sets up OS signal handlers for the service."""
        loop = asyncio.get_event_loop()
        for os_signal in (signal.SIGHUP,):
            self._logger.debug(f'Adding reload handler for OS signal {os_signal.name}.')
            # Lambda with default value to get around late binding
            loop.add_signal_handler(
                os_signal, lambda s=os_signal: asyncio.create_task(self.__reload_handler(os_signal)), ())

        for os_signal in (signal.SIGTERM, signal.SIGINT):
            self._logger.debug(f'Adding shutdown handler for OS signal {os_signal.name}.')
            # Lambda with default value to get around late binding
            loop.add_signal_handler(
                os_signal, lambda s=os_signal: asyncio.create_task(self.__shutdown_handler(os_signal)), ())

    @staticmethod
    async def _wait_forever():
        """A convenience function that waits forever when called."""
        await asyncio.get_event_loop().create_future()

    def _register_resource_manager(self, coro_func: Callable) -> None:
        """A function that registers the passed coroutine function as a resource manager.

        Resources that are used by the whole service must be acquired at service startup and released at service
        shutdown. This function provides an interface for doing that. A resource manager should be a coroutine function
        of the following form:

        # Resource acquisition code

        # ...

        yield

        # Resource release code

        # ...

        Parameters
        ----------
        coro_func : coroutine function
            The coroutine function to be registered as a resource manager. Should create an async generator when called.

        Raises
        ------
        ValueError
            If the passed function is not a coroutine function that creates an async generator when called.

        """

        if not inspect.isasyncgenfunction(coro_func):
            raise ValueError('manager must be a coroutine function.')

        self.__registered_resource_managers.append(coro_func)

    def _register_primary_task(self, coro_func: Callable, *args: Any, **kwargs: Any) -> None:
        """A function that registers the passed coroutine function as a primary task. The function will be executed with
        any passed args and keyword args.

        Primary tasks are those that are critical to the operations of the service. They run indefinitely and can create
        secondary tasks (jobs).

        Parameters
        ----------
        coro_func : coroutine function
            The coroutine function to be registered as a primary task.

        Raises
        ------
        ValueError
            If the passed function is not a coroutine function.

        """

        if not inspect.iscoroutinefunction(coro_func):
            raise ValueError('task must be a coroutine function.')

        self.__registered_primary_tasks.append((coro_func, args, kwargs))

    def _register_periodic_task(self, coro_func: Callable, cron: Cron | None = None, interval: float | None = None,
                                start_immediately: bool = False):
        """A function that registers the passed coroutine function as a periodic task (either an interval task or cron
        task depending on the arguments).

        Periodic tasks are secondary tasks that must be executed at specific times. There are two subtypes: interval
        tasks, which run every x seconds, and cron tasks, which run according to a provided Cron instance.

        Parameters
        ----------
        coro_func : coroutine function
            The coroutine function to be registered as a periodic task.
        cron : cron_converter.Cron | None, optional
            A Cron object that will be used to determine the task's execution schedule.
        interval: float | None, optional
            The interval in seconds that the task will be executed at.
        start_immediately: bool, optional
            Only pertains to interval tasks. Whether the interval task should be executed immediately on service
            startup. False by default.

        Raises
        ------
        ValueError
            If the passed function is not a coroutine function, or if neither cron nor interval are specified.

        """

        if not inspect.iscoroutinefunction(coro_func):
            raise ValueError('task must be a coroutine function.')

        if cron is None and interval is None:
            raise ValueError('either a cron or an interval must be supplied.')

        if interval is not None:
            self.__registered_interval_tasks.append((coro_func, interval, start_immediately))

        if cron is not None:
            self.__registered_cron_tasks.append((coro_func, cron))

    def __primary_task_callback(self, task: asyncio.Task) -> None:
        """A callback that is executed when a primary task concludes."""
        try:
            task.result()
        except asyncio.exceptions.CancelledError:
            self._logger.debug(f"Primary task '{task.get_coro().__name__}' shut down successfully.")
        except Exception:
            self._logger.critical(f"Primary task '{task.get_coro().__name__}' encountered an exception:", exc_info=True)
            # Since these tasks are critical, we'll just shut the service down if they fail
            self.shutdown()

    def __interval_executor_callback(self, task: asyncio.Task) -> None:
        """A callback that is executed when an interval task executor concludes."""
        try:
            task.result()
        except asyncio.exceptions.CancelledError:
            self._logger.debug(f"Interval task executor '{task.get_name()}' shut down successfully.")
        except Exception:
            self._logger.exception(f"Interval task executor '{task.get_name()}' encountered an exception:")

    def __cron_executor_callback(self, task: asyncio.Task) -> None:
        """A callback that is executed when a cron task executor concludes."""
        try:
            task.result()
        except asyncio.exceptions.CancelledError:
            self._logger.debug(f"Cron task executor '{task.get_name()}' shut down successfully.")
        except Exception:
            self._logger.exception(f"Cron task executor '{task.get_name()}' encountered an exception:")

    async def __execute_interval_task(self, coro_func: Callable, interval: float, start_immediately) -> None:
        """A function that executes interval tasks on the specified interval."""
        if not start_immediately:
            await asyncio.sleep(interval)

        while True:
            try:
                self._logger.debug(f"Executing interval task '{coro_func.__name__}'.")
                # If coro_func hasn't finished before period, it will be executed again immediately when the previous
                # instance is finished
                await asyncio.gather(coro_func(), asyncio.sleep(interval))
            except asyncio.exceptions.CancelledError:
                if asyncio.current_task().cancelling() == 0:
                    self._logger.debug(f"Periodic interval task '{coro_func.__name__}' was cancelled.")
                else:
                    raise

            except Exception:
                self._logger.exception(f"Periodic interval task '{coro_func.__name__}' encountered an exception:")

    async def __execute_cron_task(self, coro_func: Callable, cron: Cron) -> None:
        """A function that executes cron tasks according to a schedule derived from the passed Cron instance."""
        timezone = datetime.datetime.now().astimezone()
        schedule = cron.schedule(timezone_str=timezone.tzname())
        # Waiting until the first scheduled time
        await asyncio.sleep((schedule.next() - datetime.datetime.now(timezone.tzinfo)).total_seconds())
        for when in schedule:
            try:
                self._logger.debug(f"Executing cron task '{coro_func.__name__}'.")
                # If coro_func hasn't finished before the next scheduled time, it will be executed again immediately
                # when the previous instance is finished
                await asyncio.gather(coro_func(),
                                     asyncio.sleep((when - datetime.datetime.now(timezone.tzinfo)).total_seconds()))
            except asyncio.exceptions.CancelledError:
                if asyncio.current_task().cancelling() == 0:
                    self._logger.debug(f"Periodic cron task '{coro_func.__name__}' was cancelled.")
                else:
                    raise

            except Exception:
                self._logger.exception(f"Periodic cron task '{coro_func.__name__}' encountered an exception:")

    async def main(self, resource_timeout: float = 60.0) -> None:
        """A function that runs the service.

        Parameters
        ----------
        resource_timeout : float, optional
            The amount of time to wait for each resource acquisition before timing out. 60 seconds by default.

        """

        self._configure_module_loggers()
        log_listener = configure_logging(self.service_name, self._config.log_level)
        log_listener.start()

        resource_managers = set()
        resource_tasks = set()
        primary_tasks = set()
        interval_executors = set()
        cron_executors = set()
        try:
            app_version = None
            try:
                app_version = version(params.APPLICATION_NAME)
            except PackageNotFoundError:
                pass

            if app_version is not None:
                self._logger.info(f'({params.APPLICATION_NAME} v{version(params.APPLICATION_NAME)}) '
                                  f'Starting up service.')
            else:
                self._logger.info('Starting up service.')

            self.__add_signal_handlers()

            # Acquiring resources
            for coro_func in self.__registered_resource_managers:
                self._logger.debug(f"Acquiring the resource associated with '{coro_func.__name__}'.")
                try:
                    async with asyncio.timeout(resource_timeout):
                        resource_manager = coro_func()
                        await anext(resource_manager)

                except StopAsyncIteration:
                    pass
                except TimeoutError:
                    self._logger.critical(f"Timed out acquiring the resource associated with '{coro_func.__name__}'.")
                    raise RuntimeError('resource acquisition failed.')
                except Exception:
                    self._logger.critical(f"Exception encountered when acquiring the resource associated "
                                          f"with '{coro_func.__name__}':", exc_info=True)
                    raise RuntimeError('resource acquisition failed.')

                resource_managers.add(resource_manager)
                self._logger.debug(f"Resource associated with '{coro_func.__name__}' successfully acquired.")

            # Making a record of tasks created by resources
            resource_tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

            # Starting primary tasks
            for func, args, kwargs in self.__registered_primary_tasks:
                primary_task = asyncio.create_task(func(*args, **kwargs))
                self._logger.debug(f"Starting primary task '{primary_task.get_coro().__name__}'.")
                primary_task.add_done_callback(self.__primary_task_callback)
                primary_tasks.add(primary_task)

            # Scheduling interval tasks
            for coro_func, interval, start_immediately in self.__registered_interval_tasks:
                self._logger.debug(f"Starting executor for interval task '{coro_func.__name__}'.")
                executor_task = asyncio.create_task(
                    self.__execute_interval_task(coro_func, interval, start_immediately))
                executor_task.set_name(coro_func.__name__ + '_executor')
                executor_task.add_done_callback(self.__interval_executor_callback)
                interval_executors.add(executor_task)

            # Scheduling cron tasks
            for coro_func, cron in self.__registered_cron_tasks:
                self._logger.debug(f"Starting executor for cron task '{coro_func.__name__}'.")
                executor_task = asyncio.create_task(self.__execute_cron_task(coro_func, cron))
                executor_task.set_name(coro_func.__name__ + '_executor')
                executor_task.add_done_callback(self.__cron_executor_callback)
                cron_executors.add(executor_task)

            self._logger.info('Startup complete.')

            await self.__shutdown.wait()

        except RuntimeError:
            # At the moment, this can only happen if a resource acquisition fails. The logging for that is elsewhere
            pass

        except Exception:
            self._logger.exception('Exception encountered during service startup:')

        finally:
            self._logger.info('Shutting down service.')

            # Canceling cron task executors and waiting for them to end
            for task in cron_executors:
                if not task.done():
                    self._logger.debug(f"Shutting down cron task executor '{task.get_name()}'.")
                    task.cancel()

            await asyncio.gather(*cron_executors, return_exceptions=True)

            # Canceling interval task executors and waiting for them to end
            for task in interval_executors:
                if not task.done():
                    self._logger.debug(f"Shutting down interval task executor '{task.get_name()}'.")
                    task.cancel()

            await asyncio.gather(*interval_executors, return_exceptions=True)

            # Canceling primary tasks and waiting for them to end
            for task in primary_tasks:
                if not task.done():
                    self._logger.debug(f"Shutting down primary task '{task.get_coro().__name__}'.")
                    task.cancel()

            await asyncio.gather(*primary_tasks, return_exceptions=True)

            # Waiting for secondary tasks to end
            other_tasks = set([t for t in asyncio.all_tasks() if t is not asyncio.current_task()]).difference(
                resource_tasks)
            await asyncio.gather(*other_tasks, return_exceptions=True)

            # Releasing resources
            for manager in resource_managers:
                self._logger.debug(f"Releasing the resource associated with '{manager.__name__}'.")
                try:
                    await anext(manager)
                except StopAsyncIteration:
                    self._logger.debug(f"Resource associated with '{manager.__name__}' released successfully.")
                except Exception:
                    self._logger.exception(f"Exception encountered when releasing the resource associated with"
                                           f"'{manager.__name__}':")

            self._logger.info('Shutdown complete.')

            log_listener.stop()
