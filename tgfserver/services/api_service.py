"""A module containing a class that implements the tgfserver application's API service."""
import asyncio
import datetime
import fastapi
import logging
import psycopg
import psycopg_pool
import pydantic
import signal
import uvicorn
from contextlib import asynccontextmanager
from fastapi import APIRouter, FastAPI, Query
from fastapi.responses import JSONResponse
from importlib.metadata import version, PackageNotFoundError
from psycopg.rows import dict_row
from typing import Any, Annotated, AsyncGenerator, Dict, List

import tgfserver.config.parameters as params
from tgfserver.startup.config_funcs import read_config
from tgfserver.startup.configure_logging import configure_logging
from tgfserver.validation.config_validation import APIModel


class APIService:
    """A class that implements the tgfserver application's API service.

        The API service distributes information relevant to the UCSC TGF group and its instruments.

        Attributes
        ----------
        service_name : str
            The name of the service.
        _config : pydantic.BaseModel subclasses
            A pydantic model containing the service's config options.
        _logger : logging.Logger
            The service's logger.
        _pool : psycopg_pool.AsyncConnectionPool
            A database connection pool for use in the service's various functions.
        _shutdown : asyncio.Event
        An event used to signal that the service should shut down.

    """

    service_name = params.API_NAME

    def __init__(self, *args: Any, **kwargs: Any):
        self._config = APIModel(
            **dict(read_config(update_user_config=kwargs['update_user_config']
            if 'update_user_config' in kwargs
            else True).items(self.service_name)))

        self._logger = logging.getLogger(self.service_name)
        self._logger.setLevel(logging.getLevelName(self._config.log_level))

        self._pool = psycopg_pool.AsyncConnectionPool(
            f'host={self._config.db_host} '
            f'port={self._config.db_port} '
            f'connect_timeout={self._config.db_connect_timeout_sec} '
            f'dbname={self._config.db_name} '
            f'user={self._config.db_user} '
            f'password={self._config.db_password}',
            kwargs={'autocommit': True},
            min_size=self._config.db_pool_size,
            max_size=self._config.db_pool_size,
            check=psycopg_pool.AsyncConnectionPool.check_connection,
            reconnect_failed=self._pool_reconnect_failed_callback,
            open=False,
            name=f'{self.service_name}_pg_pool')

        self._shutdown = asyncio.Event()

    def _configure_module_loggers(self) -> None:
        """A helper function that sets the log level of the various modules used by the service."""
        if self._config.log_level == logging.DEBUG:
            asyncio.get_event_loop().set_debug(True)

        logging.getLogger('asyncio').setLevel(self._config.log_level)
        logging.getLogger('psycopg').setLevel(self._config.log_level)
        logging.getLogger('psycopg.pool').setLevel(self._config.log_level)
        logging.getLogger('uvicorn.error').setLevel(self._config.log_level)
        logging.getLogger('uvicorn.access').setLevel(self._config.log_level)

    async def reload(self) -> None:
        """A function that initiates a service reload when called."""
        self._logger.info('Reloading service.')
        # Reading the config file in another thread
        parser = await asyncio.to_thread(read_config, update_user_config=False)
        try:
            new_config = APIModel(**dict(parser.items(self.service_name)))
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

        restart_required = {'service_host', 'service_port', 'db_pool_size', 'db_host', 'db_port',
                            'db_connect_timeout_sec', 'db_name', 'db_user', 'db_password'}
        if len(diff.intersection(restart_required)) > 0:
            self._logger.warning('Service restart required for some updated config options to take effect.')

    def shutdown(self) -> None:
        """A function that initiates service shutdown when called."""
        self._shutdown.set()

    async def _reload_handler(self, os_signal: signal.Signals) -> None:
        """A signal handler that initiates service reload when the passed OS signal is received."""
        self._logger.debug(f'OS signal received: {signal.Signals(os_signal).name}')
        await self.reload()

    async def _shutdown_handler(self, os_signal: signal.Signals) -> None:
        """A signal handler that initiates service shutdown when the passed OS signal is received."""
        self._logger.debug(f'OS signal received: {signal.Signals(os_signal).name}.')
        self.shutdown()

    def _add_signal_handlers(self) -> None:
        """A helper function that sets up OS signal handlers for the service."""
        loop = asyncio.get_event_loop()
        for os_signal in (signal.SIGHUP,):
            self._logger.debug(f'Adding reload handler for OS signal {os_signal.name}.')
            # Lambda with default value to get around late binding
            loop.add_signal_handler(
                os_signal, lambda s=os_signal: asyncio.create_task(self._reload_handler(os_signal)), ())

        for os_signal in (signal.SIGTERM, signal.SIGINT):
            self._logger.debug(f'Adding shutdown handler for OS signal {os_signal.name}.')
            # Lambda with default value to get around late binding
            loop.add_signal_handler(
                os_signal, lambda s=os_signal: asyncio.create_task(self._shutdown_handler(os_signal)), ())

    def _pool_reconnect_failed_callback(self, pool: psycopg_pool.AsyncConnectionPool) -> None:
        """A callback which is called if the database connection pool can't reacquire connections."""
        self._logger.critical('Lost connection to database.')
        self.shutdown()

    @asynccontextmanager
    async def _lifespan(self, app: fastapi.FastAPI) -> AsyncGenerator[None, None]:
        """A function that manages the setup and teardown of the service's resources."""
        self._logger.debug('Starting database connection pool.')
        await self._pool.open(wait=True)
        self._logger.debug('Successfully started database connection pool.')

        yield

        self._logger.debug('Closing database connection pool.')
        await self._pool.close()
        self._logger.debug('Successfully closed database connection pool.')

    async def _generic_exception_handler(self, request: fastapi.Request, ex: Exception) -> JSONResponse:
        """A function that handles generic exceptions raised during the api's operations."""
        self._logger.error(f"Encountered an exception in route '{request.scope.get("route").path}':",
                           exc_info=ex)
        return JSONResponse(
            status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={'detail': 'Encountered an exception when attempting to fulfill request.'})

    async def _database_exception_handler(self, request: fastapi.Request, ex: psycopg.Error) -> JSONResponse:
        """A function that handles database exceptions raised during the api's operations."""
        self._logger.error(f"Encountered a database exception in route '{request.scope.get("route").path}':",
                           exc_info=ex)
        return JSONResponse(
            status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={'detail': 'Encountered a database exception when attempting to fulfill request.'})

    async def _get_data_root(self) -> Dict[str, str]:
        """An endpoint that returns the root location for all TGF data on the main data computer.

        Returns
        -------
        A dictionary containing the data root. It is of the form {'data_root': root}.

        """

        self._logger.debug('Servicing _get_data_root request.')
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT tgfserver.get_data_root();
                    """)
                return {'data_root': (await cur.fetchone())[0]}

    async def _get_instruments(self) -> List[str]:
        """An endpoint that returns a list of all registered UCSC TGF group instruments.

        Returns
        -------
        An array of all registered instrument names.

        """

        self._logger.debug('Servicing _get_instruments request.')
        # Getting the instruments list from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                SELECT * FROM tgfserver.get_instruments();
                """)
                return [s[0] for s in await cur.fetchall()]

    async def _get_scintillators(self) -> Dict[str, Dict[str, str | int]]:
        """An endpoint that returns a record of all registered scintillator types and their associated information.

        Returns
        -------
        A dictionary containing records for all registered scintillator types. Each entry is of the following form:
        {scint_name: {'scint_priority': x, 'plot_color': x}}.

        """

        self._logger.debug('Servicing _get_scintillators request.')
        # Getting the scintillator data from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                cur.row_factory = dict_row
                await cur.execute("""
                SELECT * FROM tgfserver.get_scintillators();
                """)
                raw_result = await cur.fetchall()

        # Grouping the row data by scintillator name
        structured_result = {}
        for row in raw_result:
            structured_result[row['scint_name']] = {}
            for key in row:
                if key == 'scint_name':
                    continue

                structured_result[row['scint_name']][key] = row[key]

        return structured_result

    async def _get_instrument_subdir(self, instrument: Annotated[str, Query(max_length=20)]) -> Dict[str, str]:
        """An endpoint that returns the data subdirectory for a particular instrument (within the data root) on the UCSC
        TGF group's main data computer.

        Parameters
        ----------
        instrument: the name of the instrument to get the subdirectory for.

        Returns
        -------
        A dictionary containing the instrument's subdirectory. It is of the form {'subdir': subdir}.

        """

        self._logger.debug('Servicing _get_instrument_subdir request.')
        # Making sure that instrument is in a valid format
        if not instrument.isalnum():
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                        detail='Invalid instrument. Instrument must consist of only alphanumeric '
                                               'characters.')

        instrument = instrument.upper()

        # Getting the instrument subdirectory from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                SELECT tgfserver.get_instrument_subdir(%s);
                """, (instrument, ))
                result = await cur.fetchone()

        if len(result) == 0 or result[0] is None:
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST, detail='Unknown instrument.')

        return {'subdir': result[0]}

    async def _get_instrument_config(self, instrument: Annotated[str, Query(max_length=20)],
                                     date: Annotated[str,
                                        Query(max_length=6)]) -> Dict[str, Dict[str, Dict[str, str | bool]]]:
        """An endpoint that returns the configuration information for the given instrument.

        Parameters
        ----------
        instrument: the name of the instrument to get configuration information for.

        date: the date to get configuration information for. This should be of the form 'YYMMDD', or an asterisk to get
        all configurations.

        Returns
        -------
        A dictionary containing instrument configurations after particular dates. Each entry has the following form:
        {after_date_1: {scint_name_1: {'erc': x, 'format_name': x, 'long_event_search': x}, scint_name_2: ...}}.

        """
        
        self._logger.debug('Servicing _get_instrument_config request.')
        # Making sure that instrument is in a valid format
        if not instrument.isalnum():
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                        detail='Invalid instrument. Instrument must consist of only alphanumeric '
                                               'characters.')

        instrument = instrument.upper()

        # Making sure that date is in a valid format
        if date == '*':
            full_date = datetime.datetime.fromtimestamp(0, datetime.UTC)
        else:
            try:
                full_date = datetime.datetime.strptime(date, '%y%m%d').replace(tzinfo=datetime.UTC)
            except Exception:
                raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                            detail="Invalid date. Date must be of the form 'YYMMDD'.")

        # Getting the config(s) from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                cur.row_factory = dict_row
                await cur.execute("""SET TIMEZONE TO UTC;""")
                if date == '*':
                    await cur.execute("""
                    SELECT * FROM tgfserver.get_all_configs(%s)
                    """, (instrument, ))
                else:
                    await cur.execute("""
                    SELECT * FROM tgfserver.get_config(%s, %s);
                    """, (instrument, full_date))

                result = await cur.fetchall()

        if len(result) == 0:
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_404_NOT_FOUND,
                                        detail='No configuration(s) found.')

        # Structuring the instrument config info in a more JSON-friendly way
        # {after_date: {scintillator_name: {other stuff}}
        configs = {}
        for row in result:
            date_str = row['after_date'].strftime('%y%m%d')
            if date_str == '700101':  # Epoch origin
                date_str = '000000'

            # Date at the top level
            if date_str not in configs:
                configs[date_str] = {}

            # Scintillator name at the next level
            scint_name = row['scint_name']
            configs[date_str][scint_name] = {}

            # Everything else
            configs[date_str][scint_name]['erc'] = row['erc']
            configs[date_str][scint_name]['format_name'] = row['format_name']
            configs[date_str][scint_name]['long_event_search'] = row['long_event_search']

        return configs

    async def _get_instrument_deployment(self, instrument: Annotated[str, Query(max_length=20)],
                                         date: Annotated[str, Query(max_length=6)]) -> List[Dict[str, str | float]]:
        """An endpoint that returns deployment information for a particular instrument.

        Parameters
        ----------
        instrument: the name of the instrument to get deployment information for.

        date: the date to get deployment information for. This should be of the form 'YYMMDD', or an asterisk to get all
        deployments.

        Returns
        -------
        An array containing matching deployments for the given instrument and date. Each deployment entry is of the
        following form: {'start_date': YYMMDD, 'end_date': YYMMDD, 'location': x, 'tz_identifier': x,
        'weather_station': x, 'sounding_station': x, 'latitude': x, 'longitude': x, 'altitude': x, 'notes': x}.

        """
        
        self._logger.debug('Servicing _get_instrument_deployment request.')
        # Making sure that instrument is in a valid format
        if not instrument.isalnum():
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                        detail='Invalid instrument. Instrument must consist of only alphanumeric '
                                               'characters.')

        instrument = instrument.upper()

        # Making sure that date is in a valid format
        if date == '*':
            full_date = datetime.datetime.fromtimestamp(0, datetime.UTC)
        else:
            try:
                full_date = datetime.datetime.strptime(date, '%y%m%d').replace(tzinfo=datetime.UTC)
            except Exception:
                raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                            detail="Invalid date. Date must be of the form 'YYMMDD'.")

        # Getting the deployment(s) from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                cur.row_factory = dict_row
                await cur.execute("""SET TIMEZONE TO UTC;""")
                if date == '*':
                    await cur.execute("""
                    SELECT * FROM tgfserver.get_all_deployments(%s)
                    """, (instrument, ))
                else:
                    await cur.execute("""
                    SELECT * FROM tgfserver.get_deployment(%s, %s);
                    """, (instrument, full_date))

                result = await cur.fetchall()

        if len(result) == 0:
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_404_NOT_FOUND, detail='No deployment(s) found.')

        # Exchanging the datetime objects for strings
        for row in result:
            for key in ['start_date', 'end_date']:
                date_str = row[key].strftime('%y%m%d')
                if date_str == '700101':  # Epoch origin
                    row[key] = '000000'
                else:
                    row[key] = date_str

        return result

    async def _get_weather(self, instrument: Annotated[str, Query(max_length=20)],
                           date: Annotated[str, Query(max_length=6)]) -> List[Dict[str, float | str]]:
        """An endpoint that returns weather information for the given instrument on the given date.

        Parameters
        ----------
        instrument: the name of the instrument to get weather information for.

        date: the date to get weather information for. This should be of the form 'YYMMDD'.

        Returns
        -------
        An array of weather measurements. Each measurement has the following form: {'measurement_time': x_epoch,
        'condition': x}.

        """

        self._logger.debug('Servicing _get_weather request.')
        # Making sure that instrument is in a valid format
        if not instrument.isalnum():
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                        detail='Invalid instrument. Instrument must consist of only alphanumeric '
                                               'characters.')

        instrument = instrument.upper()

        # Making sure that date is in a valid format
        try:
            full_date = datetime.datetime.strptime(date, '%y%m%d').replace(tzinfo=datetime.UTC)
        except Exception:
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_400_BAD_REQUEST,
                                        detail="Invalid date. Date must be of the form 'YYMMDD'.")

        # Getting weather data from the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                cur.row_factory = dict_row
                await cur.execute("""SET TIMEZONE TO UTC;""")
                await cur.execute("""
                SELECT * FROM tgfserver.get_weather(%s, %s);
                """, (instrument, full_date))
                result = await cur.fetchall()

        if len(result) == 0:
            raise fastapi.HTTPException(status_code=fastapi.status.HTTP_404_NOT_FOUND,
                                        detail='No weather data found.')

        # Exchanging the datetime objects for epoch timestamps
        for row in result:
            row['measurement_time'] = row['measurement_time'].timestamp()

        return result

    async def main(self) -> None:
        self._configure_module_loggers()
        log_listener = configure_logging(self.service_name, self._config.log_level)
        log_listener.start()

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

        self._add_signal_handlers()

        # Setting up the api server
        app = FastAPI(title='tgfserver API', description='An API that provides access to UCSC TGF group info.',
                      version='1.0.0', docs_url=None, openapi_url=None, redoc_url=None)

        router = APIRouter()
        router.lifespan_context = self._lifespan
        router.add_api_route('/data-root/', self._get_data_root, methods=['GET'])
        router.add_api_route('/instruments/', self._get_instruments, methods=['GET'])
        router.add_api_route('/scintillators/', self._get_scintillators, methods=['GET'])
        router.add_api_route('/instrument-subdir/', self._get_instrument_subdir, methods=['GET'])
        router.add_api_route('/instrument-config/', self._get_instrument_config, methods=['GET'])
        router.add_api_route('/instrument-deployment/', self._get_instrument_deployment, methods=['GET'])
        router.add_api_route('/weather/', self._get_weather, methods=['GET'])

        app.include_router(router)
        app.add_exception_handler(psycopg.Error, self._database_exception_handler)
        app.add_exception_handler(Exception, self._generic_exception_handler)

        config = uvicorn.Config(app=app, host=self._config.service_host, port=self._config.service_port,
                                log_config={'version': 1, 'disable_existing_loggers': False})
        server = uvicorn.Server(config=config)
        # Overriding uvicorn's normal signal handling behavior via a monkeypatch
        server.install_signal_handlers = lambda x: None

        # Running the api server
        serve_task = asyncio.create_task(server.serve())
        try:
            await asyncio.wait((asyncio.create_task(self._shutdown.wait()), serve_task),
                               return_when=asyncio.FIRST_COMPLETED)

            # Re-raising any exceptions that might've occurred in the serve task
            if serve_task.done():
                serve_task.result()

        except Exception:
            self._logger.critical('Encountered a critical exception:', exc_info=True)
        finally:
            self._logger.info('Shutting down service.')
            if self._shutdown.is_set() and not serve_task.done():
                server.should_exit = True
                await serve_task

            self._logger.info('Shutdown complete.')

            log_listener.stop()
