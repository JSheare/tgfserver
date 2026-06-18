"""A module containing a class that implements the tgfserver application's manager service."""
import aiohttp
import asyncio
import datetime
import logging
import multiprocessing
import os
import playwright
import psycopg
import psycopg.sql as sql
import pydantic
import signal
import weakref
import zoneinfo
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from psycopg.rows import dict_row
from typing import Any, AsyncGenerator, Callable, Dict, List, Set, Type

import tgfserver.config.parameters as params
import tgfserver.validation.sheets_validation as sv
from tgfserver.helpers.async_process import AsyncProcess
from tgfserver.validation.config_validation import ManagerModel
from tgfserver.validation.weather_validation import WeatherModel
from tgfserver.helpers.helper_funcs import expand_path
from tgfserver.helpers.string_tcp import AsyncStringTCP
from tgfserver.services.dispatcher_service import DispatcherService
from tgfserver.services.service_base import ServiceBase
from tgfserver.startup.startup_funcs import default_startup


class ManagerService(ServiceBase):
    """A class that implements the tgfserver application's manager service.

    The manager service is responsible for managing the other services, processing commands, and keeping the database up
    to date.

    Attributes
    ----------
    service_name : str
        The name of the service.
    _config_model : ManagerModel
        A pydantic model used to validate the service's section of the config file.
    _services : Dict[str, weakref.ref]
        A dictionary containing weak references to each service's AsyncProcess.
    _update_db_lock : asyncio.Lock
        A lock that ensures only one database update job can run at once.

    """

    service_name = params.MANAGER_NAME
    _config_model = ManagerModel

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(args, kwargs)

        multiprocessing.set_start_method('spawn')  # Otherwise, child processes will inherit parent signal handlers

        self._services = dict()
        self._update_db_lock = asyncio.Lock()

        self._register_resource_manager(self._pid_file)

        self._register_primary_task(self._command_listener)
        self._register_primary_task(self._service_manager, default_startup, DispatcherService, update_user_config=False)

        self._register_periodic_task(self._update_db, cron=self._config.db_update_time)

    def _configure_module_loggers(self) -> None:
        super()._configure_module_loggers()
        logging.getLogger('aiohttp.client').setLevel(self._config.log_level)
        logging.getLogger('psycopg').setLevel(self._config.log_level)

    async def _reload_callback(self, diff: Set[str]) -> None:
        restart_required = {'db_update_time'}
        if len(diff.intersection(restart_required)) > 0:
            self._logger.warning('Service restart required for some updated config options to take effect.')

    @staticmethod
    async def _pid_file() ->AsyncGenerator[None, Any]:
        """A function responsible for creating and deleting the application's pid file."""
        # Making the file
        runtime_path = expand_path(params.RUNTIME_PATH)
        with open(f'{runtime_path}/{params.PID_FILE}', 'w') as file:
            file.write(str(os.getpid()))

        yield

        # Deleting the file
        try:
            os.remove(f'{runtime_path}/{params.PID_FILE}')
        except Exception:
            pass

    async def _process_commands(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """A callback that processes commands sent through the service's localhost listener."""
        protocol = AsyncStringTCP
        try:
            async for command in protocol.get_messages(reader, timeout=10.0):
                try:
                    match command:
                        case '--status':
                            self._logger.debug("Received command '--status'.")
                            await protocol.send_message(writer, 'Current status of services:')
                            await protocol.send_message(writer, f'{self.service_name} - online')
                            for service in self._services:
                                status = 'offline'
                                if self._services[service] is not None:
                                    process = self._services[service]()
                                    if process is not None and process.is_alive():
                                        status = 'online'

                                await protocol.send_message(writer, f'{service} - {status}')

                            self._logger.debug("Command '--status' executed successfully.")
                        case '--update_db':
                            self._logger.debug("Received command '--update_db'.")
                            if self._update_db_lock.locked():
                                await protocol.send_message(writer, 'Database update already in progress.')
                            else:
                                await protocol.send_message(writer, 'Updating database.')
                                output = await self._update_db()
                                await protocol.send_message(writer, output)

                            self._logger.debug("Command '--update_db' executed successfully.")
                        case '--reload':
                            self._logger.debug("Received command '--reload'.")
                            await protocol.send_message(writer, 'Reloading application.')
                            await self.reload()
                            for service in self._services:
                                if self._services[service] is not None:
                                    process = self._services[service]()
                                    if process is not None and process.is_alive():
                                        self._logger.info(f"Reloading service '{service}'.")
                                        os.kill(process.pid, signal.SIGHUP)

                            self._logger.debug("Command '--reload' executed successfully.")
                        case _:
                            self._logger.debug(f"Received unrecognized command '{command}'.")
                            await protocol.send_message(writer, f"{command} Command not recognized.")

                except Exception:
                    self._logger.exception(f"Encountered an exception when executing command '{command}':")
                    await protocol.send_message(writer, f'Encountered an exception when executing {command} command. '
                                                        f'See log for details.')

                # Sending the signal that the current command is done being processed
                await protocol.send_message(writer, 'SENTINEL')

        except Exception:
            self._logger.exception('Encountered an exception while receiving command(s):')

        # Closing the socket if the command caller hasn't already
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    async def _command_listener(self) -> None:
        """A function responsible for starting and stopping the service's localhost listener."""
        server = None
        try:
            server = await asyncio.start_server(self._process_commands, host='127.0.0.1', backlog=1)
            await self._wait_forever()
        finally:
            if server is not None:
                server.close()

    async def _service_manager(self, startup_func: Callable, service: Type[ServiceBase], *args: Any,
                               **kwargs: Any) -> None:
        """A function responsible for starting, managing, and stopping one of the service's subservices.

        Parameters
        ----------
        startup_func : function
            The startup function to use for starting the service.
        service : tgfserver.services.service_base.ServiceBase and subclasses
            The managed service's class.

        """

        process = None
        self._services[service.service_name] = None
        try:
            starts = 1
            while True:
                self._logger.info(f"Starting service '{service.service_name}'.")
                process = AsyncProcess(target=startup_func, args=(service, args, kwargs))
                process.start()
                interval_start = datetime.datetime.now(datetime.UTC)
                self._services[service.service_name] = weakref.ref(process)
                await process.join()
                # Checking to see if we've restarted too many times within the given interval
                if ((datetime.datetime.now(datetime.UTC) - interval_start).total_seconds() <
                        self._config.start_limit_interval_sec):
                    if starts == self._config.start_limit_burst:
                        break

                # Resetting the interval if we're outside of it
                else:
                    starts = 0
                    interval_start = datetime.datetime.now(datetime.UTC)

                self._logger.error(f"Service '{service.service_name}' crashed or was terminated.")
                starts += 1
                await asyncio.sleep(self._config.restart_sec)

            self._services[service.service_name] = None
            self._logger.error(f"Service '{service.service_name}' crashed or was terminated too many times and is "
                               f"now offline.")
            await self._wait_forever()
        finally:
            if process is not None and process.is_alive():
                self._logger.info(f"Shutting down service '{service.service_name}'.")
                process.terminate()
                await process.join(timeout=self._config.shutdown_timeout_sec)
                if process.is_alive():
                    process.kill()
                    self._logger.info(f"Service '{service.service_name}' exceeded shutdown timeout and was killed.")
                else:
                    self._logger.info(f"Service '{service.service_name}' shut down successfully.")

    async def _update_from_sheet(self, session: aiohttp.ClientSession, conn:psycopg.AsyncConnection, sheet_range: str,
                                 sheet_to_db: Dict[str, str], model_class: Type[pydantic.BaseModel],
                                 query: sql.SQL | Dict[str, sql.SQL]) -> str:
        """A helper function that updates the database from the given sheet using the given validation and the given SQL
        query (or queries).

        Parameters
        ----------
        session : aiohttp.ClientSession
            A client session for making HTTP requests.
        conn : psycopg.AsyncConnection
            A database connection for executing SQL queries.
        sheet_range : str
            A string containing the sheet name and column range to update from in the following format: sheet!x:x.
        sheet_to_db : Dict[str, str]
            A dictionary that maps sheet columns to database columns.
        model_class : pydantic.BaseModel subclasses
            A pydantic model that validates input from the sheet.
        query: sql.SQL | Dict[str, sql.SQL]
            An SQL query or dict mapping columns to SQL queries that updates the relevant database columns and tables.

        """

        sheet_name = sheet_range.split('!')[0]
        self._logger.debug(f"Database update: getting and parsing sheet '{sheet_name}' and updating database.")
        try:
            self._logger.debug(f"Database update: getting sheet '{self._config.spreadsheet_id}' "
                               f"on range '{sheet_range}'.")
            async with session.get(f'https://sheets.googleapis.com/v4/spreadsheets/'
                               f'{self._config.spreadsheet_id}/values/{sheet_range}'
                               f'?key={self._config.sheets_api_key}') as response:
                response.raise_for_status()  # Raises aiohttp.ClientResponseError for HTTP status codes >= 400
                response_json = await response.json()
                if 'values' not in response_json:
                    self._logger.error(f"Database update: failed to parse sheet '{sheet_name}'. Sheet empty.")
                    return f"Error: failed to parse sheet '{sheet_name}'. Sheet empty.\n"

                rows = response_json['values']
        except aiohttp.ClientResponseError:
            self._logger.exception(f"Database update: encountered the following HTTP exception when retrieving sheet "
                                   f"'{sheet_name}':")
            return f"Error: failed to retrieve sheet '{sheet_name}'. See log for details.\n"
        except asyncio.exceptions.TimeoutError:
            self._logger.exception(f"Database update: could not retrieve sheet '{sheet_name}'. No response from Google "
                                   f"Sheets API.")
            return f"Error: failed to retrieve sheet '{sheet_name}'. See log for details.\n"

        # Mapping list indices to their corresponding columns
        column_map = {}
        for i in range(len(rows[0])):
            column = rows[0][i]
            if column in sheet_to_db:
                column_map[i] = sheet_to_db[column]

        # Returning immediately if a column is missing
        if len(column_map) != len(sheet_to_db):
            self._logger.error(f"Database update: failed to parse sheet '{sheet_name}'. "
                               "One or more missing columns.")
            return f"Error: failed to parse sheet '{sheet_name}'. One or more missing columns.\n"

        successful_rows = 0
        async with conn.cursor() as cur:
            for i in range(1, len(rows)):
                # Giving the event loop the chance to do other things between rows
                await asyncio.sleep(0)

                # Adding empty string padding at the end of the row if it's too short
                len_diff = len(rows[0]) - len(rows[i])
                while len_diff > 0:
                    rows[i].append('')
                    len_diff -= 1

                # Validating the row
                try:
                    model = model_class(**{column_map[j]: rows[i][j] for j in column_map})
                except pydantic.ValidationError as ex:
                    issues_strings = []
                    num_errors = len(ex.errors())
                    for j in range(num_errors):
                        error = ex.errors()[j]
                        issues_strings.append(f"'{error["input"]}' - {error["msg"]}")
                        if j != num_errors - 1:
                            issues_strings.append('\n')

                    self._logger.warning(f"Database update: failed to validate value(s) on row {i} of sheet "
                                         f"'{sheet_name}'. Issues: \n{''.join(issues_strings)}")
                    continue

                # Updating the row into the database
                if isinstance(query, dict):  # One sheet maps values to multiple tables
                    row_dict = model.model_dump()
                    for column in row_dict:
                        if row_dict[column] == '':
                            continue

                        try:
                            await cur.execute(query[column], (row_dict[column], ))
                        except psycopg.Error:
                            self._logger.exception(f"Database update: encountered database exception when "
                                                   f"updating values from row {i} of sheet '{sheet_name}':")
                            continue

                else:  # One table per sheet
                    try:
                        await cur.execute(query, model.model_dump())
                    except psycopg.Error:
                        self._logger.exception(f"Database update: encountered database exception when "
                                               f"updating values from row {i} of sheet '{sheet_name}':")
                        continue

                successful_rows += 1

        # Checking to see how many rows were updated successfully and generating the correct output
        total_rows = len(rows) - 1  # The first row is always just the column names, hence the - 1
        if total_rows - successful_rows == 0:
            self._logger.debug(f"Database update: successfully parsed and updated {successful_rows} rows from "
                               f"sheet '{sheet_name}'.")
            return f"Successfully parsed and updated {successful_rows} rows from sheet '{sheet_name}'.\n"
        else:
            self._logger.debug(f"Database update: {successful_rows}/{total_rows} rows from sheet '{sheet_name}' "
                               f"parsed and updated.")
            return (f"{successful_rows}/{total_rows} rows from sheet '{sheet_name}' parsed and updated. "
                    f"See log for details.\n")

    @staticmethod
    def _parse_html_for_table(content: str) -> List[Dict[str, Any]]:
        """Parses the given HTML for a weather table and returns it as a list of dictionaries: one for each row."""
        soup = BeautifulSoup(content, 'lxml')
        table_list = soup.find_all('table')
        # Historical weather data pages have two tables. We're interested in the second one
        table = table_list[1]

        # Maps webpage table column names to more programmer-friendly names
        column_map = {'Time': 'measurement_time', 'Temperature': 'temperature_f', 'Dew Point': 'dew_point_f',
                      'Humidity': 'humidity_percent', 'Wind': 'wind', 'Wind Speed': 'wind_speed_mph',
                      'Wind Gust': 'wind_gust_mph', 'Pressure': 'pressure_inches', 'Precip.': 'precipitation_inches',
                      'Condition': 'condition'}

        data = []
        headers = [header.text for header in table.find_all('th')]
        for row in table.find_all('tr'):
            row_dict = {column_map[headers[i]]: entry.text.strip() for i, entry in enumerate(row.find_all('td'))}
            if not len(row_dict) == 0:
                data.append(row_dict)

        return data

    async def _scrape_weather_table(self, page: playwright.async_api.Page, local_date: datetime.datetime,
                                    weather_station: str) -> List[Dict[str, Any]]:
        """Scrapes a weather website for weather data from the given weather station on the given local date."""
        await page.goto(f'https://www.wunderground.com/history/daily/'
                        f'{weather_station}/date/{local_date.strftime("%Y-%m-%d")}',
                        wait_until='domcontentloaded')
        # Waiting for the javascript that fetches the tables to run
        await page.wait_for_selector('table', timeout=self._config.scrape_timeout_sec * 1000)

        rows = await asyncio.to_thread(self._parse_html_for_table, await page.content())
        # Validating and coercing the rows and returning them
        beginning_of_day = local_date.timestamp() - (local_date.hour * 60 ** 2 + local_date.minute * 60 +
                                                     local_date.second + local_date.microsecond * 1e-6)
        for i in range(len(rows)):
            rows[i] = WeatherModel(**rows[i]).model_dump()
            # Replacing the clock time with a timestamp in UTC
            rows[i]['measurement_time'] = datetime.datetime.fromtimestamp(
                beginning_of_day + rows[i]['measurement_time'], datetime.UTC)
            # Giving the event loop the chance to do other things between rows
            await asyncio.sleep(0)

        return rows

    async def _update_weather(self, page: playwright.async_api.Page, conn: psycopg.AsyncConnection,
                              instrument: str) -> str:
        """A function that scrapes missing weather info for the given instrument and stores it in the database."""
        self._logger.info(f"Database update: scraping weather for instrument '{instrument}'.")
        successful_days = 0
        failed_days = False
        retrieval_failure = False
        try:
            async with conn.cursor() as cur:
                cur.row_factory = dict_row
                try:
                    await cur.execute("""SELECT * FROM tgfserver.get_all_deployments(%s);""", (instrument,))
                    deployments = await cur.fetchall()
                except psycopg.Error:
                    self._logger.exception(f"Database update: encountered a database exception when attempting to get "
                                           f"deployments list for instrument '{instrument}':")
                    return (f'Failed to scrape weather for {instrument} due to database exception. '
                            f'See log for details.\n')

                for deployment in deployments:
                    self._logger.debug(f"Database update: scraping and storing weather data for instrument "
                                       f"'{instrument}' deployment from "
                                       f"{deployment['start_date'].strftime("%Y-%m-%d")} "
                                       f"to {deployment['end_date'].strftime("%Y-%m-%d")} "
                                       f"with weather station '{deployment['weather_station']}'.")
                    # Skipping deployments that haven't started yet
                    if deployment['start_date'] > datetime.datetime.now(datetime.UTC):
                        continue

                    # Getting a list of missing weather dates based on what's in the database already
                    now = datetime.datetime.now(datetime.UTC)
                    if deployment['end_date'] > now:
                        end_date = now
                    else:
                        end_date = deployment['end_date']

                    try:
                        await cur.execute("""SELECT * FROM tgfserver.get_missing_weather_dates(%s, %s, %s)""",
                                          (instrument, deployment['start_date'], end_date))
                        missing_dates = await cur.fetchall()
                    except psycopg.Error:
                        self._logger.exception(f"Database update: encountered a database exception when attempting to "
                                               f"get missing date list for instrument '{instrument}' deployment:")
                        failed_days = True
                        continue

                    if len(missing_dates) == 0:
                        self._logger.debug(f"Database update: no new weather data to scrape for instrument "
                                           f"'{instrument}' deployment from "
                                           f"{deployment['start_date'].strftime("%Y-%m-%d")} "
                                           f"to {deployment['end_date'].strftime("%Y-%m-%d")} "
                                           f"with weather station '{deployment['weather_station']}'.")

                    missing_dates = [s['missing_date'] for s in missing_dates]
                    for missing_date in missing_dates:
                        self._logger.debug(f"Database update: scraping weather data from "
                                           f"{missing_date.strftime("%Y-%m-%d")} for instrument '{instrument}'.")
                        try:
                            local_date = missing_date.astimezone(zoneinfo.ZoneInfo(deployment['tz_identifier']))
                            # Checking to see that the day has completely elapsed and skipping it if it hasn't
                            if (datetime.datetime.now(datetime.UTC) - local_date) < datetime.timedelta(days=1):
                                continue

                            rows = await self._scrape_weather_table(page, local_date, deployment['weather_station'])
                            retrieval_failure = False
                            for row in rows:
                                row['instrument'] = instrument

                            # Inserting the data inside a transaction so that we always get whole days
                            async with conn.transaction():
                                await cur.executemany("""CALL tgfserver.insert_into_weather(%(instrument)s, 
                                                                                            %(measurement_time)s, 
                                                                                            %(condition)s);""", rows)

                            successful_days += 1
                        except playwright.async_api.Error:
                            self._logger.exception(f"Database update: failed to retrieve weather data "
                                                   f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                                   f"weather station '{deployment['weather_station']}') "
                                                   f"for instrument '{instrument}' due to an exception:")
                            failed_days = True
                            if not retrieval_failure:
                                retrieval_failure = True
                            else:
                                self._logger.warning(f"Database update: encountered multiple weather data retrieval "
                                                     f"failures in a row. This could indicate a connection issue, or "
                                                     f"it could indicate that either the weather station or the "
                                                     f"weather website URL are incorrect. Stopping scraping attempts "
                                                     f"for this deployment.")
                                break
                        except IndexError:
                            self._logger.error(f"Database update: failed to scrape weather data "
                                               f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                               f"weather station '{deployment['weather_station']}') "
                                               f"for instrument '{instrument}'. Measurements table missing. "
                                               f"Stopping scraping attempts for this instrument.")
                            failed_days = True
                            raise RuntimeError
                        except KeyError:
                            self._logger.error(f"Database update: failed to scrape weather data "
                                               f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                               f"weather station '{deployment['weather_station']}') "
                                               f"for instrument '{instrument}'. Measurements table contains unexpected "
                                               f"columns. Stopping scraping attempts for this instrument.")
                            failed_days = True
                            raise RuntimeError
                        except pydantic.ValidationError as ex:
                            issues_strings = []
                            num_errors = len(ex.errors())
                            for j in range(num_errors):
                                error = ex.errors()[j]
                                issues_strings.append(f"'{error["input"]}' - {error["msg"]}")
                                if j != num_errors - 1:
                                    issues_strings.append('\n')

                            self._logger.error(f"Database update: encountered a validation error when attempting "
                                                f"to validate scraped weather data "
                                                f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                                f"weather station '{deployment['weather_station']}') "
                                                f"for instrument '{instrument}'. "
                                                f"Issues: \n{''.join(issues_strings)}. Stopping scraping attempts "
                                                f"for this instrument.")
                            failed_days = True
                            raise RuntimeError
                        except psycopg.Error:
                            self._logger.exception(f"Database update: encountered a database exception when attempting "
                                                   f"to insert weather data "
                                                   f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                                   f"weather station '{deployment['weather_station']}') "
                                                   f"into the database for instrument '{instrument}':")
                            failed_days = True
                        except Exception:
                            self._logger.exception(f"Database update: encountered an exception when attempting "
                                                   f"to scrape sweather data "
                                                   f"(local date: {local_date.strftime("%Y-%m-%d")}; "
                                                   f"weather station '{deployment['weather_station']}') "
                                                   f"for instrument '{instrument}':")
                            failed_days = True

                        # Waiting for at least one second between scrapes to avoid triggering rate limiting
                        await asyncio.sleep(1)

        except RuntimeError:
            # This will be triggered if we encounter any page-formatting-related issues during scraping
            pass
        except Exception:
            self._logger.exception(f"Database update: encountered an exception when scraping weather data for "
                                   f"instrument '{instrument}':")
            return f'Encountered an exception when scraping weather data for {instrument}. See log for details.\n'

        if successful_days != 0:
            if failed_days:
                self._logger.info(f"Database update: successfully scraped {successful_days} day(s) of weather for "
                                  f"instrument '{instrument}'. Scrape failed for some days.")
                return (f"Successfully scraped {successful_days} day(s) of weather data for instrument '{instrument}'. "
                        f'See log for details on failures.\n')
            else:
                self._logger.info(f"Database update: successfully scraped {successful_days} day(s) of weather for "
                                  f"instrument '{instrument}'.")
                return f"Successfully scraped {successful_days} day(s) of weather data for instrument '{instrument}'.\n"

        else:
            if failed_days:
                self._logger.info(f"Database update: failed to scrape weather data for instrument '{instrument}'.")
                return f"Failed to scrape weather data for instrument '{instrument}'. See log for details.\n"
            else:
                self._logger.info(f"Database update: no new weather data to scrape for instrument '{instrument}'.")
                return ''

    async def _update_db(self) -> str:
        """A function that updates the database that backs the tgfserver application."""
        async with self._update_db_lock:
            self._logger.info('Updating database.')
            output_strings = []
            try:
                async with await psycopg.AsyncConnection.connect(
                    f'host={self._config.db_host} '
                    f'port={self._config.db_port} '
                    f'connect_timeout={self._config.db_connect_timeout_sec} '
                    f'dbname={self._config.db_name} '
                    f'user={self._config.db_user} '
                    f'password={self._config.db_password}',
                    autocommit=True) as conn:

                    # Setting database session timezone to UTC
                    await conn.execute("""SET TIMEZONE TO UTC;""")

                    async with aiohttp.ClientSession() as session:
                        # Attempting to get, parse, and update from the general sheet
                        output_strings.append(await self._update_from_sheet(
                            session,
                            conn,
                            'General!A:B',
                            {'Data Root': 'data_root', 'List Mode Formats': 'format_name'},
                            sv.GeneralModel,
                            {   'data_root': sql.SQL('CALL tgfserver.insert_into_general(%s);'),
                                'format_name': sql.SQL('CALL tgfserver.insert_into_lm_formats(%s);')}))

                        # Attempting to get, parse, and update from the other sheets
                        # Note that, because some of the database tables depend on one another, the execution order of
                        # these is important

                        # Instruments sheet
                        output_strings.append(await self._update_from_sheet(
                            session,
                            conn,
                            'Instruments!A:B',
                            {'Name': 'instrument_name', 'Data Subdirectory': 'subdir'},
                            sv.InstrumentsModel,
                            sql.SQL("""CALL tgfserver.insert_into_instruments(%(instrument_name)s, %(subdir)s);""")))

                        # Scintillators sheet
                        output_strings.append(await self._update_from_sheet(
                            session,
                            conn,
                            'Scintillators!A:C',
                            {   'Scintillator Name': 'scint_name', 'Scintillator Priority': 'scint_priority',
                                'Plot Color': 'plot_color'},
                            sv.ScintillatorsModel,
                            sql.SQL("""CALL tgfserver.insert_into_scintillators(%(scint_name)s, %(scint_priority)s, 
                                                                                %(plot_color)s);""")))

                        # Configurations sheet
                        output_strings.append(await self._update_from_sheet(
                            session,
                            conn,
                            'Configurations!A:F',
                            {   'Instrument': 'instrument_name', 'After Date': 'after_date',
                                'Scintillator': 'scint_name', 'eRC': 'erc', 'List Mode Format': 'format_name',
                                'Long Event Search': 'long_event_search'},
                            sv.ConfigurationsModel,
                            sql.SQL("""CALL tgfserver.insert_into_configurations(%(instrument_name)s, %(after_date)s, 
                                                                                 %(scint_name)s, %(erc)s, 
                                                                                 %(format_name)s, 
                                                                                 %(long_event_search)s);""")))

                        # Deployments sheet
                        output_strings.append(await self._update_from_sheet(
                            session,
                            conn,
                            'Deployments!A:K',
                            {   'Location': 'location', 'Instrument': 'instrument_name', 'Start date': 'start_date',
                                'End date': 'end_date', 'Timezone': 'tz_identifier',
                                'Nearest weather station': 'weather_station',
                                'Nearest sounding station': 'sounding_station',
                                'Latitude (N)': 'latitude', 'Longitude (E, 0-360)': 'longitude',
                                'Altitude (km)': 'altitude', 'Notes': 'notes'},
                            sv.DeploymentsModel,
                            sql.SQL("""CALL tgfserver.insert_into_deployments(%(instrument_name)s, %(start_date)s, 
                                                                              %(end_date)s, %(location)s, 
                                                                              %(tz_identifier)s, %(weather_station)s, 
                                                                              %(sounding_station)s, %(latitude)s, 
                                                                              %(longitude)s, %(altitude)s,
                                                                              %(notes)s);""")))

                    # Scraping weather for each instrument
                    if self._config.scrape_weather:
                        async with conn.cursor() as cur:
                            await cur.execute("""SELECT * FROM tgfserver.get_instruments();""")
                            instruments = [s[0] for s in await cur.fetchall()]

                        async with async_playwright() as pw:
                            browser = await pw.firefox.launch()
                            context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 11.0; Win64; x64)')
                            page = await context.new_page()
                            for instrument in instruments:
                                output_strings.append(await self._update_weather(page, conn, instrument))

                            await browser.close()

            except psycopg.Error as ex:
                self._logger.exception('Database update: encountered the following database exception:')
                output_strings.append(f'Error: encountered a database exception: {ex}.\n')

            output = ''.join(output_strings)
            if len(output) > 0 and output[-1] == '\n':
                output = output[:-1]

            return output
