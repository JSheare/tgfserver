"""A module containing classes that implement the tgfserver application's instrument dispatcher service."""
from __future__ import annotations

import aiohttp
import argon2
import asyncio
import base64
import datetime
import json
import logging
import math
import pathlib
import psycopg
import psycopg_pool
import pydantic
import statistics
import weakref
from aiohttp import web
from dataclasses import dataclass
from email.message import EmailMessage
from enum import IntEnum
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from types import SimpleNamespace
from typing import Any, AsyncGenerator, Dict, List, Set, Tuple

import tgfserver.config.parameters as params
import tgfserver.validation.dispatcher_validation as dv
from tgfserver.startup.config_funcs import read_config, write_config
from tgfserver.validation.config_validation import DispatcherModel
from tgfserver.helpers.helper_funcs import expand_path, read_pickle_file, write_pickle_file
from tgfserver.helpers.lockout_cache import LockoutCache
from tgfserver.services.service_base import ServiceBase


@dataclass
class ClientInfo:
    """A helper class that keeps track of registered client info for the scheduler."""
    name: str
    target_date: datetime.datetime
    days_since_transfer: int
    transfer_rate: float
    slot_size: int

    def __eq__(self, other: ClientInfo) -> bool:
        """Magic method for equality. Returns True if instances are of equal priority."""
        return self.days_since_transfer == other.days_since_transfer and self.transfer_rate == other.transfer_rate

    def __lt__(self, other: ClientInfo) -> bool:
        """Magic method for less than. Returns True if the current instance is of lower priority."""
        if self.days_since_transfer < other.days_since_transfer:
            return True
        elif self.days_since_transfer > other.days_since_transfer:
            return False
        else:
            return self.transfer_rate < other.transfer_rate

    def __le__(self, other: ClientInfo) -> bool:
        """Magic method for less than or equal to. Returns True if the current instance is of lower or equal
        priority."""
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other: ClientInfo) -> bool:
        """Magic method for greater than. Returns True if the current instance is of greater priority."""
        if self.days_since_transfer > other.days_since_transfer:
            return True
        elif self.days_since_transfer < other.days_since_transfer:
            return False
        else:
            return self.transfer_rate > other.transfer_rate

    def __ge__(self, other: ClientInfo) -> bool:
        """Magic method for greater than or equal to. Returns True if the current instance is of greater or equal
        priority."""
        return self.__gt__(other) or self.__eq__(other)


class TransferScheduler:
    """A class that implements file transfer scheduling operations for the instrument dispatcher service.

    Parameters
    ----------
    config : DispatcherModel
        The instrument dispatcher service's config.

    Attributes
    ----------
    _day_map : Dict[int, bool]
        A dictionary mapping days of the week to whether they're valid for scheduling
    _waitlist : Dict[str, ClientInfo]
        A dictionary containing info of clients waitlisted for scheduling.
    _waitlist_lock : asyncio.Lock
        A lock used to prevent race conditions when writing the waitlist to a file.
    _waitlist_writes_pending : int
        A counter for the number of waitlist file writing jobs that are pending.
    _waitlist_available : asyncio.Event
        An event signaling that waitlist registration is open.
    _schedule : Dict[str, Tuple[datetime.datetime]]
        A dictionary containing the start and end timestamps for each scheduled client's transfer time slot.
    _schedule_available : asyncio.Event
        An event signaling that the schedule is available.

    """

    def __init__(self, config: DispatcherModel) -> None:
        self._config = config
        self._day_map = self._get_day_map(self._config.start_day, self._config.end_day)
        self._waitlist = self._read_waitlist()
        self._waitlist_lock = asyncio.Lock()
        self._waitlist_writes_pending = 0
        self._waitlist_available = asyncio.Event()
        self._schedule = self._read_schedule()
        self._schedule_available = asyncio.Event()

        self._waitlist_available.set()
        self._schedule_available.set()

    @staticmethod
    def _get_target_date() -> datetime.datetime:
        """Returns the target date for scheduling (the day after the current day)."""
        system_timezone = datetime.datetime.now().astimezone()
        next_day = datetime.datetime.now(system_timezone.tzinfo) + datetime.timedelta(seconds=86400)
        return next_day.replace(hour=0, minute=0, second=0, microsecond=0)  # Returning the start of the day

    @staticmethod
    def _get_day_map(start_day: int, end_day: int) -> Dict[int, bool]:
        """Returns a dictionary that maps days of the week to whether they're valid for scheduling."""
        days = {}
        total_days = 0
        current_day = start_day
        current_flag = True
        while total_days < 7:
            days[current_day] = current_flag

            if current_day == end_day:
                current_flag = False

            current_day += 1
            if current_day > 7:
                current_day = 1

            total_days += 1

        return days

    @staticmethod
    def _read_waitlist() -> Dict[str, ClientInfo]:
        """Returns the waitlist from a pkl file."""
        file = f'{expand_path(params.DATA_PATH, make_dir=False)}/waitlist.pkl'
        if pathlib.Path(file).exists():
            return read_pickle_file(file)
        else:
            return dict()

    @staticmethod
    def _read_schedule() -> Dict[str, List[datetime.datetime]]:
        """Returns the schedule from a pkl file."""
        file = f'{expand_path(params.DATA_PATH, make_dir=False)}/schedule.pkl'
        if pathlib.Path(file).exists():
            return read_pickle_file(file)
        else:
            return dict()

    async def _write_waitlist(self) -> None:
        """Writes the waitlist to a pkl file."""
        self._waitlist_writes_pending += 1
        async with self._waitlist_lock:
            self._waitlist_writes_pending -= 1
            # Only write to the file if there are no other write jobs pending. This ensures that the waitlist file
            # isn't redundantly rewritten. This works based on the assumption that asyncio.Lock is fair (FIFO), which
            # is guaranteed by the docs as of writing
            if self._waitlist_writes_pending == 0:
                file = f'{expand_path(params.DATA_PATH)}/waitlist.pkl'
                # Doing the writing in another thread to prevent filesystem I/O lag from blocking the event loop
                await asyncio.to_thread(write_pickle_file, self._waitlist, file)

    async def _write_schedule(self) -> None:
        """Writes the schedule to a pkl file."""
        file = f'{expand_path(params.DATA_PATH)}/schedule.pkl'
        # Doing the writing in another thread to prevent filesystem I/O lag from blocking the event loop
        await asyncio.to_thread(write_pickle_file, self._schedule, file)

    def get_rate(self, measured_rates: List[float], timestamps: List[datetime.datetime]) -> float:
        """Calculates the client's projected transfer rate based on the passed measurements and their timestamps.

        Parameters
        ----------
        measured_rates : List[float]
            The client's transfer rate measurements.
        timestamps : List[datetime.datetime]
            The timestamps for each measured rate

        Returns
        -------
        float
            The transfer rate. -1.0 is returned if there are no measurements, or if the measurements are too old.

        """

        # Returning an invalid rate if there are no measurements
        if len(measured_rates) == 0:
            return -1.0

        now = datetime.datetime.now(datetime.UTC)
        valid_rates = []
        for i in range(len(measured_rates)):
            if (0 <= (now - timestamps[i]).total_seconds() < self._config.stale_stats_thresh_sec and
                    measured_rates[i] > 0):
                valid_rates.append(measured_rates[i])

        # Returning an invalid rate if all the measurements are too old
        if len(valid_rates) == 0:
            return -1.0

        if len(valid_rates) > 1:
            mean = statistics.mean(valid_rates)
            standard_dev = statistics.stdev(valid_rates)
            # One standard deviation tends to undershoot, so we're using two
            return mean + 2 * standard_dev

        return valid_rates[0]

    async def register(self, client_name: str, total_bytes: int, transfer_rate: float,
                       last_transfer: datetime.datetime) -> Tuple[bool, str]:
        """Attempts to register the client with the given info on the scheduling waitlist.

        Parameters
        ----------
        client_name : str
            The name of the client instrument.
        total_bytes : int
            The total number of bytes that the client wants to transfer.
        transfer_rate : float
            The client's calculated data transfer rate (bytes/sec).
        last_transfer : datetime.datetime
            The timestamp of the client's last successful data transfer

        Returns
        -------
            Tuple[bool, str]
                A bool indicating whether the registration was successful, and a string indicating the reason.

        """

        await self._waitlist_available.wait()  # Waiting for waitlist registration to open
        target_date = self._get_target_date()
        # Rejecting the registration if the next day isn't valid for scheduling or if there's nothing to transfer
        if not self._day_map[target_date.isoweekday()] or total_bytes <= 0:
            return False, 'no time available'

        if transfer_rate == -1.0:  # This will happen if the clients' stats get reset/don't exist
            slot_size = self._config.max_slot_size_sec
        else:
            slot_size = math.ceil(total_bytes / transfer_rate)
            # Rejecting the registration if the required time goes above the max slot size
            if slot_size > self._config.max_slot_size_sec:
                return False, 'transfer too large'

        # Updating the waitlist with the new registration
        days_since_transfer = math.ceil((datetime.datetime.now(datetime.UTC) - last_transfer).total_seconds() / 86400)
        info = ClientInfo(client_name, target_date, days_since_transfer, transfer_rate, slot_size)
        self._waitlist[client_name] = info

        # Writing the updated waitlist to a file
        await self._write_waitlist()

        return True, 'success'

    async def get_time(self, client_name: str) -> Tuple[datetime.datetime, datetime.datetime]:
        """Returns the time slot for the given client, if it exists.

        Parameters
        ----------
        client_name : str
            The name of the client instrument.

        Returns
        -------
            Tuple[datetime.datetime, datetime.datetime]
                The start and end timestamps of the time slot. If no slot exists, both timestamps will be the Unix
                epoch.

        """

        await self._schedule_available.wait()  # Waiting for the schedule to become available
        invalid_time = datetime.datetime.fromtimestamp(0, datetime.UTC)  # Just using unix epoch for this
        if client_name in self._schedule:
            start_time, end_time = self._schedule[client_name]
            # If the scheduled time has already passed, treat it like a failed scheduling
            if end_time <= datetime.datetime.now(datetime.UTC):
                return invalid_time, invalid_time
            else:
                return start_time, end_time

        else:
            return invalid_time, invalid_time

    async def make_schedule(self) -> None:
        """Makes up the file transfer schedule according to waitlisted clients' priority."""
        # Denying new waitlist registrations and schedule retrievals until we're done
        self._waitlist_available.clear()
        self._schedule_available.clear()

        # Making the schedule
        self._schedule.clear()
        target_date = self._get_target_date()
        current_start = (target_date + datetime.timedelta(seconds=self._config.start_time_sec)).timestamp()
        time_remaining = self._config.end_time_sec - self._config.start_time_sec
        # Sorting registered clients from highest to lowest priority
        clients = sorted([self._waitlist[s] for s in self._waitlist], reverse=True)
        for client in clients:
            # Checking to make sure that the client is trying to schedule for the target day
            if client.target_date != target_date:
                continue

            # Scheduling the client if there's enough time remaining in the window
            if time_remaining > client.slot_size:
                start_time = datetime.datetime.fromtimestamp(current_start, datetime.UTC)
                end_time = datetime.datetime.fromtimestamp(current_start + client.slot_size, datetime.UTC)
                self._schedule[client.name] = [start_time, end_time]
                current_start += client.slot_size + self._config.gap_size_sec
                time_remaining -= client.slot_size + self._config.gap_size_sec

        self._waitlist.clear()

        # Writing the updated waitlist and schedule to files
        await self._write_waitlist()
        await self._write_schedule()

        self._schedule_available.set()

        # Waiting until the beginning of the target day to reopen waitlist registration
        await asyncio.sleep((target_date - datetime.datetime.now(datetime.UTC)).total_seconds())
        self._waitlist_available.set()


class IDStatusCode(IntEnum):
    """An integer enum class containing status codes used by the instrument dispatcher service when communicating."""
    OK = 0
    UNAUTHORIZED = 1
    INVALID_OPERATION = 2
    NO_TIME_AVAILABLE = 3
    TRANSFER_TOO_LARGE = 4


class DispatcherSession:
    """A class that implements an instrument-dispatcher-client exchange.

    Parameters
    ----------
    config: DispatcherModel
        The instrument dispatcher service's config.
    logger: logging.Logger
        The instrument dispatcher service's logger.
    pool: psycopg_pool.AsyncConnectionPool
        The instrument dispatcher service's database connection pool.
    ph: argon2.PasswordHasher
        The instrument dispatcher service's password hasher.
    scheduler: tgfserver.helpers.transfer_scheduler.TransferScheduler
        The instrument dispatcher service's transfer scheduler.
    client_addr: str
        The ip address of the client.

    Attributes
    ----------
    _session : SimpleNamespace
        A simple namespace used to store session info.

    """

    def __init__(self, config: DispatcherModel, logger: logging.Logger, ip_cache: LockoutCache,
                 pool: psycopg_pool.AsyncConnectionPool, ph: argon2.PasswordHasher, scheduler: TransferScheduler,
                 client_addr: str) -> None:
        # Resources used during the session
        self._config = config
        self._logger = logger
        self._ip_cache = ip_cache
        self._pool = pool
        self._ph = ph
        self._scheduler = scheduler

        # Session specific data
        self._client_addr = client_addr
        self._session = SimpleNamespace()

    async def get_response(self, raw_message: str) -> Tuple[str, bool]:
        """Acts on the given raw message and returns the appropriate raw response, along with a bool indicating
        whether the exchange is over.

        Parameters
        ----------
        raw_message : str
            The raw message from the client.

        Returns
        -------
        Tuple[str, bool]
            The appropriate raw response, and a bool indicating whether the exchange is over.

        Raises
        ------
        pydantic.ValidationError
            If the passed message isn't properly formed.
        psycopg.Error
            If a database issue is encountered during the session.

        """

        message = dv.MessageModel.model_validate_json(raw_message, strict=True)
        if not self._client_authenticated():
            if message.type == 'authentication':
                status, response_payload, done = await self._authenticate(message.payload)
            else:
                status = IDStatusCode.UNAUTHORIZED
                response_payload = {'reason': 'client never authenticated or was unsuccessful'}
                done = True

        else:
            if message.type == 'authentication':
                status = IDStatusCode.OK
                response_payload = {}
                done = False
            elif message.type == 'check_in':
                status, response_payload, done = await self._check_in(message.payload)
            elif message.type == 'negotiation':
                status, response_payload, done = await self._negotiate(message.payload)
            elif message.type == 'callback':
                status, response_payload, done = await self._callback(message.payload)
            else:
                status = IDStatusCode.INVALID_OPERATION
                response_payload = {'reason': 'operation not found'}
                done = True

        return json.dumps({'status': status, 'payload': response_payload}), done

    def _client_authenticated(self) -> bool:
        """A helper function that checks if the client has been authenticated."""
        return hasattr(self._session, 'authenticated') and self._session.authenticated

    async def _authenticate(self, raw_payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], bool]:
        """A function implementing the authentication operation."""
        # Validating the payload
        payload = dv.AuthenticationModel(**raw_payload)

        # Checking that the client's ip isn't locked out
        if self._ip_cache.is_locked_out(self._client_addr):
            self._logger.info(f'Refusing authentication request from {self._client_addr} due to temporary lockout.')
            return IDStatusCode.UNAUTHORIZED, {'reason': 'too many attempts'}, True

        # Retrieving the client's hashed password from the database
        self._session.instrument = payload.instrument
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                SELECT tgfserver.get_instrument_password(%s)
                """, (self._session.instrument,))
                password_hash = (await cur.fetchone())[0]

        # Checking the client's password
        self._session.authenticated = False
        if password_hash is None:
            self._logger.info(f'Invalid instrument name from {self._client_addr}.')
        else:
            try:
                await asyncio.to_thread(self._ph.verify, password_hash, payload.password)
                self._session.authenticated = True
            except argon2.exceptions.VerifyMismatchError:
                self._logger.info(f'Incorrect password from {self._client_addr}.')

        # Rejecting the client if authentication was unsuccessful
        if not self._session.authenticated:
            self._ip_cache.increment_attempts(self._client_addr)
            return IDStatusCode.UNAUTHORIZED, {'reason': 'invalid instrument or password'}, True

        self._ip_cache.reset_attempts(self._client_addr)
        self._logger.info(f'Successful login as {self._session.instrument} from {self._client_addr}.')

        # Updating the password hash if it needs rehashing
        if self._ph.check_needs_rehash(password_hash):
            new_hash = await asyncio.to_thread(self._ph.hash, payload.password)
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                    CALL tgfserver.update_instrument_password(%s, %s);
                    """, (self._session.instrument, new_hash))

        return IDStatusCode.OK, {}, False

    async def _check_in(self, raw_payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], bool]:
        """A function implementing the check in operation."""
        # Decoding and validating the payload
        payload = dv.CheckInModel(**raw_payload)

        # Updating the check in info in the database
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""SET TIMEZONE TO UTC;""")
                await cur.execute("""
                CALL tgfserver.update_instrument_check_in(%s, %s, %s)
                """, (self._session.instrument, payload.storage_frac, payload.gps))

        self._logger.info(f'Successful check in for {self._session.instrument} from {self._client_addr}.')
        return IDStatusCode.OK, {}, False

    async def _negotiate(self, raw_payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], bool]:
        """A function implementing the file transfer negotiation operation."""
        # Decoding and validating the payload
        payload = dv.NegotiationModel(**raw_payload)

        # Getting the transfer rate and the last transfer date if we haven't already
        if not hasattr(self._session, 'transfer_rate'):
            self._session.transfer_rate = self._scheduler.get_rate(payload.measured_rates, payload.timestamps)
            if len(payload.timestamps) > 0:
                self._session.last_transfer = max(payload.timestamps)
            else:
                self._session.last_transfer = datetime.datetime.fromtimestamp(0, datetime.UTC)

        # Attempting to register on the waitlist
        success, reason = await (self._scheduler.register(self._session.instrument, payload.total_bytes,
                                                         self._session.transfer_rate, self._session.last_transfer))
        if not success:
            self._logger.debug(f'Transfer waitlist registration failed for {self._session.instrument} from '
                               f'{self._client_addr}. Reason: {reason}.')
            if reason == 'transfer too large':
                return (IDStatusCode.TRANSFER_TOO_LARGE,
                        {'reason': 'too many bytes to fit transfer within normal window'},
                        False)
            else:
                return IDStatusCode.NO_TIME_AVAILABLE, {'reason': 'no time available for scheduling'}, False

        else:
            self._logger.info(f'Successful transfer waitlist registration for {self._session.instrument} from '
                              f'{self._client_addr}.')
            timezone = datetime.datetime.now().astimezone()
            callback_time = self._config.scheduling_deadline.schedule(timezone_str=timezone.tzname()).next()
            return IDStatusCode.OK, {'callback_time': callback_time.astimezone(datetime.UTC).isoformat()}, False

    async def _callback(self, raw_payload: str) -> Tuple[int, Dict[str, Any], bool]:
        """A function implementing the schedule callback operation."""
        # Getting the instrument's scheduled transfer time from the scheduler
        start_time, end_time = await self._scheduler.get_time(self._session.instrument)
        # Checking to see if the times are valid
        if start_time.timestamp() == 0:
            return IDStatusCode.NO_TIME_AVAILABLE, {'reason': 'all available time was booked'}, False
        else:
            # Getting the current data root and instrument subdirectory from the database
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                    SELECT tgfserver.get_data_root();
                    """)
                    data_root = (await cur.fetchone())[0]
                    await cur.execute("""
                    SELECT tgfserver.get_instrument_subdir(%s)
                    """, (self._session.instrument,))
                    subdir = (await cur.fetchone())[0]

        self._logger.info(f'Successful callback for {self._session.instrument} from {self._client_addr}.')
        return (IDStatusCode.OK,
                {'start_time': start_time.isoformat(), 'end_time': end_time.isoformat(),
                 'path': f'{data_root}/{subdir}'},
                False)


class DispatcherService(ServiceBase):
    """A class that implements the tgfserver application's instrument dispatcher service.

    The instrument dispatcher service acts as a dispatcher for TGF Group instruments.

    Attributes
    ----------
    service_name : str
        The name of the service.
    _config_model : DispatcherModel
        A pydantic model used to validate the service's section of the config file.
    _pool : psycopg_pool.AsyncConnectionPool
        A database connection pool for use in the service's various functions.
    _websockets : weakref.WeakSet
        A weak reference set that keeps track of all currently-open websocket connections.
    _ip_cache : LockoutCache
        A cache that keeps track of authorization attempts for client ip addresses and locks them out after too many
        attempts.
    _ph : argon2.PasswordHasher
        A password hasher used to verify and rehash client passwords
    _scheduler : tgfserver.helpers.transfer_scheduler.TransferScheduler
        A transfer scheduler instance.

    """

    service_name = params.DISPATCHER_NAME
    _config_model = DispatcherModel

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        self._websockets = weakref.WeakSet()
        self._ip_cache = LockoutCache(self._config.ip_cache_size_bytes, self._config.ip_period_sec,
                                      self._config.max_auth_attempts)
        self._ph = argon2.PasswordHasher()
        self._scheduler = TransferScheduler(self._config)

        self._register_resource_manager(self._database_conn_pool)

        self._register_primary_task(self._websocket_listener)

        self._register_periodic_task(self._scheduler.make_schedule, cron=self._config.scheduling_deadline)
        self._register_periodic_task(self._make_check_in_digest, cron=self._config.digest_time)

    def _configure_module_loggers(self) -> None:
        super()._configure_module_loggers()
        logging.getLogger('aiohttp.server').setLevel(self._config.log_level)
        logging.getLogger('aiohttp.web').setLevel(self._config.log_level)
        logging.getLogger('aiohttp.websocket').setLevel(self._config.log_level)
        logging.getLogger('psycopg').setLevel(self._config.log_level)
        logging.getLogger('psycopg.pool').setLevel(self._config.log_level)

    async def _reload_callback(self, diff: Set[str]) -> None:
        restart_required = {'service_host', 'service_port', 'db_pool_size', 'db_host', 'db_port',
                            'db_connect_timeout_sec', 'db_name', 'db_user', 'db_password', 'start_day', 'end_day',
                            'scheduling_deadline', 'digest_time'}
        if len(diff.intersection(restart_required)) > 0:
            self._logger.warning('Service restart required for some updated config options to take effect.')

    def _pool_reconnect_failed_callback(self, pool: psycopg_pool.AsyncConnectionPool) -> None:
        """A callback which is called if the database connection pool can't reacquire connections."""
        self._logger.critical('Lost connection to database.')
        self.shutdown()

    async def _database_conn_pool(self) -> AsyncGenerator[None, Any]:
        """A function responsible for opening, holding, and closing the service's database connection pool."""
        await self._pool.open(wait=True)

        yield

        await self._pool.close()

    async def _instrument_handler(self, request: web.BaseRequest) -> web.WebSocketResponse:
        """A handler for incoming client websocket connections."""
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            client_ip = forwarded_for.split(',')[0].strip()
        else:
            client_ip = request.headers.get('X-Real-IP', request.remote)

        self._logger.info(f'Opening websocket connection from {client_ip}.')
        ws = aiohttp.web.WebSocketResponse(receive_timeout=self._config.receive_timeout_sec,
                                           max_msg_size=self._config.max_msg_size_bytes)
        await ws.prepare(request)
        self._websockets.add(ws)
        session = DispatcherSession(self._config, self._logger, self._ip_cache, self._pool, self._ph, self._scheduler,
                                    client_ip)
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        response, done = await session.get_response(msg.data)
                        if not ws.closed:
                            await ws.send_str(response)

                        if done:
                            break

                    except pydantic.ValidationError:
                        self._logger.info(f'Message from {client_ip} contained improperly formed info.')
                        await ws.close(code=aiohttp.WSCloseCode.UNSUPPORTED_DATA,
                                       message=b'Message contained improperly formed info')
                        break
                    except psycopg.Error:
                        self._logger.exception(f'Encountered a database exception during session with '
                                               f'{client_ip}:')
                        await ws.close(code=aiohttp.WSCloseCode.INTERNAL_ERROR)
                        break
                    except Exception:
                        self._logger.exception(f'Encountered an exception during session with {client_ip}:')
                        await ws. close(code=aiohttp.WSCloseCode.INTERNAL_ERROR)
                        break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self._logger.info(f'Websocket from {client_ip} closed with an exception: {ws.exception()}.')

        except asyncio.exceptions.TimeoutError:
            self._logger.info(f'Websocket connection from {client_ip} timed out.')
        except web.HTTPException:
            self._logger.info(f'Encountered an exception during websocket connection from {client_ip}:',
                              exc_info=True)
        except ConnectionError:
            self._logger.info(f'Websocket connection from {client_ip} closed unexpectedly.')
        finally:
            self._websockets.discard(ws)
            if not ws.closed:
                await ws.close()

            self._logger.info(f'Websocket connection from {client_ip} closed.')
            if ws.close_code > aiohttp.WSCloseCode.OK:
                self._logger.info(f'Websocket connection from {client_ip} closed with an unexpected code: '
                                  f'{ws.close_code}.')

        return ws

    async def _websocket_listener(self) -> None:
        """A function responsible for starting and stopping the service's websocket connection listener."""
        self._logger.info(f'Starting websocket listener on '
                          f'{self._config.service_host}:{self._config.service_port}.')
        runner = None
        try:
            server = aiohttp.web.Server(self._instrument_handler)
            runner = aiohttp.web.ServerRunner(server)
            await runner.setup()
            site = aiohttp.web.TCPSite(runner, self._config.service_host, self._config.service_port)
            await site.start()
            await self._wait_forever()
        finally:
            if runner is not None:
                if len(self._websockets) > 0:
                    self._logger.info('Closing all open websocket connections.')

                for ws in set(self._websockets):
                    await ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, message="Server shutting down")

                self._logger.info('Shutting down websocket listener.')
                await runner.cleanup()

    @classmethod
    def _update_gmail_creds(cls, creds: Credentials) -> None:
        """A function that refreshes the given Google API credentials and stores them in the application config file."""
        creds.refresh(Request())
        config = read_config(update_user_config=False)
        config.set(cls.service_name, 'gmail_api_credentials', creds.to_json())
        write_config(config)

    @staticmethod
    def _send_email(creds: Credentials, message: EmailMessage) -> None:
        """A function that sends the given email message using the given credentials."""
        encoded_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        service = build('gmail', 'v1', credentials=creds)
        service.users().messages().send(userId='me', body=encoded_message).execute()

    async def _make_check_in_digest(self) -> None:
        """A function that makes and sends the service's check in digests."""
        try:
            # Retrieving the check ins from the database
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""SET TIMEZONE TO UTC;""")
                    await cur.execute("""SELECT * FROM tgfserver.get_check_ins();""")
                    check_ins = await cur.fetchall()

        except psycopg.Error:
            self._logger.exception('Check in digest: encountered a database exception when retrieving check in info:')
            return

        # Making digest strings for each instrument and logging the relevant warnings
        message_strings = [f'Instrument dispatcher check in digest '
                           f'(as of {datetime.datetime.now(datetime.UTC).isoformat()}):\n']
        for instrument, timestamp, storage_frac, gps in check_ins:
            await asyncio.sleep(0)  # Giving the event loop a chance to do other things
            seconds_since = (datetime.datetime.now(datetime.UTC) - timestamp).total_seconds()
            days_since = seconds_since / 86400

            # Adding the main lines for each instrument
            message_strings.append(f'{instrument} - last check in at {timestamp.isoformat()} '
                                   f'({"<=1 day" if days_since <= 1 else format(days_since, ".1f") + " days"} ago):\n')
            message_strings.append(f'\tStorage usage: {format(storage_frac * 100, ".2f")}%; GPS status: {gps}\n')

            # Adding a line if the instrument has been out of contact for a while
            if seconds_since >= self._config.stale_check_in_thresh_sec:
                self._logger.warning(f'Check in digest: {instrument} has been out of contact for some time.')
                message_strings.append('\tALERT: OUT OF CONTACT FOR SOME TIME.\n')

            # Adding a line if the instrument's storage usage is high
            if storage_frac >= self._config.storage_warning_thresh:
                self._logger.warning(f'Check in digest: {instrument} storage usage is high.')
                message_strings.append(f'\tALERT: STORAGE USAGE IS HIGH.\n')

        # Getting Gmail API credentials and refreshing them if necessary
        try:
            creds = Credentials.from_authorized_user_info(self._config.gmail_api_credentials, params.GMAIL_SCOPES)
        except Exception:
            self._logger.exception('Check in digest: encountered an exception when getting Gmail API credentials:')
            return

        if not creds.valid:
            if creds.expired and creds.refresh_token:
                try:
                    # Refresh code is blocking, so it needs to be executed in another thread
                    await asyncio.to_thread(self._update_gmail_creds, creds)
                except Exception:
                    self._logger.exception('Check in digest: encountered an exception when refreshing Gmail API '
                                           'credentials:')
                    return

                self._config.gmail_api_credentials = json.loads(creds.to_json())
            else:
                self._logger.warning(f'Gmail API credentials are invalid and cannot be refreshed.')
                return

        # Building the email message
        message = EmailMessage()
        message.set_content(''.join(message_strings))
        message['To'] = self._config.gmail_address
        message['From'] = self._config.gmail_address
        message['Subject'] = f'Instrument Dispatcher Digest {datetime.datetime.now().strftime("%m/%d/%Y")}'

        # Sending the email
        try:
            # Email send code is blocking, so it needs to be executed in another thread
            await asyncio.to_thread(self._send_email, creds, message)
            self._logger.info('Check in digest: successfully sent digest email.')
        except Exception:
            self._logger.exception('Check in digest: encountered an exception when sending digest email:')
