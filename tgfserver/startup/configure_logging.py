"""A module containing a function that performs logging setup for a service."""
import logging
import logging.handlers
import queue

import tgfserver.config.parameters as params
from tgfserver.helpers.helper_funcs import expand_path


def configure_logging(service_name: str, log_level: int) -> logging.handlers.QueueListener:
    """A function that performs logging setup for a service and returns a log listener.

    Parameters
    ----------
    service_name : str
        The name of the service to do logging for.
    log_level : int
        The level to set logging at as an integer.

    Returns
    -------
    logging.handlers.QueueListener
        A listener that waits for log messages via a queue and writes them in another thread. Using this listener
        for logging is vital to prevent logging calls from blocking the asyncio event loop via filesystem I/O lag.

    """

    logging.captureWarnings(True)
    # Configuring a handler on the root logger so that everything goes to the same log file.
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    log_queue = queue.Queue()
    handler = logging.handlers.QueueHandler(log_queue)
    handler.setFormatter(logging.Formatter("{asctime} - {levelname} - {name} - {message}",
                                           style="{",
                                           datefmt="%Y-%m-%d %H:%M:%S"))
    root_logger.addHandler(handler)
    # Writes log entries in a different thread, to prevent filesystem I/O lag from blocking the event loop
    log_listener = logging.handlers.QueueListener(log_queue, logging.handlers.RotatingFileHandler(
        filename=f'{expand_path(params.LOG_PATH)}/{service_name}.txt',
        encoding='utf-8',
        maxBytes=params.MAX_LOG_SIZE_BYTES,
        backupCount=params.MAX_LOG_ROLLOVERS))
    return log_listener
