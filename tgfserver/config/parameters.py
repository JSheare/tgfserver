"""A module containing the parameters used by various parts of the tgfserver application."""
USER_CONFIG_PATH = '~/.config/tgfserver'
CONFIG_FILE = 'tgfserver.ini'
LOG_PATH = '~/.local/state/tgfserver/log'
MAX_LOG_SIZE_BYTES = 10000000
MAX_LOG_ROLLOVERS = 5
RUNTIME_PATH = '/run/user/<uid>/tgfserver'
PID_FILE = 'tgfserver.pid'
DATA_PATH = '~/.local/share/tgfserver'
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.compose']
MANAGER_NAME = 'manager_service'
DISPATCHER_NAME = 'dispatcher_service'
API_NAME = 'api_service'