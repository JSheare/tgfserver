"""A module containing the parameters used by various parts of the tgfserver application."""
user_config_path = '~/.config/tgfserver'
config_file = 'tgfserver.ini'
log_path = '~/.local/state/tgfserver/log'
max_log_size_bytes = 10000000
max_log_rollovers = 5
runtime_path = '/run/user/<uid>/tgfserver'
pid_file = 'tgfserver.pid'
data_path = '~/.local/share/tgfserver'
gmail_scopes = ['https://www.googleapis.com/auth/gmail.compose']
manager_name = 'manager_service'
dispatcher_name = 'dispatcher_service'
