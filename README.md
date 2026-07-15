# Tgfserver
## A Linux backend application for the operations of the UCSC TGF group.
### To install the application, run the following pip command:

    pip install tgfserver@git+https://github.com/JSheare/tgfserver

A few notes:

The application is backed by a PostgreSQL database. Therefore, before running the application, please ensure that 
Postgres is installed, that a database and the relevant roles have been set up, and that the migrations found in the 
following GitHub repository have been applied: https://github.com/JSheare/tgf_database.

The application also makes use of Playwright. Once the application is installed, please ensure that you are in the 
install environment and run the following command to finish installing and setting up Playwright's browser bundles:

    playwright install.

After installation, it's also recommended that the following commands be executed before starting the application:
- This command will make the user config file and help in filling it out:

        tgfserver --setup

- And this command will validate the config file and report any issues. Run it after filling the file out:

        tgfserver --test_config

For a full list of application commands, use the help flag:

    tgfserver -h

And finally, to start the application, use the following command:

    tgfserver

## Config File Options:
The user config file can be found at ~/.config/tgfserver/tgfserver.ini

### Manager service options:
- log_level: the log level of the application's manager service.
- start_limit_interval_sec: services that are crashed or terminated more than start_limit_burst times within this
  interval (in seconds) will not be restarted by the manager service again.
- start_limit_burst: the number of times (within start_limit_interval_sec) that the manager service will attempt to 
  restart a crashed or terminated service before giving up.
- restart_sec: the number of seconds that the manager service will wait before attempting to restart a crashed or
  terminated service.
- shutdown_timeout_sec: the number of seconds that the manager service will wait for services to stop during application 
  shutdown. Services that don't stop in time will be killed.
- db_host: the IPv4 address of the database.
- db_port: the port used by the database.
- db_connect_timeout_sec: the amount of time (in seconds) to wait for database connections to be established.
- db_name: the name of the backend database.
- db_user: the name of the database role used by the manager service.
- db_password: the password of the database role used by the manager service.
- spreadsheet_id: the ID of the Google Sheet that the database will be partially updated from.
- sheets_api_key: the API key used to access the Google Sheets API.
- db_update_time: the time when the database will be updated. Should be a crontab-style string.
- scrape_weather: a true or false value that toggles weather scraping during database updates.
- scrape_timeout_sec: the amount of time (in seconds) to wait for weather data from the internet to load before giving
  up.

### Instrument Dispatcher service options:
- log_level: the log level of the application's instrument dispatcher service.
- service_host: the IPv4 address that the dispatcher service will bind to.
- service_port: the TCP port that the dispatcher service will listen for websocket connections on.
- receive_timeout_sec: the amount of time (in seconds) that the dispatcher service will wait for a websocket message
  before closing the connection.
- max_msg_size_bytes: the maximum size of a websocket message (in bytes).
- ip_cache_size_bytes: the maximum size of the cache used to keep track of authentication requests from specific ip
  addresses (in bytes).
- ip_period_sec: the length of the authorization attempt window/lockout time (in seconds).
- max_auth_attempts: the maximum number of authorization attempts that can be made by a specific ip address within
  ip_period_sec seconds before the ip address is locked out for ip_period_sec seconds.
- db_pool_size: the number of connections in the dispatcher's database connection pool.
- db_host: the IPv4 address of the database.
- db_port: the port used by the database.
- db_connect_timeout_sec: the amount of time (in seconds) to wait for database connections to be established.
- db_name: the name of the backend database.
- db_user: the name of the database role used by the dispatcher service.
- db_password: the password of the database role used by the dispatcher service.
- start_day: the starting day of the week for data file transfer scheduling. Should be a number 1-7.
- end_day: the ending day of the week for data file transfer scheduling. Should be a number 1-7.
- start_time_sec: the starting time of day (in seconds) for data file transfer scheduling on each valid day of the week.
- end_time_sec: the ending time of day (in seconds) for data file transfer scheduling on each valid day of the week.
- max_slot_size_sec: the longest amount of time (in seconds) that can be booked for a data file transfer per instrument.
- gap_size_sec: the amount of time (in seconds) to reserve between booked data file transfer times.
- stale_stats_thresh_sec: the age (in seconds) at or above which a measured transfer rate is considered to be too old.
- scheduling_deadline: the time at which the data file transfer schedule is made up. Should be a crontab-style string.
- stale_check_in_thresh_sec: the amount of time (in seconds) at or above which an instrument check in is considered
  stale. Instruments whose last check in was at or above this threshold have been out of contact for a while.
- storage_warning_thresh: the instrument computer storage usage fraction at or above which usage is considered high.
- gmail_address: the Gmail address used to send instrument check in digests.
- gmail_api_credentials: the Gmail API credentials used to send emails.
- digest_time: the time at which instrument check in digests will be generated. Should be a crontab-style string.

### API service options:
- log_level: the log level of the application's API service.
- service_host: the IPv4 address that the API service will bind to.
- service_port: the TCP port that the API service will listen for requests on.
- db_pool_size: the number of connections in the API's database connection pool.
- db_host: the IPv4 address of the database.
- db_port: the port used by the database.
- db_connect_timeout_sec: the amount of time (in seconds) to wait for database connections to be established.
- db_name: the name of the backend database.
- db_user: the name of the database role used by the API service.
- db_password: the password of the database role used by the API service.