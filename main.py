import os
import logging
import pandas as pd
import sqlalchemy

from datetime import datetime

# Create a logger object
logger = logging.getLogger(__name__)
# Set the logging level
logger.setLevel(logging.DEBUG)
# Create a stream handler for the logger
stream_handler = logging.StreamHandler()
# Create a formatter for the stream handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Add the formatter to the file handler
stream_handler.setFormatter(formatter)
# Add the file handler to the logger
logger.addHandler(stream_handler)

# connect to db
database_username = os.environ.get("DBUSER")
database_password = os.environ.get("DBPASSWD")
database_hostname = os.environ.get("DBHOST")
database_name = os.environ.get("DBNAME")
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_hostname, database_name))

# load timesheet table into a pandas dataframe
sql = "SELECT ts FROM timesheet ORDER by ts ASC"
df = pd.read_sql(sql=sql, con=database_connection)

# create a backup of the imported data as csv
now = datetime.now()
filename = now.strftime('backups/backup_%Y-%m-%d_%H-%M-%S.csv')
with open(filename, 'w') as f:
    f.write(df.to_csv(lineterminator='\n'))

# sort the values chronologically into a pandas series
df1 = df["ts"].apply(pd.Timestamp)
s = df1.sort_values(ascending=True)
s.reset_index(drop=True, inplace=True)

# initialize variables for clock-in/out
clock_in = None
clock_out = None

# initialize variables for capturing the calculated timeintervals
time_intervals = pd.Series(dtype='timedelta64[ns]')
time_intervals_week = pd.Series(dtype='timedelta64[ns]')
time_intervals_month = pd.Series(dtype='timedelta64[ns]')

# initialize variables for storing time anchors for comparison
last_processed_date = None
current_date = None
last_processed_calender_week = None
last_processed_month = None

# define the output dataframe
data = {
    'cw': pd.Series(dtype='int'),
    'clock_in': [],
    'clock_out': [],
    'daily_balance': [],
    'weekly_balance': [],
    'monthly_balance': []
}
df_output = pd.DataFrame(data)

# if the time intervals are incomplete, add a timestamp
if len(s) % 2 == 1:
    clock_in = s.iloc[-1]
    clock_out = pd.Timestamp(year=clock_in.year,
                             month=clock_in.month,
                             day=clock_in.day,
                             hour=23,
                             minute=59,
                             second=59
                             )
    s.loc[len(s) + 1] = clock_out

# iterate over all timestamps in the series
it = iter(s)
for x in it:
    # read the current and next timestamp
    try:
        clock_in, clock_out = x, next(it)
    except StopIteration:
        break
    current_month = clock_in.month
    if not current_month == last_processed_month:
        last_processed_month = current_month
        time_intervals_month = pd.Series(dtype='timedelta64[ns]')
    current_calender_week = clock_in.week
    if not current_calender_week == last_processed_calender_week:
        last_processed_calender_week = current_calender_week
        time_intervals_week = pd.Series(dtype='timedelta64[ns]')
    current_date = clock_in.date()
    if not current_date == last_processed_date:
        last_processed_date = current_date
        total_time_current_day = pd._libs.Timedelta(0)
        time_intervals = pd.Series(dtype='timedelta64[ns]')
    new_time_interval = clock_out - clock_in
    time_intervals = pd.concat([time_intervals, pd.Series([new_time_interval])], ignore_index=True)
    time_intervals_week = pd.concat([time_intervals_week, pd.Series([new_time_interval])], ignore_index=True)
    time_intervals_month = pd.concat([time_intervals_month, pd.Series([new_time_interval])], ignore_index=True)
    logger.debug(f"Clock-In: {clock_in} - Clock-Out: {clock_out}")
    try:
        total_time_current_day = time_intervals.sum()
        total_time_current_week = time_intervals_week.sum()
        total_time_current_month = time_intervals_month.sum()
        new_row = pd.DataFrame({
            'cw': [last_processed_calender_week],
            'clock_in': [clock_in],
            'clock_out': [clock_out],
            'daily_balance': [total_time_current_day],
            'weekly_balance': [total_time_current_week],
            'monthly_balance': [total_time_current_month]
        })
        df_output = pd.concat([new_row, df_output], ignore_index=True)
        logger.debug(f"Daily balance: {total_time_current_day}")
        logger.debug(f"Weekly balance: {total_time_current_week}")
        logger.debug(f"Monthly balance: {total_time_current_month}")
    except AttributeError:
        pass
    except KeyError:
        pass

    daily_sum = None

print(df_output.to_string())
filename = "report.csv"
with open(filename, 'w') as f:
    f.write(df_output.to_csv(lineterminator='\n'))

# Group the DataFrame by day and calculate the maximum value of the time interval column for each day
max_time_interval_per_day = df_output.groupby(df_output['clock_in'].dt.date)['daily_balance'].max()

# Write the maximum time intervals as a separate report
filename = "max_time_interval_per_day.csv"
with open(filename, 'w') as f:
    f.write(max_time_interval_per_day.to_csv(lineterminator='\n'))

# Calculate the mean value of the maximum time interval values
current_utc_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z'
mean_hours = round(max_time_interval_per_day.mean().total_seconds() / 3600, 2)

# Write the mean hours a day as a separate report
print(f"Mean hours per day: {mean_hours}")
filename = "mean_hours_per_day.csv"
with open(filename, 'a') as f:
    f.write(f"{current_utc_timestamp};{str(mean_hours)}")
