import datetime as dt
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

udf_date_timedelta = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(int(float(x)))).isoformat() if x else None)

# create datetime column from original timestamp column
@udf
def get_datetime(value):
    return dt.datetime.fromtimestamp(value)

# extract columns to create time table
@udf
def datetimeToYear(value):
    return value.year if value is not None else None

@udf
def datetimeToHour(value):
    return value.hour if value is not None else None

# weekofyear
@udf
def datetimeToWeek(value):
    return value.strftime("%W") if value is not None else None

# dayofmonth
@udf
def datetimeToDay(value):
    return value.strftime("%d") if value is not None else None

@udf
def datetimeToMonth(value):
    return value.strftime("%B") if value is not None else None

@udf
def datetimeToWeekDay(value):
    return value.strftime("%A") if value is not None else None