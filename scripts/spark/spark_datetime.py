import datetime as dt
import pyspark.sql.functions as F
from pyspark.sql.functions import udf

udf_date_timedelta = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(int(float(x)))).isoformat() if x else None)


'''udf to create datetime column from original timestamp column 
    Parameters:
            value (int): timestamp
    Returns
            datetime (object): datetime created from timestamp
'''
@udf
def get_datetime(value):
    return dt.datetime.fromtimestamp(value)

'''udf to extract year from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (int): year from datetime
'''
@udf
def datetimeToYear(value):
    return value.year if value is not None else None

'''udf to extract hour from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): year from datetime
'''
@udf
def datetimeToHour(value):
    return value.hour if value is not None else None

'''udf to extract week of the year from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (int): week of the year from datetime
'''
@udf
def datetimeToWeek(value):
    return value.strftime("%W") if value is not None else None

'''udf to extract day of the month from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): week of the year from datetime
'''
@udf
def datetimeToDay(value):
    return value.strftime("%d") if value is not None else None

'''udf to extract month from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): month from datetime
'''
@udf
def datetimeToMonth(value):
    return value.strftime("%B") if value is not None else None

'''udf to extract weekday from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): week day from datetime
'''
@udf
def datetimeToWeekDay(value):
    return value.strftime("%A") if value is not None else None

'''udf to convert a string to datetime
 in the format yyyy-mm-dd
    Parameters:
            value (string): string in the format
    Returns
            value (datetime): datetime object
'''
def stringToDatetimeYYYYMMDD(string_value):
    return dt.datetime.strptime(string_value, '%Y-%m-%d')

'''udf to extract the year in the 
short format from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): year with the last two digits
'''
def datetimeToYearShort(value):
    return value.strftime("%y") if value is not None else None

'''udf to extract the mont in the 
short format from a datetime 
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): month in the three digits (Ian, Apr)
'''
def datetimeToMonthShort(value):
    return value.strftime("%b") if value is not None else None

'''udf to extract the day of the month
    Parameters:
            value (datetime): datetime object
    Returns
            value (string): the day of the month
'''
def datetimeToDayClasic(value):
    return value.strftime("%d") if value is not None else None