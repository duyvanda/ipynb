import dateutil.parser
import math
import re
import datetime
import time
import os
from datetime import timedelta
from dateutil import tz
from collector.libs.constant import *


class DateTimeUtil(object):

    DATE_FORMAT = "%Y-%m-%d"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def who_am_i():
        print("I'am batman")

    @staticmethod
    def get_current_day():
        return datetime.datetime.now().strftime(Constant.DEFAULT_DAY_FORMAT)

    @staticmethod
    def get_yesterday():
        yesterday = datetime.datetime.now() - timedelta(days=1)
        return yesterday.strftime(Constant.DEFAULT_DAY_FORMAT)

    @staticmethod
    def get_current_date_time():
        return datetime.datetime.now().strftime(Constant.DEFAULT_DATE_TIME_FORMAT)

    @staticmethod
    def get_current_hour():
        return datetime.datetime.now().strftime("%H")

    @staticmethod
    def get_current_microseconds():
        return int(time.time() * 1000000)

    @staticmethod
    def get_current_milliseconds():
        return int(time.time() * 1000)

    @staticmethod
    def convert_date(date_time, new_format):
        return datetime.datetime.strptime(
            date_time, Constant.DEFAULT_DAY_FORMAT
        ).strftime(new_format)

    @staticmethod
    def date_increment(date_time, number_of_day):
        if number_of_day == 0:
            return date_time
        t = datetime.datetime.strptime(
            date_time, Constant.DEFAULT_DAY_FORMAT
        ) + datetime.timedelta(days=number_of_day)
        return t.strftime(Constant.DEFAULT_DAY_FORMAT)

    @staticmethod
    def get_day_arr(from_date, to_date, timing="1"):
        day_list = []
        if DateTimeUtil.datetime_to_timestamp(
            from_date, DateTimeUtil.DATE_FORMAT
        ) >= DateTimeUtil.datetime_to_timestamp(to_date, DateTimeUtil.DATE_FORMAT):
            return day_list

        if timing == Constant.Timing.T1:
            loop_day = from_date
            while loop_day != to_date:
                day_list.append(loop_day)
                loop_day = DateTimeUtil.date_increment(loop_day, 1)
            day_list.append(to_date)
        elif timing == Constant.Timing.WEEKLY:
            from_w = DateTimeUtil.get_end_of_week(from_date)
            to_w = DateTimeUtil.get_end_of_week(to_date)
            while from_w != to_w:
                day_list.append(from_w)
                from_w = DateTimeUtil.date_increment(from_w, 7)
            day_list.append(to_w)
        elif timing == Constant.Timing.MONTHLY:
            from_m = DateTimeUtil.get_end_of_month(from_date)
            to_m = DateTimeUtil.get_end_of_month(to_date)
            while from_m != to_m:
                day_list.append(from_m)
                t1 = DateTimeUtil.date_increment(from_m, 1)
                from_m = DateTimeUtil.get_end_of_month(t1)
            day_list.append(to_m)
        return day_list

    @staticmethod
    def get_list_date_from_timing(log_date, timing):
        day_list = []

        if timing == Constant.Timing.WEEKLY:
            start_day = DateTimeUtil.get_start_of_week(log_date)
            loop_day = log_date
            while loop_day != start_day:
                day_list.append(loop_day)
                loop_day = DateTimeUtil.date_increment(loop_day, -1)
            day_list.append(loop_day)
        elif timing == Constant.Timing.MONTHLY:
            start_day = DateTimeUtil.get_start_of_month(log_date)
            loop_day = log_date
            while loop_day != start_day:
                day_list.append(loop_day)
                loop_day = DateTimeUtil.date_increment(loop_day, -1)
            day_list.append(loop_day)
        else:
            day_list.append(log_date)
            for i in range(1, int(timing)):
                day_append = DateTimeUtil.date_increment(log_date, -i)
                day_list.append(day_append)

        return day_list

    @staticmethod
    def get_start_of_week(log_date):
        dt = datetime.datetime.strptime(log_date, Constant.DEFAULT_DAY_FORMAT)
        start_of_week = dt - timedelta(days=dt.weekday())
        return start_of_week.strftime(Constant.DEFAULT_DAY_FORMAT)

    @staticmethod
    def get_start_of_month(log_date):
        dt = datetime.datetime.strptime(log_date, Constant.DEFAULT_DAY_FORMAT)
        return dt.strftime("%Y-%m-01")

    @staticmethod
    def get_end_of_week(log_date):
        dt = datetime.datetime.strptime(log_date, Constant.DEFAULT_DAY_FORMAT)
        start_of_week = dt - timedelta(days=dt.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        return end_of_week.strftime(Constant.DEFAULT_DAY_FORMAT)

    @staticmethod
    def get_end_of_month(log_date):
        dt = datetime.datetime.strptime(log_date, Constant.DEFAULT_DAY_FORMAT)
        start_of_month = dt.strftime("%Y-%m-01")
        some_day_next_month = DateTimeUtil.date_increment(start_of_month, 32)
        start_of_next_month = DateTimeUtil.get_start_of_month(some_day_next_month)
        end_of_month = DateTimeUtil.date_increment(start_of_next_month, -1)
        return end_of_month

    @staticmethod
    def get_random_id_by_time():
        millis = int(round(time.time() * 1000000))
        return millis

    @staticmethod
    def timestamp_to_datetime(timestamp, datetime_format):
        return datetime.datetime.fromtimestamp(int(timestamp)).strftime(datetime_format)

    @staticmethod
    def datetime_to_timestamp(date_time, datetime_format=DATETIME_FORMAT):
        return int(
            time.mktime(
                datetime.datetime.strptime(date_time, datetime_format).timetuple()
            )
        )


def parse_time(_time, fmt=None, use_utils=True, accept_null=True):
    if _time is None:
        if accept_null:
            return None
        else:
            raise ValueError(
                "time data '{0}' does not match format '{1}'".format(_time, fmt)
            )
    if fmt:
        try:
            return datetime.datetime.strptime(_time, fmt)
        except Exception as _:
            return parse_time(_time, None, use_utils, accept_null)
    elif use_utils:
        try:
            return dateutil.parser.parse(_time)
        except ValueError as _:
            if accept_null:
                return None
            else:
                raise ValueError(
                    "time data '{0}' does not match format '{1}'".format(_time, fmt)
                )
    else:
        if accept_null:
            return None
        else:
            raise ValueError(
                "time data '{0}' does not match format '{1}'".format(_time, fmt)
            )


def parse_timestamp(_time, accept_null=True):
    if isinstance(_time, int) or isinstance(_time, float):
        if _time > 10e10:
            return datetime.datetime.fromtimestamp(_time / 1e3)
        else:
            return datetime.datetime.fromtimestamp(_time)
    elif isinstance(_time, str):
        if _time.isdigit():
            return parse_timestamp(int(_time))
        _pattern = re.compile(r"Timestamp\((\d+), \d+\)")
        s = _pattern.search(_time)
        if s:
            return parse_timestamp(int(s.group(1)))

    if accept_null:
        return None
    else:
        raise ValueError("timestamp '{0}' can't parse".format(_time))


def parse_time_timezone(s):
    to_zone = tz.tzlocal()
    return parse_time(s).astimezone(to_zone).replace(tzinfo=None)


def convert_datetime_timestamp(_time):
    return time.mktime(_time.timetuple())


def generate_block_datetime(_datetime, interval_time):
    assert isinstance(_datetime, datetime.datetime)
    # fix local timestamp
    _timestamp = time.mktime((_datetime + datetime.timedelta(hours=7)).timetuple())

    beg_datetime = datetime.datetime.utcfromtimestamp(
        math.floor(_timestamp / interval_time) * interval_time
    )
    end_datetime = datetime.datetime.utcfromtimestamp(
        math.floor(_timestamp / interval_time) * interval_time + interval_time
    )

    return beg_datetime, end_datetime


def generate_file_name_datetime(
    topic,
    _datetime,
    interval_seconds,
    fmt_datetime="%Y-%m-%d/%Y%m%d_%H%M%S",
    extension=".json",
):
    _times = generate_block_datetime(_datetime, interval_seconds)
    # file_name = "_".join(map(lambda x: x.strftime(fmt_datetime), _times))
    file_name = _times[0].strftime(fmt_datetime) + extension
    return "/".join(map(str, [topic, file_name]))


def datetime_to_utc_datetime(local_time):
    utc_zone = tz.tzutc()
    local_zone = tz.tzlocal()
    # datetime objects are 'naive' by default
    local_time = local_time.replace(tzinfo=local_zone)
    # Convert time to UTC
    utc_time = local_time.astimezone(utc_zone)
    return utc_time


def get_create_modify_time_file(in_path):
    return (
        datetime.datetime.fromtimestamp(os.path.getmtime(in_path)),
        datetime.datetime.fromtimestamp(os.path.getctime(in_path)),
    )
