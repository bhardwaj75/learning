#!/usr/bin/env python
__author__ = "Diwakar Bhardwaj"
__copyright__ = "2019 Diwakar Bhardwaj. All rights reserved"
__version__ = "1.0.0"
__maintainer__ = "Diwakar Bhardwaj"
__email__ = "diwakar.bhardwaj@datami.com"
import datetime
import json
import logging
from csv import reader
from datetime import datetime, timedelta

import pandas as pd
from dateutil.parser import parse

get_desire_format_date = lambda s_date, format: parse(s_date).strftime(format)
format = "%d/%m/%Y"
rdate_format = "%m/%d/%Y"

# Reading Config Details
with open('epg.conf') as f:
    epg_config = json.load(f)


def valid_date(datestring):
    try:
        datetime.strptime(datestring, format)
        return True
    except ValueError:
        return False


def modify_duration_format(time):
    if time.count(':') + 1 >= 3:
        hrs, min, sec, msec = time.split(':')
        return hrs + ":" + min
    if time.count(':') + 1 == 2:
        hrs, min, sec = time.split(':')
        return hrs + ":" + min

    if 'min' not in time:
        return time
    hrs = time.split(" ")[0]
    min = time.split(" ")[2]
    # print(hrs+":"+min)
    return hrs + ":" + min


extra_day_to_add = 0


def get_end_time(current_time, duration):
    if len(duration.split(':')) == 2:
        duration += ":00"
    if len(current_time.split(':')) == 2:
        current_time += ":00"
    parsed_time = datetime.strptime(current_time, "%H:%M:%S")
    hours, rest = duration.split(':', 1)

    time = timedelta(hours=int(hours)) + datetime.strptime(rest, "%M:%S")
    hour_str = time.hour
    minute_str = time.minute
    second_str = time.second

    global extra_day_to_add
    extra_day_to_add = int(int(hours) / 24)

    str_time = str(hour_str) + ":" + str(minute_str) + ":" + str(second_str)
    parsed_duration = datetime.strptime(str_time, "%H:%M:%S")
    then = parsed_time + timedelta(hours=parsed_duration.hour,
                                   minutes=parsed_duration.minute,
                                   seconds=parsed_duration.second)

    result = then.strftime("%H:%M:%S")
    return result


def get_end_time2(date, start_time, end_time, flag):
    if len(start_time.split(':')) == 2:
        start_time += ":00"
    # print("_func_", date, start_time, end_time)
    start_date = datetime.strptime(date + ' ' + start_time, "%d/%m/%Y %H:%M:%S")
    end_date = datetime.strptime(date + ' ' + end_time, "%d/%m/%Y %H:%M:%S")

    if end_date.timestamp() < start_date.timestamp():
        next_date = end_date + timedelta(days=1)
        return next_date.strftime("%d/%m/%Y")
    elif extra_day_to_add and flag:
        next_date = end_date + timedelta(days=extra_day_to_add)
        return next_date.strftime("%d/%m/%Y")
    return start_date.strftime("%d/%m/%Y")


def convert_excel_file_to_csv(file):
    df = pd.read_excel(file)
    csv_file_name = file.split(".")[0]
    df.to_csv(csv_file_name + ".csv", encoding='utf-8', index=False, sep=',')
    print("converted ", csv_file_name + ".csv")


def make_correct_format(s_date, s_date_wt_mod):
    sday, smonth, syear = s_date.split("/")
    bfsday, bfsmonth, bfsyear = s_date_wt_mod.split("/")
    if sday == bfsmonth and bfsday == smonth:
        s_date = get_desire_format_date(s_date_wt_mod, rdate_format)
    return s_date


# correct csv file if end date is not correct
def convert_epg_data_to_desire_format_type_3(file):
    out_put_file_prefix = file.rsplit(".", 1)[0]
    date = []
    start_time = []
    synopsis = []
    program_name = []
    priority_header = []
    end_date = []
    end_time = []
    language = []
    out_put_file = open(out_put_file_prefix + '_out.csv', 'w')

    print("file ",out_put_file_prefix,"is being processed")

    with open(file, "r", encoding='latin1') as f:

        is_read_header = 0

        is_unness_line_skipped2 = 0
        row_reading = 0
        is_language = 0
        for line in reader(f):

            if not is_read_header:
                header_name = file.rsplit('.', 1)[0] + "_" + "headers"
                header_pointer = epg_config['epg_config_details']['type3']['header'][header_name]
                if not is_unness_line_skipped2:
                    if header_pointer['pname'] not in line:
                        row_reading += 1
                        continue
                    else:
                        is_unness_line_skipped2 = 1

                header = line
                index_date = header.index(header_pointer['Date'])
                index_start_time = header.index(header_pointer['stime'])
                index_synopsis = header.index(header_pointer['synopsis'])
                index_program_name = header.index(header_pointer['pname'])
                index_end_time = header.index(header_pointer['endtime'])

                if 'Language' in header_pointer:
                    index_language = header.index(header_pointer['Language'])
                    is_language = 1
                priority_header.append(index_date)
                priority_header.append(index_start_time)
                priority_header.append(index_synopsis)
                priority_header.append(index_program_name)
                priority_header.append(index_end_time)
                if 'Language' in header:
                    priority_header.append(index_language)
                    is_language = 1
                is_read_header = True
            else:
                # print(line)
                header = line
                date.append(header[index_date])
                start_time.append(header[index_start_time])
                synopsis.append(header[index_synopsis])
                program_name.append(header[index_program_name])
                end_time.append(header[index_end_time])
                if is_language:
                    language.append(header[index_language])

    for i in range(0, len(end_time)):

        if len(start_time[i].split(':')) <= 2:
            start_time[i] = start_time[i] + ":00"
        if len(end_time[i].split(':')) <= 2:
            end_time[i] = end_time[i] + ":00"
        #  print( program_name[i],date[i], start_time[i] + ":00", end_time[i] + ":00", 1)
        end_date.append(get_end_time2(date[i], start_time[i], end_time[i], 1))
    header_name = 'PROGRAM NAME,START DATE,START TIME,END DATE,END TIME,LANGUAGE,SYNOPSIS'
    out_put_file.write(header_name + '\n')

    for i in range(0, len(end_time)):
        data = ""

        if len(start_time[i]) != 5:
            start_time[i] = '0' + start_time[i]
        if len(end_time[i]) != 5:
            end_time[i] = '0' + end_time[i]

        date[i] = datetime.strptime(date[i], format)
        end_date[i] = datetime.strptime(end_date[i], format).strftime(format)

        if len(start_time[i].split(':')[0]) == 3:
            start_time[i] = start_time[i][1:]

        if len(end_time[i].split(':')[0]) == 3:
            end_time[i] = end_time[i][1:]

        if len(start_time[i].split(':')) >= 3:
            start_time[i] = start_time[i][:-3]
        if len(end_time[i].split(':')) >= 3:
            end_time[i] = end_time[i][:-3]

        if not is_language:
            data += program_name[i] + ',' + date[i] + ',' + start_time[i] + ',' + end_date[
                i] + ',' + end_time[i] + ',' + "English" + ',' + "\"" + synopsis[i].replace('"', '\'') + "\""
        else:
            data += program_name[i] + ',' + date[i].strftime(format) + ',' + start_time[i] + ',' + end_date[
                i] + ',' + end_time[i] + ',' + language[i] + ',' + "\"" + synopsis[i].replace('"', '\'') + "\""
        out_put_file.write(data + '\n')
    print("%s file has been generated" % (out_put_file_prefix + '_out.csv'))


def convert_epg_data_to_desire_format_type_2(file):
    print(file)
    out_put_file_prefix = file.split(".")[0]
    date = []
    start_time = []
    synopsis = []
    program_name = []
    priority_header = []
    end_date = []
    end_time = []
    program_duration = []
    language = []
    out_put_file = open(out_put_file_prefix + '_out.csv', 'w')
    with open(file, "r") as f:
        is_read_header = 0
        index = -1
        is_unness_line_skipped2 = 0
        row_reading = 0
        is_language = 0

        for line in reader(f):

            if line[0] == '':
                continue
            if not is_read_header:
                header_name = file.split('.')[0] + "_" + "headers"
                try:
                    header_pointer = epg_config['epg_config_details']['type2']['header'][header_name]
                except:
                    logging.debug("Headers are not set properly please check epg.conf file")
                    exit(0)

                if not is_unness_line_skipped2:
                    if header_pointer['pname'] not in line:
                        row_reading += 1
                        continue
                    else:
                        is_unness_line_skipped2 = 1
                header = line
                header = [x.rstrip() for x in header]
                index_date = header.index(header_pointer['Date'])
                index_start_time = header.index(header_pointer['stime'])
                index_synopsis = header.index(header_pointer['synopsis'])
                index_program_name = header.index(header_pointer['pname'])
                index_program_duration = header.index(header_pointer['pduration'])

                if 'Language' in header_pointer:
                    index_language = header.index(header_pointer['Language'])
                    is_language = 1
                priority_header.append(index_date)
                priority_header.append(index_start_time)
                priority_header.append(index_synopsis)
                priority_header.append(index_program_name)
                priority_header.append(index_program_duration)
                if 'Language' in header:
                    priority_header.append(index_language)
                    is_language = 1
                is_read_header = True

            else:
                header = line

                is_modify_date = False
                if len(header[index_date].split('/')) == 3:
                    is_modify_date = True
                s_date = get_desire_format_date(header[index_date], format)

                if is_modify_date:
                    s_date = make_correct_format(s_date, header[index_date])
                date.append(s_date)
                if len(header[index_start_time].split(':')) == 3:
                    a, b, c = header[index_start_time].split(':')
                    header[index_start_time] = a + ':' + b
                start_time.append(header[index_start_time])
                synopsis.append(header[index_synopsis])
                program_name.append(header[index_program_name])
                program_duration.append(modify_duration_format(header[index_program_duration]))
                if is_language:
                    language.append(header[index_language])

    for i in range(0, len(start_time)):
        end_time.append(get_end_time(start_time[i], program_duration[i]))
    for i in range(0, len(end_time)):
        end_date.append(get_end_time2(date[i], start_time[i], end_time[i], 1))
    header_name = 'PROGRAM NAME,START DATE,START TIME,END DATE,END TIME,LANGUAGE,SYNOPSIS'
    out_put_file.write(header_name + '\n')

    for i in range(0, len(end_time)):
        data = ""
        if not is_language:
            data += '"' + program_name[i] + '"' + ',' + date[i] + ',' + start_time[i] + ',' + end_date[
                i] + ',' + end_time[i][:-3] + ',' + "English" + ',' + "\"" + synopsis[i].replace('"', '\'') + "\""
        else:
            data += '"' + program_name[i] + '"' + ',' + date[i] + ',' + start_time[i] + ',' + end_date[
                i] + ',' + end_time[i][:-3] + ',' + language[i] + ',' + "\"" + synopsis[i].replace('"', '\'') + "\""

        out_put_file.write(data + '\n')
    print("%s file has been generated" % (out_put_file_prefix + '_out.csv'))


def convert_epg_data_to_desire_format_type_1(file):
    out_put_file_prefix = file.split(".")[0]
    date = []
    start_time = []
    synopsis = []
    program_name = []
    priority_header = []
    end_date = []
    end_time = []
    out_put_file = open(out_put_file_prefix + '_out.csv', 'w')

    with open(file, "r") as f:
        is_read_header = 0
        is_unness_line_skipped = 0;
        index = -1
        for line in reader(f):
            if line[0] == '':
                continue
            if not is_read_header:

                header = line
                header = [x.rstrip() for x in header]
                header_name = file.split('.')[0] + "_" + "headers"
                header_pointer = epg_config['epg_config_details']['type1']['header'][header_name]

                if not is_unness_line_skipped:
                    if header_pointer['pname'] not in line:
                        continue
                    is_unness_line_skipped = 1

                index_date = header.index(header_pointer['Date'])
                index_start_time = header.index(header_pointer['stime'])
                index_synopsis = header.index(header_pointer['synopsis'])
                index_program_name = header.index(header_pointer['pname'])
                priority_header.append(index_date)
                priority_header.append(index_start_time)
                priority_header.append(index_synopsis)
                priority_header.append(index_program_name)
                is_read_header = True

            else:
                header = line

                is_modify_date = False
                if len(header[index_date].split('/')) == 3:
                    is_modify_date = True

                s_date = get_desire_format_date(header[index_date], format)

                if is_modify_date:
                    s_date = make_correct_format(s_date, header[index_date])
                date.append(s_date)
                start_time.append(header[index_start_time])
                synopsis.append(header[index_synopsis])
                program_name.append(header[index_program_name])
        start_time.append('00:00:00')

    # Logic has to be added if endDate is not just a next day. (If program start at 10:00 pm no day 3rd and end 00:00 on 5th
    for i in range(1, len(start_time)):
        end_time.append(start_time[i])
    for i in range(0, len(end_time)):
        end_date.append(get_end_time2(date[i], start_time[i], end_time[i], 0))
    header_name = 'PROGRAM NAME,START DATE,START TIME,END DATE,END TIME,LANGUAGE,SYNOPSIS'
    out_put_file.write(header_name + '\n')
    print(header_name)

    for i in range(0, len(end_time)):
        data = ""
        data += program_name[i] + ',' + date[i] + ',' + start_time[i][:-3] + ',' + end_date[
            i] + ',' + end_time[i][:-3] + ',' + "English," + '"' + synopsis[i] + '"'
        out_put_file.write(data + '\n')
    print("%s file has been generated" % (out_put_file_prefix + '_out.csv'))


if __name__ == '__main__':

    type_details = epg_config['epg_config_details']['type_details'].split('|')

    for types in type_details:
        if types == "type1":  # If only start time is given
            files_name = epg_config['epg_config_details'][types]['file_name'].split('|')
            for f in files_name:
                convert_excel_file_to_csv(f)
                convert_epg_data_to_desire_format_type_1(f.split('.')[0] + '.csv')
        elif types == "type2":  # If start and duration (in x h y min only) given
            files_name = epg_config['epg_config_details'][types]['file_name'].split('|')
            for f in files_name:
                # convert_epg_data_to_desire_format_type_2(f)
                convert_excel_file_to_csv(f)
                convert_epg_data_to_desire_format_type_2(f.split('.')[0] + '.csv')
        elif types == "type3":  # to correct generated epg file
            files_name = epg_config['epg_config_details'][types]['file_name'].split('|')
            for f in files_name:
                convert_epg_data_to_desire_format_type_3(f)
