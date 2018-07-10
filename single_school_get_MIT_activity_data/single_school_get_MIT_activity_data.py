"""
Extract from mongoDB the data from OEA, RKR and OST into JSON format.
Extract from gstudio logs the time spent for a specific user,
across  platform and all Tools.
Compress everything into a single file.

Use the code from:

https://github.com/gnowledge/gstudio/blob/master/gnowsys-ndf/gnowsys_ndf/ndf/management/commands/activity_timespent.py

But directly reach into MongoDB, like we do for assessments.
"""
import csv
import datetime
import json
import os
import shutil
import sys
import tarfile

import pexpect
import pytz

try:
    from bson import ObjectId
except ImportError:  # old pymongo
    from pymongo.objectid import ObjectId

from copy import deepcopy
from diskcache import Cache
from pymongo import MongoClient
from slugify import slugify

from settings import MONGO_DB_HOST, MONGO_DB_PORT, OUTPUT_DIR, SYNC_DATA_PATH


def aggregate_user_activity():
    """ This checks data for the specified MongoDB """
    # Add the $text index...
    command = 'mongo --host {0} --port {1} --eval \'{2}\''.format(
        MONGO_DB_HOST,
        str(MONGO_DB_PORT),
        'db=db.getSiblingDB("gstudio-mongodb");db.Benchmarks.createIndex({calling_url: "text"});'
    )
    p = pexpect.spawn(command,
                      timeout=60)
    p.expect('numIndexesAfter')
    p.close()

    num_students = activity_details()

    print('Processed {0} students'.format(
        str(num_students)))
    return num_students


def compress_all_data():
    """ compress all qbank json dump files + student
    activity file into a single .tar.gz file """
    output_file = 'qbank_data.tar.gz'
    output_path = os.path.join(OUTPUT_DIR, 'qbank', output_file)
    with tarfile.open(output_path, 'w:gz') as tar:
        mongo_dump_dir = os.path.join(OUTPUT_DIR, 'qbank', 'mongo-dump')
        activity_dump_dir = os.path.join(
            OUTPUT_DIR, 'qbank', 'student-activities')
        tar.add(mongo_dump_dir, arcname=os.path.basename(mongo_dump_dir))
        tar.add(activity_dump_dir, arcname=os.path.basename(activity_dump_dir))


def export_qbank_data():
    """ export the qbank data from mongo as json """
    output_qbank_dir = os.path.join(OUTPUT_DIR, 'qbank', 'mongo-dump')
    if not os.path.exists(output_qbank_dir):
        os.makedirs(output_qbank_dir)
    for database in ['assessment', 'assessment_authoring',
                     'hierarchy', 'id', 'logging',
                     'relationship', 'repository',
                     'resource']:
        command = 'mongodump --host {0} --port {1} -d {2} -o \'{3}\''.format(
            MONGO_DB_HOST,
            str(MONGO_DB_PORT),
            database,
            output_qbank_dir
        )
        p = pexpect.spawn(command,
                          timeout=60)
        p.expect('done dumping')
        p.close()


def generate_postgres_dump():
    """ run command to export postgres data. Assumes you are in
    the docker container. File should initially be created in
    `/var/lib/postgresql/pg_dump_all.sql` """
    command = 'echo "pg_dumpall > pg_dump_all.sql;" | sudo su - postgres ;'
    p = pexpect.spawn('/bin/bash', ['-c', command])
    p.expect(pexpect.EOF)
    p.close()
    output_pg_dump = os.path.join(OUTPUT_DIR, 'qbank', 'pg_dump_all.sql')
    shutil.move('/var/lib/postgresql/pg_dump_all.sql',
                output_pg_dump)


def remove_old_files():
    """ Remove the old files, so we don't
        append to them. """
    qbank_output = os.path.join(OUTPUT_DIR, 'qbank')
    if os.path.exists(qbank_output):
        shutil.rmtree(qbank_output)
    if not os.path.exists(qbank_output):
        os.makedirs(qbank_output)


# ========================================
# Many of these methods are copied from
#   activity_timespent.py
#   tool_logs.py
# Pasted here to provide a single-file for
#   sending to TISS.
# Slightly modified to run for a single
#   school instead of scanning multiple.
# ========================================


def activity_details():
    """ Looks for tools data in SYNC_DATA_PATH,
        postgres dump data in OUTPUT_DIR """
    file_name = school_filename()

    # Generate the list of users that have logged in, from the pg_dump_all.sql
    #    file.
    #  1) Look for "COPY public.auth_user ("
    #  2) Start reading rows
    #  3) Match all rows where the last_login != date_joined
    #  4) Stop at the first "--" line
    #
    # Return a list of tuples [(username, userID)]
    user_list = generate_user_list()
    full_user_list = generate_user_list(full_list=True)

    all_user_data = {}

    for user_data in user_list:
        username = unicode(user_data[0].strip())
        user_id = user_data[1]

        if username not in all_user_data:
            all_user_data[username] = [user_id]

        # activity_in_regex_pattern = '.*/course/activity_player.*'
        # activity_out_regex_pattern =
        #   '.*/course.*|.*my-desk.*|.*explore.*|.*tools/tool-page.*|.*course/content.*'

        activity_in_text_search = 'activity_player'
        # activity_out_text_search = 'course my-desk explore tool-page content'

        GSTUDIO_DB = gstudio_db()

        # This is timing out when there are many records,
        #   so set the `no_cursor_timeout`
        #   optional flag.
        # Changed from $regex to $text, which requires a
        #   $text index on `calling_url` to be set...
        all_visits = GSTUDIO_DB['Benchmarks'].find(
            {
                '$text':
                {
                    '$search': activity_in_text_search
                },
                'user': username
            },
            no_cursor_timeout=True
        ).sort('last_update', -1)

        print "\nTotal activity-player visits for {0}: {1}".format(
            username,
            all_visits.count())

        # Once we know the sequence of Activity / Tool interaction,
        #   we need to aggregate the data into the desired format
        # user_row = []  # prepend username and user_id later

        user_nav_out_by_session = get_all_nav_out_events_by_session(username,
                                                                    GSTUDIO_DB)

        for each_visit in all_visits:
            # Temporarily store each of these in row_data.
            # We'll also add in the tool activity to row_data.
            # Then sort by visited_on.
            row_blob = {
                'unit': 'NA',
                'visited_on': 'NA',
                'language': 'NA',
                'lesson': 'NA',
                'activity': 'NA',
                'timespent': 'NA',
                'buddies': 'NA',
                'out_action': 'NA',
                '_type': 'activity'
            }

            # last_update is saved as IST via Django, even though
            #   it looks like UTC in MongoDB:
            #    ISODate("2017-06-18T04:49:46.243Z")
            # Because they create the Benchmark last_updated field
            #    with `datetime.datetime.now()`, which is system time.
            # So to make it consistent with the tool logging, we'll
            #   convert it to IST...
            visited_on = convert_utc_to_ist(each_visit['last_update'])

            # if visited_on.date() >= start_date.date():
            row_blob['visited_on'] = visited_on
            locale = 'en'
            if 'locale' in each_visit:
                locale = each_visit['locale']

            row_blob['language'] = str(locale)
            calling_url_str = each_visit['calling_url']
            if (calling_url_str.startswith('/') and
                    calling_url_str.endswith('/')):
                splitted_results = calling_url_str.split('/')

                if len(splitted_results) == 7:
                    unit_id = splitted_results[1]
                    lesson_id = splitted_results[4]
                    activity_id = splitted_results[5]
                    unit_node = get_group_name_id(unit_id, get_obj=True)
                    lesson_node = GSTUDIO_DB['Nodes'].find_one(
                        {'_id': ObjectId(lesson_id)})
                    activity_node = GSTUDIO_DB['Nodes'].find_one(
                        {'_id': ObjectId(activity_id)})
                    if not lesson_node:
                        lesson_name = 'Deleted lesson? Unknown name.'
                    else:
                        lesson_name = lesson_node['name']
                    if not activity_node:
                        activity_name = 'Deleted activity? Unknown name.'
                    else:
                        activity_name = activity_node['name']
                    if not unit_node:
                        unit_name = 'Deleted unit? Unknown name.'
                    else:
                        unit_name = unit_node['name']
                        if 'altnames' in unit_node and unit_node['altnames']:
                            unit_name = unit_node['altnames']
                    row_blob.update(
                        {'unit': slugify(unit_name),
                         'lesson': slugify(lesson_name),
                         'activity': slugify(activity_name)})

                    # Using $regex is slow and kills performance...
                    #   4-5 seconds per query.
                    # Wonder how we can speed it up while preserving
                    #   the data captured.

                    # Changed from $regex to $text, which requires a
                    #   $text index on `calling_url` to be set...
                    # Manually checking, it seems to capture the same data.
                    # nav_out_action_cur = GSTUDIO_DB['Benchmarks'].find(
                    #     {'last_update': {'$gte': each_visit['last_update']},
                    #      '_id': {'$ne': each_visit['_id']},
                    #      'user': username,
                    #      'session_key': each_visit['session_key'],
                    #      '$text': {
                    #          '$search': activity_out_text_search
                    #     }}, {'last_update': 1,
                    #          'name': 1}).sort('last_update', 1).limit(1)

                    nav_out_action_cur = find_next_nav_out_action(
                        user_nav_out_by_session,
                        each_visit
                    )
                    if nav_out_action_cur is not None:
                        # if nav_out_action_cur.count():
                        # nav_out_obj = nav_out_action_cur[0]
                        nav_out_obj = nav_out_action_cur
                        end_time = convert_utc_to_ist(
                            nav_out_obj['last_update'])
                        timespent = (end_time - visited_on).total_seconds()
                        print " Time spent: ", timespent, " seconds."
                        row_blob.update({
                            'timespent': str(timespent),
                            'out_action': nav_out_obj['name']})
                    else:
                        print (" ## Unable to track time spent "
                               "on this activity. ##")
                    # Get the buddy ID from the username, using the CSVs
                    buddies_obj = GSTUDIO_DB['Buddies'].find_one({
                        'loggedin_userid': int(user_id),
                        'session_key': str(each_visit['session_key'])
                    })
                    if buddies_obj:
                        auth_id_list = buddies_obj['buddy_in_out'].keys()
                        buddies_names = get_names_list_from_obj_id_list(
                            auth_id_list, u'Author')
                        row_blob.update({'buddies': buddies_names})
                        for buddy in buddies_names:
                            if buddy not in all_user_data:
                                # how do we get the buddy's user ID here?
                                all_user_data[buddy] = [get_buddy_id(
                                    full_user_list,
                                    buddy)]
                            all_user_data[buddy].append(row_blob)
                else:
                    print ("## Unable to track time spent "
                           "on this activity. ##")
                all_user_data[username].append(row_blob)
        all_visits.close()

    for username, user_row in all_user_data.items():
        user_id = user_row[0]

        # Now also check for tool logs
        # Do this here because buddies may not have ``all_visits``
        #   from above, so they won't get their log files checked if
        #   we leave this log-check above.
        for directory, subdirectory, files in os.walk(SYNC_DATA_PATH):
            if ('gstudio_tools_logs' in directory and
                    user_log_file_in(user_id, files)):
                full_path = os.path.join(directory,
                                         get_user_log_filename(user_id, files))
                user_row += get_tool_logs(full_path)
                # Don't need to do separate check for buddies, because gstudio
                #   seems to log each buddy's tool events separately.

    # Once we've collected all the user data + logs, go through
    #   each one and sort them.
    # Grab all of the found dates, so that we can add
    #   filler data, so each user row in the output
    #   has columns that line up.
    all_dates = get_all_dates_from_data(all_user_data)

    # Keep in mind that index 0 is the user_id
    row_data = []
    for username, user_row in all_user_data.items():
        user_id = user_row[0]
        del user_row[0]

        # Now all the data is in user_row, let's sort them in ascending
        #   order by the `visited_on` key.
        user_row = sorted(user_row, key=lambda k: k['visited_on'])

        # Aggregate the data into five columns per date
        user_row = aggregate_user_data(user_row, all_dates)

        # We need to convert the datetime objects to nice strings
        # user_row = [serialize_datetime(r) for r in user_row]

        # Prepend the username and userID to the row
        # user_row = [username, user_id] + [data for r in user_row
        #                                   for data in extract_data(r)]
        user_row = [username, user_id] + user_row
        row_data.append(user_row)

    with open(file_name, 'a') as file_handle:
        activity_writer = csv.writer(file_handle,
                                     dialect='excel')

        column_list = ['Username', 'UserID']

        for one_date in all_dates:
            column_list += ['Date ({0})'.format(str(one_date)),
                            'Total Timestamps',
                            'Total Timespent (HH:MM:SS.microsec)',
                            'Activities Viewed',
                            'Activity Timestamps']
        activity_writer.writerow(column_list)
        # write the row_data out to the activity_writer
        for row in row_data:
            activity_writer.writerow(row)
    return len(row_data)


def activity_headers():
    """ return the standardized set of activity headers for each in / out
        action """
    return ['Unit', 'VisitedOn (IST)',
            'Language', 'Lesson', 'Activity',
            'Timespent', 'Buddies', 'OutAction']


def aggregate_user_data(user_row, all_dates):
    """
    From conversation with Glenda, to support the Adoption Study, we'll
    reformat the output as data within a date range. `all_dates` should
    include all student dates, which means that this student most likely
    will not have activity on some of those dates. Blank cells are
    inserted for these dates.

    For dates with data, we'll have five columns per
    active date:

    * Date of activity
    * Number of timestamps (gstudio + tools)
    * Total time spent on the given date (oldest - newest)
    * Text-list of activities (unique set)
    * Bracket-separated list of timestamps per activity

    This would be repeated for each date a given student has system activity.

    For example, one row might be:

    12/1/2017, 5, '06:32:01.612000', PoliceQuad, [12/1/2017 5:01pm, 12/1/2017 5:02pm]
                                     Let's talk  [12/1/2017 10:30am, 12/1/2017 10:35am]
                                     Reflection  [12/1/2017 11:00am]


    """
    results = []
    student_unique_dates = filter_unique_dates(user_row)
    for date in all_dates:
        if date in student_unique_dates:
            activity_count = count_events_by_date(user_row, date)
            activity_list = get_activities_by_date(user_row, date)
            total_activity_time = get_total_activity_time(user_row, date)
            activity_timestamps = get_activity_timestamps_by_date(user_row,
                                                                  date)
            results.append(get_timestamp_as_date_string(date))
            results.append(activity_count)
            results.append(total_activity_time)
            results.append(activity_list)
            results.append(activity_timestamps)
        else:
            results += [' '] * 5
    return results


def convert_ist_to_utc(ist_time):
    """
    last_update is saved as IST via Django, even though
      it looks like UTC in MongoDB:
       ISODate("2017-06-18T04:49:46.243Z")
    Because they create the Benchmark last_updated field
       with `datetime.datetime.now()`, which is system time (IST).
    So in order to do a $gte filter against the user input
        `start_date`, we need to convert `start_date` to UTC.
    """
    return ist_time.replace(tzinfo=pytz.UTC)


def convert_utc_to_ist(utc_time):
    """
    last_update is saved as IST via Django, even though
      it looks like UTC in MongoDB:
       ISODate("2017-06-18T04:49:46.243Z")
    Because they create the Benchmark last_updated field
       with `datetime.datetime.now()`, which is system time (IST).
    So to make it consistent with the tool logging, we'll
      convert this "fake" UTC time to actual IST...
    """
    ist = pytz.timezone('Asia/Kolkata')
    return utc_time.replace(tzinfo=ist)


def count_events_by_date(user_row, date):
    """ Given a specific date object, count how many timestamps appear
    in the user data on that date """
    return len(filter_activities_by_date(user_row, date))


def extract_data(blob):
    """ pull out only the values we want saved, in order of the headers """
    if blob['_type'] == 'activity':
        return [blob['unit'],
                blob['visited_on'],
                blob['language'],
                blob['lesson'],
                blob['activity'],
                blob['timespent'],
                blob['buddies'],
                blob['out_action']]
    elif blob['_type'] == 'tool':
        return [blob['app_name'],
                blob['created_at'],
                blob['event_type'],
                blob['params']]
    return []


def extract_unique_activity_names(activities):
    """ return just the names of all activities.
    To make them appear semi-logically, sort them according to
    the first timestamp in each activity's timestamps... """
    unique_names = []
    sorted_activities = sorted(activities, key=lambda k: k['visited_on'])
    for activity in sorted_activities:
        activity_name = extract_name(activity)
        if activity_name not in unique_names:
            unique_names.append(activity_name)
    return unique_names


def extract_name(blob):
    """ pull out the activity name to show in the cell """
    if blob['_type'] == 'activity':
        return '{0} / {1} / {2}'.format(blob['unit'],
                                        blob['lesson'],
                                        blob['activity'])
    elif blob['_type'] == 'tool':
        if 'app_name' in blob:
            return blob['app_name']
        elif 'appName' in blob:
            return blob['appName']
        return 'Unknown Tool??'
    return None


def filter_activities_by_date(user_row, date):
    """ return all the activities on a specific date """
    return [blob for blob in user_row
            if blob['visited_on'].date() == date]


def filter_unique_dates(user_row):
    """ given a list of activities (all with `visited_on` as a datetime
    object), returns the list of unique DATES """
    all_dates = [blob['visited_on'].date() for blob in user_row]
    return sorted(list(set(all_dates)))


def find_next_nav_out_action(user_nav_out_by_session, each_visit):
    """ find the next event, hopefully like

    nav_out_action_cur = GSTUDIO_DB['Benchmarks'].find(
        {'last_update': {'$gte': each_visit['last_update']},
         '_id': {'$ne': each_visit['_id']},
         'user': username,
         'session_key': each_visit['session_key'],
         '$text': {
             '$search': activity_out_text_search
        }}, {'last_update': 1,
             'name': 1}).sort('last_update', 1).limit(1)
    """
    session_key = each_visit['session_key']
    visit_id = each_visit['_id']
    visit_time = each_visit['last_update']
    match = None

    for visit in user_nav_out_by_session[session_key]:
        if visit['last_update'] >= visit_time and visit['_id'] != visit_id:
            return visit

    return match


def get_all_dates_from_data(all_user_data):
    """ Get all of the dates in the dataset, across all users """
    all_dates = []
    for username, row in all_user_data.items():
        # Avoid the [0] index, which is `user_id`
        all_dates += filter_unique_dates(row[1::])
    return sorted(list(set(all_dates)))


def get_all_nav_out_events_by_session(username, GSTUDIO_DB):
    """ see if getting all the user events once and putting
    them into memory as a dict with session_key as the keys,
    makes things faster instead of re-querying MongoDB
    constantly. Need to be able to replicate:

    nav_out_action_cur = GSTUDIO_DB['Benchmarks'].find(
        {'last_update': {'$gte': each_visit['last_update']},
         '_id': {'$ne': each_visit['_id']},
         'user': username,
         'session_key': each_visit['session_key'],
         '$text': {
             '$search': activity_out_text_search
        }}, {'last_update': 1,
             'name': 1}).sort('last_update', 1).limit(1)

    More or less...
    """
    activity_out_text_search = 'course my-desk explore tool-page content'

    all_out_records = GSTUDIO_DB['Benchmarks'].find(
        {
            'user': username,
            '$text': {
                '$search': activity_out_text_search
            }
        }, {
            'last_update': 1,
            'name': 1,
            'session_key': 1,
            '_id': 1}).sort('last_update', 1)
    results = {}
    for record in all_out_records:
        session_key = record['session_key']
        if session_key not in results.keys():
            results[session_key] = []
        results[session_key].append(record)

    # now sort all records by `last_update`
    for session, records in results.items():
        results[session] = sorted(records, key=lambda k: k['last_update'])

    return results


def get_buddy_id(user_list, buddy_name):
    """ given a buddy name, return their ID from the list of active users """
    for user_data in user_list:
        username = unicode(user_data[0].strip())
        user_id = user_data[1]
        if username == buddy_name:
            return user_id
    raise LookupError('Buddy not found??')


def get_total_activity_time(user_row, date):
    """ for a given date return the timespan of activity data """
    activities = filter_activities_by_date(user_row, date)
    sorted_activities = sorted(activities, key=lambda k: k['visited_on'])
    time_spent = (sorted_activities[-1]['visited_on'] -
                  sorted_activities[0]['visited_on'])
    return str(time_spent)


def get_group_name_id(group_name_or_id, get_obj=False):
    '''
      Taken from https://github.com/gnowledge/gstudio/blob/master/
        gnowsys-ndf/gnowsys_ndf/ndf/views/methods.py

      - This method takes possible group name/id as an argument and returns
        (group-name and id) or group object.
      - If no second argument is passed, as method name suggests, returned
        result is "group_name" first and "group_id" second.
      - When we need the entire group object, just pass second argument as
        boolian) True. In the case group object will be returned.
      Example 1: res_group_name, res_group_id = get_group_name_id(
        group_name_or_id)
      - "res_group_name" will contain name of the group.
      - "res_group_id" will contain _id/ObjectId of the group.
      Example 2: res_group_obj = get_group_name_id(group_name_or_id,
                                                   get_obj=True)
      - "res_group_obj" will contain entire object.
      Optimization Tip: before calling this method, try to cast group_id to
      ObjectId as follows (or copy paste following snippet at start of function
      or wherever there is a need):
      try:
          group_id = ObjectId(group_id)
      except:
          group_name, group_id = get_group_name_id(group_id)
    '''
    cache = Cache('/tmp/clix-research-data')
    # if cached result exists return it
    if not get_obj:
        slug = slugify(group_name_or_id)
        # for unicode strings like hindi-text slugify doesn't works
        cache_key = 'get_group_name_id_' + str(slug) if slug else str(
            abs(hash(group_name_or_id)))
        cache_result = cache.get(cache_key)

        if cache_result:
            return (cache_result[0], ObjectId(cache_result[1]))
    # ---------------------------------

    GSTUDIO_DB = gstudio_db()

    # case-1: argument - "group_name_or_id" is ObjectId
    if ObjectId.is_valid(group_name_or_id):

        group_obj = GSTUDIO_DB['Nodes'].find_one(
            {"_id": ObjectId(group_name_or_id)})

        # checking if group_obj is valid
        if group_obj:
            # if (group_name_or_id == group_obj._id):
            group_id = group_name_or_id
            group_name = group_obj['name']

            if get_obj:
                return group_obj
            else:
                # setting cache with both ObjectId and group_name
                cache.set(cache_key, (group_name, group_id), 60 * 60)
                cache_key = u'get_group_name_id_' + slugify(group_name)
                cache.set(cache_key, (group_name, group_id), 60 * 60)
                return group_name, group_id

    # case-2: argument - "group_name_or_id" is group name
    else:
        group_obj = GSTUDIO_DB['Nodes'].find_one(
            {"_type": {"$in": ["Group", "Author"]},
             "name": unicode(group_name_or_id)})

        # checking if group_obj is valid
        if group_obj:
            # if (group_name_or_id == group_obj.name):
            group_name = group_name_or_id
            group_id = group_obj['_id']

            if get_obj:
                return group_obj
            else:
                # setting cache with both ObjectId and group_name
                cache.set(cache_key, (group_name, group_id), 60 * 60)
                cache_key = u'get_group_name_id_' + slugify(group_name)
                cache.set(cache_key, (group_name, group_id), 60 * 60)
                return group_name, group_id

    if get_obj:
        return None
    else:
        return None, None


def get_names_list_from_obj_id_list(obj_ids_list, node_type):
    """ Taken from https://github.com/gnowledge/gstudio/blob/master/gnowsys-ndf/gnowsys_ndf/ndf/models/node.py
    """
    GSTUDIO_DB = gstudio_db()
    obj_ids_list = map(ObjectId, obj_ids_list)
    nodes_cur = GSTUDIO_DB['Nodes'].find(
        {
            '_type': node_type,
            '_id': {'$in': obj_ids_list}
        },
        {
            'name': 1
        }
    )
    result_list = [node['name'] for node in nodes_cur]
    return result_list


def generate_user_list(full_list=False):
    """
    TODO: gstudio now does this differently:

    https://github.com/gnowledge/gstudio/blob/master/doc/deployer/get_all_users_activity_timestamp_csvs.py#L4
    https://github.com/gnowledge/gstudio/blob/master/gnowsys-ndf/gnowsys_ndf/ndf/models/author.py#L113-L130

    Which requires database access to the Nodes collection...

    So we would have to do something similar, because it seems like gstudio
    does not necessarily use the postgres logins now. I was getting different
    results for active users from Nodes versus Postgres.

    Still use this method to get the full list, because we use that
    to match user IDs and usernames (for tool logs).

    =========================================================

    Generate the list of users that have logged in, from the pg_dump_all.sql
       file.
     1) Look for "COPY public.auth_user ("
     2) Start reading rows
     3) Match all rows where the last_login != date_joined
        - Rows are tab delimited
        - last_login = index 2 (starting at 0)
        - date_joined = index 10 (starting at 0)
     4) Stop at the first "\." line

    Return a list of tuples [(username, userID)]
    """
    user_list = []
    active_user_ids = []

    if not full_list:
        # Use the new gstudio method to query counter_collections
        # But use the postgres dump file to match to usernames instead
        #       of relying on the Author collection.
        GSTUDIO_DB = gstudio_db()
        active_user_ids = GSTUDIO_DB['Counters'].find().distinct('user_id')

    postgres_dump = os.path.join(OUTPUT_DIR, 'qbank', 'pg_dump_all.sql')
    with open(postgres_dump, 'r') as postgres_file:
        start_reading = False
        for row in postgres_file.readlines():
            if start_reading and row.strip() == '\.':
                # delimiter for end of the COPY block
                break
            if start_reading:
                # last_login = grab_last_login(row)
                # date_joined = grab_date_joined(row)
                user_id = grab_user_id(row)
                if user_id in active_user_ids or full_list:
                    user_list.append((grab_username(row),
                                      grab_user_id(row)))
            if row.startswith('COPY public.auth_user ('):
                start_reading = True

    print('Found {0} active users at this school'.format(str(len(user_list))))
    return user_list


def get_activities_by_date(user_row, date):
    """ aggregate all of the activities that happened on a single date,
    into a single "cell" -- text separated by newlines. """
    activities = filter_activities_by_date(user_row, date)
    activity_names = extract_unique_activity_names(activities)
    return newline_join(activity_names)


def get_activity_timestamps_by_date(user_row, date):
    """ aggregate all of the activity timestamps that happened on a single
    date, into a single "cell" -- bracketed lists separated by newlines """
    activities = filter_activities_by_date(user_row, date)
    # need to make sure it's in the same order as get_activities_by_date
    activity_names = extract_unique_activity_names(activities)
    results = []
    for activity_name in activity_names:
        activity_timestamps = [serialize_datetime(blob)['visited_on']
                               for blob in activities
                               if extract_name(blob) == activity_name]
        results.append(json.dumps(sorted(activity_timestamps)))
    return newline_join(results)


def grab_date_joined(row):
    """ return date_joined, which should be index 10 ... and strip() it
        because it's the last item in the row and includes a \n """
    return row.split('\t')[10].strip()


def grab_last_login(row):
    """ return last_login, which should be index 2 """
    return row.split('\t')[2]


def grab_username(row):
    """ return username, which should be index 4 """
    return row.split('\t')[4]


def grab_user_id(row):
    """ return the ID, which should be index 0 """
    return row.split('\t')[0]


def gstudio_db():
    """ get MongoClient instance for gstudio """
    MC = MongoClient(host=MONGO_DB_HOST, port=MONGO_DB_PORT)
    return MC['gstudio-mongodb']


def headers(blob):
    """ if is an activity blob, return the activity headers
        if is a tool blob, return the tool headers """
    if blob['_type'] == 'activity':
        return activity_headers()
    return tool_headers()


def newline_join(stuff):
    """ return a newline joined string, from the stuff list """
    return '\r\n'.join(stuff)


def school_filename():
    """ helper method that returns the output file name """
    activities_dir = os.path.join(OUTPUT_DIR, 'qbank', 'student-activities')
    if not os.path.exists(activities_dir):
        os.makedirs(activities_dir)
    return os.path.join(activities_dir, 'activity-data.csv')


def serialize_datetime(blob):
    """ Make the output human readable,
    ISODate("2017-06-18T04:49:46.243Z") => 2017-06-18  4:27:00 PM """
    blob = deepcopy(blob)
    output_format = '%Y-%m-%d %I:%M:%S %p'
    if blob['_type'] == 'activity':
        blob['visited_on'] = blob['visited_on'].strftime(output_format)
    elif blob['_type'] == 'tool':
        blob['created_at'] = blob['created_at'].strftime(output_format)
        blob['visited_on'] = blob['visited_on'].strftime(output_format)
    return blob


def get_timestamp_as_date_string(datetime_object):
    """ Make the output human readable,
    ISODate("2017-06-18T04:49:46.243Z") => 2017-06-18 """
    output_format = '%Y-%m-%d'
    date_string = datetime_object.strftime(output_format)
    return date_string


def get_user_log_filename(user_id, files):
    """ return the actual user log filename """
    if not user_log_file_in(user_id, files):
        raise IOError('User log not found.')
    if '{0}.json'.format(user_id) in files:
        return '{0}.json'.format(user_id)
    for filename in files:
        if (filename.startswith('{0}-'.format(user_id)) and
                filename.endswith('.json')):
            return filename


def user_log_file_in(user_id, files):
    """ check if `{user_id}-{activity-name}.json` is in the list of files.
    Returns the filename """
    if '{0}.json'.format(user_id) in files:
        return True
    for filename in files:
        if (filename.startswith('{0}-'.format(user_id)) and
                filename.endswith('.json')):
            return True
    return False


def get_tool_logs(log_file):
    """ Assumes the log file is the full path to a JSON file,
        with list of log objects.
        start_date is the date from which we want data.

        Assumes keys are (per policequad examples):
            * user_id
            * created_at
            * params
            * app_name
            * event_type

        Some TISS tools switched to camelCase...so we'll just
            convert back if we see camelCase. But it's not consistent.
    """
    with open(log_file, 'r') as tool_log_data:
        filtered_results = []
        try:
            tool_log = json.load(tool_log_data)
        except ValueError:
            print('{0} has invalid JSON data...report it to TISS'.format(
                log_file))
        else:
            for blob in tool_log:
                if 'created_at' in blob:
                    blob['created_at'] = convert_iso_string_to_ist(
                        blob['created_at'])
                elif 'createdAt' in blob:
                    blob['created_at'] = convert_iso_string_to_ist(
                        blob['createdAt'])
                else:
                    # Some tools aren't logging data correctly.. put some
                    #   absurdly future time here so the data isn't lost...
                    blob['created_at'] = fake_future_ist()
                blob['visited_on'] = blob['created_at']  # need this to sort
                blob['_type'] = 'tool'
                if 'appName' in blob:
                    blob['app_name'] = blob['appName']
                filtered_results.append(blob)
        return filtered_results


def convert_iso_string_to_ist(ist_string):
    """ because the tools save JSON strings like "2017-11-7 11:14:12"
        as the created_at values...but to do datetime comparisons and
        sort, we need a datetime object """
    ist = pytz.timezone('Asia/Kolkata')
    # Use strptime because the tool logs do not 0-pad minutes / seconds...
    #   i.e."created_at": "2017-11-7 11:35:6"
    # Which breaks libraries like iso8601.
    input_format = '%Y-%m-%d %H:%M:%S'
    naive_time = datetime.datetime.strptime(ist_string, input_format)
    return naive_time.replace(tzinfo=ist)


def fake_future_ist():
    """ create a fake future IST time since some tools like Astroamer
    Moon Track aren't logging a `created_at` time in the logs entries """
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.datetime.now().replace(
        tzinfo=ist) + datetime.timedelta(days=5000 * 365)


def tool_headers():
    """ return the standardized set of tool headers for each tool
        interaction """
    return ['AppName', 'CreatedAt (IST)',
            'EventType', 'Params']

# ===============================
# End of mostly-copied methods.
# ===============================


# Make this method available at the command line, so we can call it
# from Jenkins.

if __name__ == '__main__':
    try:
        remove_old_files()
        export_qbank_data()
        generate_postgres_dump()
        try:
            aggregate_user_activity()
        except:
            pass  # Grab QBank data anyways
        output_filename = compress_all_data()
        print('Export all qbank data as {0}'.format(output_filename))
    except:
        # http://stackoverflow.com/questions/1000900/how-to-keep-a-python-script-output-window-open#1000968
        import traceback
        print sys.exc_info()[0]
        print traceback.format_exc()
    finally:
        print('Done!')
