#!/usr/bin/python
# -*- coding: utf-8 -*-

# Dump an Invenio bibliographic database to standard output

from __future__ import print_function, division

import os
import sys
import time
import sqlite3

sys.path.append(os.path.expanduser('~/lib/python/'))
from invenio.search_engine import perform_request_search, \
    search_pattern, print_record

# Set here the collections not accessible from the main tree.
hidden_collections = ['hidden', 'collection', 'list']


def seconds2human(seconds):
    '''Convert numeric seconds to a human readable string.'''
    days = int((seconds // 3600) // 24)
    hours = int((seconds // 3600) % 24)
    minutes = int((seconds // 60) % 60)
    seconds = int(seconds % 60)
    human = ''
    if days:
        human += '%dd' % (days)
    if hours:
        human += '%dh' % (hours)
    if minutes:
        human += '%dm' % (minutes)
    if seconds:
        human += '%ds' % (seconds)
    return human


def db_create(dbname):
    '''Create SQLite cache database.'''
    print('%s database does not exist.  Creating...' % (dbname),
          file=sys.stderr)
    sql = '''
CREATE TABLE records (
       recid integer primary key,
      record varchar
);'''
    db = sqlite3.connect(dbname)
    db.execute(sql)
    db.close()


def db_get_all_recids(db):
    '''Retrieve all recids from SQLite cache database.'''
    sql = '''
SELECT recid
  FROM records;'''
    cursor = db.cursor()
    cursor.execute(sql)
    recids = cursor.fetchall()
    recids = [recid[0] for recid in recids]
    return recids


def db_get_record(db, recid):
    '''Retrieve a single record from SQLite cache database.'''
    sql = '''
SELECT record
  FROM records
 WHERE recid=?;'''
    cursor = db.cursor()
    values = (recid,)
    record = ''
    cursor.execute(sql, values)
    fields = cursor.fetchone()
    if fields:
        record = fields[0]
    return record


def db_delete_record(db, recid):
    '''Delete a single record from SQLite cache database.'''
    sql = '''
DELETE
  FROM records
 WHERE recid=?;'''
    values = (recid,)
    db.execute(sql, values)
    return True


def invenio_get_all_recids(hidden_collections):
    '''Get all recids stored in Invenio database.'''
    recids = perform_request_search()
    if hidden_collections:
        hidden_recids = perform_request_search(c=hidden_collections)
        recids.extend(hidden_recids)
    return recids


def invenio_get_record(recid):
    '''Retrieve a single record via Invenio API.'''
    record = ''
    try:
        record = print_record(
            recid, 'tm', user_info={'precached_canseehiddenmarctags': True })
    except:
        print('Cannot read record %s' % (recid), file=sys.stderr)
        return ''
    tag = record.split()[0]
    if len(tag) > 3:
        # Records have recid as the first column.  Remove it.
        lines = []
        for line in record.split('\n'):
            line = line.strip()
            if ' ' in line:
                lines.append(line.split(None, 1)[-1])
        record = '\n'.join(lines)
    try:
        record = unicode(record, 'utf-8')
    except UnicodeDecodeError:
        print('UnicodeDecodeError record %s' % (recid), file=sys.stderr)
    return record


def invenio_get_deleted_record(recid):
    '''Create an empty Marc21 record for a recid.'''
    fmt = '''001 __ %s
980 __ $c DELETED
'''
    return fmt % (recid)


def db_update(db, since, verbose):
    '''Retrieve Invenio records since last time, and update SQLite
    database.'''
    sql = '''
REPLACE INTO records
      VALUES (?, ?);'''
    (year, month, day) = time.localtime(since)[:3]
    # Go back a few days, to pick up those records updated on
    # weekends, or when the script didn't run for whatever reason.
    # Each month and year go back a month and a year, too.
    if day > 5:
        day -= 3
    else:
        month -= 1
        day = 1
        if month == 0:
            year -= 1
            month = 12

    recids = perform_request_search(dt='m', d1y=year, d1m=month, d1d=day)
    if hidden_collections:
        hidden_recids = perform_request_search(
            c=hidden_collections, dt='m', d1y=year, d1m=month, d1d=day)
        recids.extend(hidden_recids)
    if not recids:
        if verbose:
            print('No records to update.', file=sys.stderr)
        return 0
    start_time = int(time.time())
    print('Updating %s records...' % (len(recids)), file=sys.stderr)
    n = 0
    for recid in sorted(recids):
        record = invenio_get_record(recid)
        values = (recid, record)
        try:
            db.execute(sql, values)
        except sqlite3.ProgrammingError:
            print('Unrecoverable encoding error: %s' % (recid),
                  file=sys.stderr)
        n += 1
        if n % 100 == 0:
            if verbose:
                elapsed_time = int(time.time()) - start_time
                time_per_record = round(elapsed_time / n, 2)
                remaining_time = time_per_record * (len(recids) - n)
                print('%s of %s records updated. Remaining time: %s (%s seconds per record)' % (
                    n, len(recids),
                    seconds2human(remaining_time), time_per_record),
                      file=sys.stderr)
            db.commit()

    if verbose:
        print('All %s records updated.' % (len(recids)), file=sys.stderr)
    db.commit()

    n = 0
    if verbose:
        print('Searching for deleted records...', file=sys.stderr)
    deleted_recids = search_pattern(p='deleted', f='980').tolist()
    for recid in deleted_recids:
        record = db_get_record(db, recid)
        if not 'DELETED' in record:
            record = invenio_get_deleted_record(recid)
            values = (recid, unicode(record, 'utf-8'))
            db.execute(sql, values)
            n += 1
    db.commit()

    # Perform a final sync comparing the existence of recids
    invenio_recids = set(invenio_get_all_recids(hidden_collections))
    db_recids = set(db_get_all_recids(db))

    deleted_recids = db_recids - invenio_recids
    for recid in deleted_recids:
        db_delete_record(db, recid)
        n += 1
    db.commit()

    if verbose:
        print('Updated %s deleted records.' % (n), file=sys.stderr)

    n = 0
    if verbose:
        print('Searching for missing records...', file=sys.stderr)
    missing_recids = invenio_recids - db_recids
    for recid in missing_recids:
        record = invenio_get_record(recid)
        values = (recid, record)
        try:
            db.execute(sql, values)
        except sqlite3.ProgrammingError:
            print('Unrecoverable encoding error: %s' % (recid),
                  file=sys.stderr)
        n += 1
    if verbose:
        print('Updated %s missing records.' % (n), file=sys.stderr)
    db.commit()
    return n


def db_dump(db):
    '''Dump SQLite database to standard output.'''
    sql = '''
  SELECT record
    FROM records
ORDER BY recid desc;'''
    cursor = db.cursor()
    for row in cursor.execute(sql):
        print(row[0].encode('utf-8'))
        print()


def usage():
    print('''Usage: %s [options] path-to-sqlite-database.db
Dump Invenio Marc database to standard output, using a SQLite database as cache.

Options:
 -h --help     display this help and exit.
 -v --verbose  be verbose.
 -u --update   only update cache database, don't dump the records.
 -d --dump     only dump cached records, don't update the cache database.

With no option, do both: update database, and dump records to standard output.

When run for the first time, that is, when SQLite database does not
yet exist, it will be created and initialised.  Also, as it may take
several hours to fill it (depending on database size), verbose output
will automatically be activated to show expected remaining time.''' % (
        os.path.split(sys.argv[0])[-1]), file=sys.stderr)
    sys.exit(1)


def main():
    args = sys.argv[1:]
    verbose = False
    if not args:
        usage()
    actions = ['update', 'dump']
    while args and args[0].startswith('-'):
        arg = args.pop(0)
        if arg in ['-h', '--help']:
            usage()
        elif arg in ['-v', '--verbose']:
            verbose = True
        elif arg in ['-u', '--update']:
            actions = ['update']
        elif arg in ['-d', '--dump']:
            actions = ['dump']
        else:
            usage()
    if len(args) != 1:
        usage()
    dbname = args[0]
    if os.path.isfile(dbname):
        since_mtime = os.path.getmtime(dbname)
    else:
        db_create(dbname)
        since_mtime = 0
        verbose = True
    db = sqlite3.connect(dbname)
    if 'update' in actions:
        db_update(db, since_mtime, verbose)
    if 'dump' in actions:
        db_dump(db)
    db.close()


if __name__ == '__main__':
    main()
