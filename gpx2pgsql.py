#!/usr/bin/python2

import psycopg2
import argparse
import os
import sys
import tarfile
import getpass
from xml.dom import pulldom

def create_tables(proj=4326):
    cur = db.cursor()
    cur.execute('drop table if exists gpx_data;')
    cur.execute('drop table if exists gpx_info;')
    cur.execute('drop type if exists gpx_visibility;')

    cur.execute("create type gpx_visibility as enum( 'private', 'trackable', 'public', 'identifiable' )")
    cur.execute("""create table gpx_info (
        gpx_id integer not null primary key,
        visibility gpx_visibility not null,
        gpx_date date,
        uid integer,
        user_name varchar(100),
        description varchar(500)
    );""")
    cur.execute("""create table gpx_data (
        gpx_id integer not null references gpx_info (gpx_id),
        segment_id integer not null,
        track_date date,
        track geometry(linestring, {0}) not null,
        primary key (gpx_id, segment_id)
    );""".format(proj))
    cur.execute('create index on gpx_data (gpx_id);')
    cur.execute('create index on gpx_data using gist (track);')
    cur.close()

def process_metadata(f):
    count = 0
    metadata = {}
    events = pulldom.parse(f)
    for event, node in events:
        if node.localName == 'gpxFile' and event == pulldom.START_ELEMENT:
            m = {}
            for k in ['visibility', 'user']:
                if node.hasAttribute(k):
                    m[k] = node.getAttribute(k)
            for k in ['id', 'uid', 'points']:
                if node.hasAttribute(k):
                    m[k] = int(node.getAttribute(k))
            if node.hasAttribute('timestamp'):
                m['date'] = node.getAttribute('timestamp')[0:10]
            events.expandNode(node)
            desc = node.getElementsByTagName('description')
            if desc and desc[0].firstChild:
                m['description'] = desc[0].firstChild.data[0:500]
            tags = node.getElementsByTagName('tag')
            if tags:
                t = []
                for tag in tags:
                    if tag.firstChild:
                        t.append(tag.firstChild.data)
                m['tags'] = t
            metadata[node.getAttribute('filename')] = m
            count += 1
            if count % 10000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
    return metadata

def store_metadata(db, info):
    cur = db.cursor()
    cur.execute('insert into gpx_info (gpx_id, visibility, gpx_date, uid, user_name, description) values (%s, %s, %s, %s, %s, %s)',
            (info['id'], info['visibility'], info['date'], info.get('uid', None), info.get('user', None), info.get('description', None)))
    cur.close()

def process_gpx(db, gpx_id, f, options):
    cur = db.cursor()
    geomfromtext = 'ST_GeomFromText(%s)'
    if options.reproject:
        geomfromtext = 'ST_Transform({0}, 900913)'.format(geomfromtext)
    segment = 0
    needWrite = False
    events = pulldom.parse(f)
    for event, node in events:
        if event == pulldom.START_ELEMENT:
            if node.localName == 'trkseg':
                points = []
                polledPoints = []
                needWrite = False
                lastNode = None
                lastDate = None
            elif node.localName == 'trkpt':
                lat = float(node.getAttribute('lat'))
                lon = float(node.getAttribute('lon'))
                dist = abs(lon - lastNode[0]) + abs(lat - lastNode[1]) if lastNode else options.dmin * 2
                lastNode = (lon, lat)
                if dist and dist > options.dmax:
                    needWrite = True
                    polledPoints = [(lon, lat)]
                elif not dist or dist >= options.dmin:
                    points.append((lon, lat))
                    if len(points) >= options.pmax:
                        needWrite = True

                    events.expandNode(node)
                    t = node.getElementsByTagName('time');
                    if t and t[0].firstChild and len(t[0].firstChild.data) >= 10:
                        lastDate = t[0].firstChild.data[0:10]
        elif event == pulldom.END_ELEMENT and node.localName == 'trkseg':
            needWrite = True
        if needWrite:
            if points and len(points) >= max(2, options.pmin):
                geom = 'SRID=4326;LINESTRING(' + ','.join(['{0} {1}'.format(x[0], x[1]) for x in points]) + ')'
                cur.execute('insert into gpx_data (gpx_id, segment_id, track_date, track) values (%s, %s, %s, {0})'.format(geomfromtext),
                        (gpx_id, segment, lastDate, geom))
                lastDate = None
                segment += 1
                points = polledPoints
                polledPoints = []
            needWrite = False
    cur.close()

if __name__ == '__main__':
    default_user = getpass.getuser()

    parser = argparse.ArgumentParser(description='Loads OpenStreetMap GPX dump into a PostgreSQL database with PostGIS extension')
    apg_input = parser.add_argument_group('Input')
    apg_input.add_argument('-f', '--file', type=argparse.FileType('r'), help='a file to process', required=True)
    apg_input.add_argument('-s', '--single', action='store_true', help='process a single GPX file')

    apg_filter = parser.add_argument_group('Filter')
    apg_filter.add_argument('--dmin', type=float, help='minimum distance in degrees between track points', default=1e-6)
    apg_filter.add_argument('--dmax', type=float, help='maximum distance in degrees between track points', default=1e-3)
    apg_filter.add_argument('--pmin', type=int, help='minimum number of points in a track segment (default: {0})'.format(10), default=10)
    apg_filter.add_argument('-p', '--pmax', type=int, help='maximum number of points in a track segment (default: {0})'.format(10000), default=10000)

    apg_db = parser.add_argument_group('Database')
    apg_db.add_argument('-u', '--user', help='user name for db (default: {0})'.format(default_user), default=default_user)
    apg_db.add_argument('-w', '--password', action='store_true', help='ask for password')
    apg_db.add_argument('--host', help='database host', default='localhost')
    apg_db.add_argument('--port', type=int, help='database port', default='5432')
    apg_db.add_argument('-d', '--dbname', metavar='DB', help='database (default: gis)', default='gis')
    apg_db.add_argument('-c', '--create-tables', dest='tables', action='store_true', help='recreate tables')

    apg_db.add_argument('-9', '--900913', dest='reproject', action='store_true', help='reproject points to 900913 projection')

    options = parser.parse_args()

    if options.password:
        passwd = getpass.getpass('Please enter database password: ')

    try:
        db = psycopg2.connect(database=options.dbname, user=options.user, password=passwd, host=options.host, port=options.port)
        db.set_client_encoding('UTF8')
    except Exception, e:
        print "Error connecting to database: ", e
        sys.exit(1)

    if options.tables:
        create_tables(900913 if options.reproject else 4326)

    if options.single:
        gpxinfo = { 'id': -1, 'visibility': 'trackable' }
        store_metadata(db, gpxinfo)
        process_gpx(db, gpxinfo['id'], options.file, options)
        sys.exit(0)

    tar = tarfile.open(fileobj=options.file, mode='r|')
    i = 0
    fakeid = -1
    for f in tar:
        if 'metadata.xml' in f.name:
            sys.stdout.write('Processing metadata')
            sys.stdout.flush()
            metadata = process_metadata(tar.extractfile(f))
            db.commit()
            print
            count = len(metadata)
        elif count and f.isfile and '.gpx' in f.name:
            i += 1
            if i % 50 == 0:
                db.commit()
                print i, 'of', count
            for k, v in metadata.items():
                if k in f.name:
                    gpxinfo = v
            if not gpxinfo:
                gpxinfo = { 'id': fakeid, 'visibility': 'trackable' }
                fakeid -= 1
            store_metadata(db, gpxinfo)
            process_gpx(db, gpxinfo['id'], tar.extractfile(f), options)
    tar.close()

    cur = db.cursor()
    cur.execute('vacuum analyze gpx_data;')
    cur.close()
    db.commit()
    db.close()
