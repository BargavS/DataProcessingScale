#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os
from operator import truediv

def getOpenConnection(user='postgres', password='postgres', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection.cursor()
    dropRatingstable = 'DROP TABLE IF EXISTS ' + ratingstablename + ' ;'
    conn.execute(dropRatingstable)
    createTable = "CREATE TABLE " + ratingstablename + " (userid INT, movieid INT, rating  FLOAT) ;"
    conn.execute(createTable)

    with open(ratingsfilepath, 'r') as ratingsFile:
        for r in ratingsFile:
            split = ','.join(r.split('::')[:3])
            split_data = 'INSERT INTO ' + ratingstablename + ' values(' + split + ');'
            conn.execute(split_data)
    openconnection.commit()
    conn.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection.cursor()
    rangePart = round(5 / numberofpartitions, 2)
    rangebegin = 0

    for i in range(0, numberofpartitions):
        if (i == 0):
            rangePartQuery = "CREATE TABLE " + 'range_part' + str(i) + " AS  SELECT * FROM " + \
                          ratingstablename + " WHERE rating >= " + str(rangebegin) + " AND rating <= " + str(rangebegin+rangePart) + ";"

        else:
            rangePartQuery = "CREATE TABLE " + 'range_part' + str(i) + " AS  SELECT * FROM " + \
                          ratingstablename + " WHERE rating > " + str(rangebegin) + " AND rating <= " + str(rangebegin+rangePart) + ";"
        conn.execute('DROP TABLE IF EXISTS ' + 'range_part' + str(i));

        conn.execute(rangePartQuery)

        rangebegin = rangebegin+rangePart

    openconnection.commit()
    conn.close()
def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection.cursor()

    for i in range(0, numberofpartitions):
        conn.execute('DROP TABLE IF EXISTS ' + 'rrobin_part' + str(i) + ' ;')

        roundrobinquery = 'CREATE TABLE ' + 'rrobin_part' + str(i) + ' ( userid INT, movieid INT, rating FLOAT);'
        conn.execute(roundrobinquery)

    conn.execute("SELECT * FROM "+ratingstablename+";")

    partitioningNo = 0

    for r in conn.fetchall():
        insertintoQuery = 'INSERT INTO ' + 'rrobin_part' + str(partitioningNo) + ' VALUES(' + str(r[0]) + ',' + str(r[1]) + ',' + str(
            r[2]) + ');'
        conn.execute(insertintoQuery)
        partitioningNo = partitioningNo + 1
        partitioningNo = (partitioningNo) % numberofpartitions

    openconnection.commit()
    conn.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    datainsert = 'INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(
        rating) + ');'
    cursor.execute(datainsert)
    cursor.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "rrobin_part" + "%';")
    totalNo = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM " + ratingstablename + ";")
    totalRows = (cursor.fetchall())[0][0]
    part = (totalRows - 1) % totalNo

    datainsert = 'INSERT INTO ' + 'rrobin_part' + str(part) + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute(datainsert)

    openconnection.commit()
    cursor.close()
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):



    cursor = openconnection.cursor()

    datainsert = 'INSERT INTO ' + ratingstablename + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(
        rating) + ');'
    cursor.execute(datainsert)

    cursor.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE " + "'" + "range_part" + "%';")
    totalNo = cursor.fetchone()[0]
    rangePart = round(5 / totalNo, 2)

    value = int(rating / rangePart)
    if rating % rangePart == 0 and value != 0:
        value = value - 1

    datainsert = 'INSERT INTO ' + 'range_part' + str(value) + ' VALUES(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ');'
    cursor.execute(datainsert)

    openconnection.commit()
    cursor.close()
def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()

#connection = getOpenConnection()
#loadRatings('ratings', '/Users/bharghavisankar/Documents/Study/ASU/CSE511/assignment3/ratings.dat', connection)
#rangeinsert('ratings', 100, 589, 5, connection)
