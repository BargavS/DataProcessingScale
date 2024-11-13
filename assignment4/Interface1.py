#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	con = openconnection
	cur = con.cursor()
	rangeprefix = "RangeRatingsPart"
	cur.execute("select * from RangeRatingsMetadata;")
	metarows = cur.fetchall()
	outputPath = "RangeQueryOut.txt"
	for row in metarows:
		minRating = row[1]
		maxRating = row[2]
		tableName = rangeprefix + str(row[0])
		if not ((ratingMinValue > maxRating) or (ratingMaxValue < minRating)):
			cur.execute("select * from " + tableName + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
			results = cur.fetchall()
			with open(outputPath, "a") as file:
				for result in results:
					file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")
	robinprefix = "RoundRobinRatingsPart"
	cur.execute("select partitionnum from RoundRobinRatingsMetadata;")
	numPartitions = cur.fetchall()[0][0]
	for i in range(0, numPartitions):
		tableName = robinprefix + str(i)
		cur.execute("select * from " + tableName + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
		results = cur.fetchall()
		with open(outputPath, "a") as file:
			for result in results:
				file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")

def PointQuery(ratingsTableName, ratingValue, openconnection):
	con = openconnection
	cur = con.cursor()
	rangeprefix = "RangeRatingsPart"
	outputPath = "PointQueryOut.txt"
	cur.execute("select * from RangeRatingsMetadata;")
	metarows = cur.fetchall()
	for row in metarows:
		minRating = row[1]
		maxRating = row[2]
		tableName = rangeprefix + str(row[0])
		if ((row[0] == 0 and ratingValue >= minRating and ratingValue <= maxRating) or (row[0] != 0 and ratingValue > minRating and ratingValue <= maxRating)):
			cur.execute("select * from " + tableName + " where rating = " + str(ratingValue) + ";")
			results = cur.fetchall()
			with open(outputPath, "a") as file:
				for result in results:
					file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")
	robinprefix = "RoundRobinRatingsPart"
	cur.execute("select partitionnum from RoundRobinRatingsMetadata;")
	numPartitions = cur.fetchall()[0][0]
	for i in range(0, numPartitions):
		tableName = robinprefix + str(i)
		cur.execute("select * from " + tableName + " where rating = " + str(ratingValue) + ";")
		results = cur.fetchall()
		with open(outputPath, "a") as file:
			for result in results:
				file.write(str(tableName) + "," + str(result[0]) + "," + str(result[1]) + "," + str(result[2]) + "\n")

def writeToFile(filename, rows):
	f = open(filename, 'w')
	for line in rows:
		f.write(','.join(str(s) for s in line))
		f.write('\n')
		f.close()
