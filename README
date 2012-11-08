Description
===========

SCRAPI is a Java-based web server that provides an API endpoint to querying timeseries data
in a specific format.  It assumes your rows and columns are ascii plaintext or can at least
be queried in this manner (such that you can turn a plaintext url into a row and column in
whatever internal-to-Cassandra format you have chosen).  It assumes that your columns are
composite columns and that the first field in the column is a long representing the timestamp
of the data in that column.  SCRAPI returns JSON, although it wouldn't be hard to change this
to support multiple output formats.

Example schema
==============

Keyspace:  STATS
Column Family:  rollup1d (containing data rolled up to 1 day buckets)
Row:  "total hits by page id"
Column:
	Field 1: the timestamp: 1234567890 (epoch time in seconds)
	Field 2: the page id: 57

A query must include the keyspace, column family, and row name.  A query SHOULD contain at
least a start time.  A query MAY contain a limit on the number of columns to return, a flag
to reverse the query and return results in reverse time-series order, and an ending timestamp.

A query SHOULD contain the CassandraAPIVersion header with the value set to 1.  This is to
support multiple query versions in the future.

SCRAPI sets a default limit on the number of records it will return for a single query, but in
the case where it had more records to return it will set the value of "has_more" to true in the
resultset's metadata, as well as the next start or end offset to use.  This tells you, the client,
that you can query again with the given parameters to receive more data.

NASTY DETAILS ABOUT THE SCHEMA
==============================

We found that certain rows could get very, very large and hurt performance, so we sharded the
rows based on how much time they were allowed to cover.  This allows cassandra to put one row
on one server, the next row on another server, etc.  As a result of this, most queries will
hit 1 or 2 rows, assuming you aren't querying for a year's worth of 5 minute data.

Row names are in fact "total hits by page id/1234567890" where the 1234567890 represents the
timestamp that this row starts on.  For rollup5m column family rows, that timestamp will be
for 00:00 AM on any given day.  Whatever you have feeding Cassandra and creating the rows and
columns must understand and abide by this standard.  Great caution should be taken in defining
the standard as it will be very difficult and time-consuming to change it.

To be specific, one row in the rollup5m column family only contains data for a 24 hour period.

CF			Amount of Time Per Row		Total number of timestamps in each row, max

rollup5m		24 hours			288
rollup1h		7 days				168
rollup1d		4 months			124
rollup1M (monthly data)	10 years			120

NASTY DETAILS ABOUT THE NASTY DETAILS
=====================================

If you wish to change this, you must modify the code in RestInterfaceHandler.java to understand
your own specific bucketing and row-splitting scheme.

Example Queries
===============

Queries that would return all page ids for all timestamps in the specified ranges within the specified limits:

curl -H "CassandraAPIVersion: 1.0" http://127.0.0.1:9912/STATS/rollup1d/total hits by page id/?start=0&end=2000000000
curl -H "CassandraAPIVersion: 1.0" http://127.0.0.1:9912/STATS/rollup1d/total hits by page id/?start=0&end=2000000000&reversed=true
curl -H "CassandraAPIVersion: 1.0" http://127.0.0.1:9912/STATS/rollup1d/total hits by page id/?start=0&limit=2

A query for a specific page id (page id 57):

curl -H "CassandraAPIVersion: 1.0" http://127.0.0.1:9912/STATS/rollup1d/total hits by page id/57?start=0&limit=2

Output Format
=============

SCRAPI returns results in a json format with associated metadata.  The timestamps in the data
section are in order.  This data can easily be turned into a line graph or a pie chart or a report.

Example query output for the query for a specific page id (page id 57):

{
   "metadata" : {
      "count" : 6,			// how many records returned
      "version" : 1,			// output version
      "has_more" : true,		// there are more records to fetch, we should
					// issue another query using next_start!
      "next_start" : "1463011200",
      "cake" : false			// the cake is always a lie
   },
   "data" : {
      "1454457600" : {
         "57" : 27
      },
      "1454284800" : {
         "57" : 40
      },
      "1454371500" : {
         "57" : 15
      },
      "1454457900" : {
         "57" : 88
      },
      "1454371200" : {
         "57" : 29
      },
      "1454285100" : {
         "57" : 54
      }
   }
}

Build Depends
=============

	ant
	java 1.6 (tested on sun java and openjdk)
	The ant package target requires fpm.
	If you are going to build an fpm .deb file, the VERSION file should reflect the
		version you would like your .deb file to have, e.g. 4.0.1
		In our internal repo, I made sure the VERSION file changed on whatever commit
		was tagged as that version, so when I checked in the VERSION file with
		"4.0.1" as its contents, I would git tag that commit as v4.0.1.

Run Depends
===========

	ant will build a monolithic jarball with all dependencies included
	java 1.6

Run Notes
=========

Running it in jdb, kinda:

jdb -server -sourcepath ../../src/ -classpath Scrapi.jar com.threecrowd.cassy.ScrapiStarter -c ../../test_data/configs/sample_node_config.conf -D
jdb -server -Xdebug -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=50000 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -classpath /Users/dhawth/git/cassandra_related/scrapi/dist/lib/CassandraAPI.jar com.threecrowd.cassy.CassandraAPIDaemonLauncher -c ../../sample_node_config.conf -D

Running it normally:

java -jar Scrapi.jar -c ../../test_data/configs/sample_node_config.conf
java -cp Scrapi.jar com.threecrowd.scrapi.ScrapiStarter -c ../../test_data/configs/sample_node_config.conf

Configuration of Cassandra
==========================

SCRAPI also contains the com.threecrowd.cassy.CreateCF class that can be used to create, delete,
etc, keyspaces and column families in the right format for SCRAPI.

To list keyspaces and cfs:

java -cp Scrapi.jar com.threecrowd.cassy.CreateCF -h localhost:9160 -l

To drop keyspace test:

java -cp Scrapi.jar com.threecrowd.cassy.CreateCF -h localhost:9160 -k test -d -f

To create keyspace and column families at the same time:

java -cp Scrapi.jar com.threecrowd.cassy.CreateCF -h localhost:9160 -k test -c rollup5m -c rollup1h -c rollup1d

Notes
=====

	Testing relies on the embedded cassandra server.
	cassandra.yaml must be edited so all directories are in /tmp/test_cassandra/*
	log4j-server.properties must be changed so rootlogger does not log to R, which
		requires access to /var/log
	Embedded server requires snakeyaml

Performance
===========

	1.  Running it on my laptop and using ab as a client, we know that if it cannot connect
		to the cassandra cluster and if the timeout is set to 100ms, the average response
		time to the client is 177ms and the maximum is 469ms

	2.  Running it on my laptop with a request queue depth of 1000 and allowing 40 connections to
		cassandra, with 2 acceptors, and doing it *through* an ssh tunnel from home to the colo, 
		hitting it with ab -n 10000 -c 100 results in 1k connections/s, 100% success rate,
		with a mean response time of 92ms.  This can probably be made faster with more
		acceptors, a bigger thread pool, a larger queue depth, and more client connections.
		Best requests/sec seen running this test: 1700/s
		Best time I saw running this test multiple times: avg 60ms.
		Worst time: 11s, and that was all in connection time, so it is important to have a
			timeout on the client side and potentially a retry interval.

Upgrading Cassandra
===================

Upgrading to a new version of cassandra generally requires copying a few jars from hector and apache-cassandra/lib

Hector switched from perf4j to speed4j at 0.8, which has to be grabbed from:

git clone https://github.com/jalkanen/speed4j.git
cd speed4j
git checkout speed4j-0.7
mvn clean package -DskipTests
cp target/speed4j-0.7.jar $scrapi/lib

Hector tests also require the high-scale-lib jar, which can be grabbed from http://sourceforge.net/projects/high-scale-lib/
Hector also requires commons-lang 2.4+, downloadable here: http://commons.apache.org/lang/download_lang.cgi
Hector also requires google-collections, now called guava, from here: http://code.google.com/p/guava-libraries/downloads/detail?name=guava-r09.zip&can=2&q=
