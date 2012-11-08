/*
 * Copyright 2012 David Hawthorne, 3Crowd/XDN, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.threecrowd.scrapi;

import junit.framework.TestCase;
import org.apache.log4j.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
import java.util.*;

//
// jackson json parser
//
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonParser;

import com.threecrowd.scrapi.models.*;

import me.prettyprint.cassandra.serializers.*;

import me.prettyprint.cassandra.service.ThriftColumnDef;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import me.prettyprint.hector.api.factory.HFactory;

import org.apache.thrift.TException;

import me.prettyprint.cassandra.model.HColumnImpl;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.CounterSuperSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.beans.Composite;

import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.RangeSubSlicesQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.query.SubCountQuery;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperCountQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;
import me.prettyprint.hector.api.query.SuperSliceCounterQuery;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.contrib.utils.service.*;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.*;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.threecrowd.cassy.CassandraSetup;

public final class TestRestInterfaceHandler extends TestCase
{
	@Nullable
	private Logger log = null;
	@Nullable
	private Cluster cluster;
	@NotNull
	static final private StringSerializer se = new StringSerializer();
	@NotNull
	static final private LongSerializer le = new LongSerializer();
	@NotNull
	static final private CompositeSerializer cs = new CompositeSerializer();

	@SuppressWarnings({"AssignmentToNull"})
	public void testRestInterfaceHandler()
	{
		Properties logProperties = new Properties();

		logProperties.put("log4j.rootLogger", "ERROR, stdout");
		logProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		logProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.EnhancedPatternLayout");
		logProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%d [%F:%L] [%p] %C{1}: %m%n");
		logProperties.put("log4j.appender.stdout.immediateFlush", "true");

		logProperties.put("log4j.appender.null", "org.apache.log4j.varia.NullAppender");
		logProperties.put("log4j.category.com.threecrowd.scrapi.RestInterfaceHandler", "DEBUG, stdout");
		logProperties.put("log4j.additivity.com.threecrowd.scrapi.RestInterfaceHandler", "false");
		logProperties.put("log4j.category.com.threecrowd.scrapi.TestRestInterfaceHandler", "DEBUG, stdout");
		logProperties.put("log4j.additivity.com.threecrowd.scrapi.TestRestInterfaceHandler", "false");

		BasicConfigurator.resetConfiguration();
		PropertyConfigurator.configure(logProperties);

		//
		// setting it to debug here enables all of the hector log output, as well as
		// jetty output.
		//
		// Logger.getRootLogger().setLevel((Level) Level.DEBUG);

		log = Logger.getLogger(TestRestInterfaceHandler.class);

		try
		{
			CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
			cleaner.prepare();
			EmbeddedCassandraService cassandraServer = new EmbeddedCassandraService();
			cassandraServer.start();

			try
			{
				CassandraSetup cassandraSetup = new CassandraSetup("localhost:9161");

				log.debug("running setup with keyspaces and column families");
				cassandraSetup.createKeyspace("test", 1);
				cassandraSetup.createCF("test", "rollup5m", 1);
				cassandraSetup.createCF("test", "rollup1h", 1);
				cassandraSetup.createCF("test", "rollup1d", 1);
				HashMap<String, ArrayList<String>> results = cassandraSetup.getKeyspaceInfo();
				log.debug("keyspace info: " + results);
			}
			catch (final Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			NodeConfig config = new NodeConfig();

			config.column_families.put("rollup5m", 1);
			config.column_families.put("rollup1h", 1);
			config.column_families.put("rollup1d", 1);

			StatsObject so = StatsObject.getInstance();
			Server server = new Server(19913);
			ContextHandlerCollection cc = new ContextHandlerCollection();
			ContextHandler hch = cc.addContext("/", ".");
			RestInterfaceHandler h = new RestInterfaceHandler(config);
			hch.setHandler(h);
			hch.setAllowNullPathInfo(true);
			server.setHandler(cc);
			server.start();

			URL u = null;
			HttpURLConnection conn = null;
			String response = null;

			u = new URL("http://localhost:19913/node/config");
			conn = (HttpURLConnection) u.openConnection();
			conn.connect();
			assertTrue("expected 200, but instead got " + conn.getResponseCode(),
				   conn.getResponseCode() == 200);
			response = getResponse(conn);
			assertTrue("response not as expected: " + response, response.equals(
				"{\"cassandra_host\":null,\"local_port\":null,\"local_address\":null,\"client_idle_timeout\":null,\"statsd_config\":null,\"jetty_config\":{\"single_threaded\":false,\"number_of_acceptors\":2,\"accept_queue_size\":100,\"thread_pool_size\":256},\"cassandra_config\":{\"max_connections\":5,\"thrift_socket_timeout\":5000,\"connection_retry_time\":2,\"queries_per_request\":100},\"column_families\":{\"rollup1h\":1,\"rollup5m\":1,\"rollup1d\":1}}"));
			log.debug("response is " + response);
			conn.disconnect();

			u = new URL("http://localhost:19913/usage");
			conn = (HttpURLConnection) u.openConnection();
			conn.setRequestProperty("CassandraAPIVersion", "1");
			conn.connect();
			assertTrue("expected 200, but instead got " + conn.getResponseCode(),
				   conn.getResponseCode() == 200);
			response = getResponse(conn);
			assertTrue("response not as expected: " + response, response.equals(
				"usage: GET /keyspace/column_family/row[/subcolum_name][/?start=X&end=Y]"));
			log.debug("response is " + response);
			conn.disconnect();

			so.clear();

			u = new URL("http://localhost:19913/node/stats");
			conn = (HttpURLConnection) u.openConnection();
			conn.connect();
			log.debug("response message is " + conn.getResponseMessage());
			response = getResponse(conn);
			log.debug("response is " + response);
			assertTrue("expected 200, but instead got " + conn.getResponseCode(),
				   conn.getResponseCode() == 200);
			assertTrue("response not as expected: " + response, response.equals(
				"{\"RestInterfaceHandler.total_node_stats_requests\":\"1\",\"RestInterfaceHandler.total_hits\":\"1\",\"RestInterfaceHandler.total_hits_for_method_GET\":\"1\"}"));
			conn.disconnect();

			config.cassandra_config.thrift_socket_timeout = 200;
			config.cassandra_config.max_connections = 20;
			config.cassandra_config.queries_per_request = 100;

			//
			// first try to connect to something that shouldn't work
			//
			config.cassandra_host = "localhost:9162";

			String ks = "test";

			cluster = null;

			//
			// test a timeout failure and make sure it happens in less than 2x the
			// configured time
			//
			Long startTime = System.currentTimeMillis();

			try
			{
				if (cluster == null)
				{
					CassandraHostConfigurator cassandraHostConfigurator =
						new CassandraHostConfigurator(config.cassandra_host);

					//
					// The maximum amount of time to wait if there are no clients available
					//
					cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(100);
					//
					// max timeout for any operation
					//
					cassandraHostConfigurator.setCassandraThriftSocketTimeout(100);
					//
					// max number of connections to hold open to this host
					//
					cassandraHostConfigurator.setMaxActive(1);

					if (config.cassandra_config != null)
					{
						if (config.cassandra_config.thrift_socket_timeout != null)
						{
							cassandraHostConfigurator.setCassandraThriftSocketTimeout(
								config.cassandra_config.thrift_socket_timeout);
						}

						if (config.cassandra_config.max_connections != null)
						{
							cassandraHostConfigurator.setMaxActive(
								config.cassandra_config.max_connections);
						}
					}

					log.debug("getting cluster");
					cluster = HFactory.getOrCreateCluster("FailCluster", cassandraHostConfigurator);
				}

				List<KeyspaceDefinition> ksDefs = null;
				ksDefs = cluster.describeKeyspaces();
			}
			catch (Exception he)
			{
				assertTrue("hector exception didn't match expectations: " + he.getMessage(),
					   he.getMessage().contains("All host pools marked down"));
				Long endTime = System.currentTimeMillis();
				assertTrue("failure to connect took too long: " + (endTime - startTime),
					   (endTime - startTime) < 200);
			}

			cluster = null;

			//
			// now try to make a connection to something that should work
			//

			//
			// this is set up in the test_conf/cassandra.yaml
			//
			config.cassandra_host = "localhost:9161";
			config.cassandra_config.thrift_socket_timeout = 2000;

			if (cluster == null)
			{
				CassandraHostConfigurator cassandraHostConfigurator =
					new CassandraHostConfigurator(config.cassandra_host);

				//
				// The maximum amount of time to wait if there are no clients available
				//
				cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(100);
				//
				// max timeout for any operation
				//
				cassandraHostConfigurator.setCassandraThriftSocketTimeout(2000);
				//
				// max number of connections to hold open to this host
				//
				cassandraHostConfigurator.setMaxActive(1);

				if (config.cassandra_config != null)
				{
					if (config.cassandra_config.thrift_socket_timeout != null)
					{
						cassandraHostConfigurator.setCassandraThriftSocketTimeout(
							config.cassandra_config.thrift_socket_timeout);
					}

					if (config.cassandra_config.max_connections != null)
					{
						cassandraHostConfigurator.setMaxActive(
							config.cassandra_config.max_connections);
					}
				}

				log.debug("getting cluster");
				cluster = HFactory.getOrCreateCluster("CassandraCluster", cassandraHostConfigurator);
			}

			//
			// column families were already created, go ahead and load all the data
			//
			String data_file_contents = ConfigReader.ReadFile("test_data/load_data.json");

			Keyspace keyspace = HFactory.createKeyspace(ks, cluster);

			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
			JsonNode root = mapper.readTree(data_file_contents);

			Iterator<String> cfNames = root.getFieldNames();

			while (cfNames.hasNext())
			{
				String cf = cfNames.next();
				JsonNode cfData = root.get(cf);
				log.debug("found cf: " + cf);
				Iterator<String> rowNames = cfData.getFieldNames();

				Mutator<String> mutator = HFactory.createMutator(keyspace, se);
				assertNotNull(mutator);

				while (rowNames.hasNext())
				{
					String rowname = rowNames.next();
					JsonNode row = cfData.get(rowname);
					log.debug("found row: " + rowname + ": " + row);
					Iterator<String> timestamps = row.getFieldNames();

					while (timestamps.hasNext())
					{
						String ts = timestamps.next();
						log.debug("row: " + rowname + ", timestamp: " + ts);
						JsonNode tsEntry = row.get(ts);
						Iterator<String> keys = tsEntry.getFieldNames();

						while (keys.hasNext())
						{
							String key = keys.next();
							Long value = tsEntry.get(key).getLongValue();
							Long tsLong = Long.parseLong(ts);

							log.debug(
								"row: " + rowname + ", timestamp: " + tsLong + ", key: " + key + " = " + value);

							HColumnImpl<Composite, Long> column = new HColumnImpl<Composite, Long>(
								cs, le);
							column.setClock(keyspace.createClock());
							Composite dc = new Composite();
							dc.add(0, tsLong);
							dc.add(1, key);
							column.setName(dc);
							column.setValue(value);
							mutator.addInsertion(rowname, cf, column);
						}
						//
						// end of foreach key in row in CF
						//
					}
					//
					// end of foreach row in CF
					//

					log.debug("mutator is: " + mutator);

					mutator.execute();
				}
				//
				// end of foreach CF
				//
			}

			//
			// now make a few queries against the cluster to make sure that is working
			//
			String RowName = "row";

			//
			// simple single query tests against rollup5m to make sure it works
			// and we have a working sample of how to do a slice query
			//
			{
				SliceQuery<String, Composite, Long> sliceQuery = HFactory.createSliceQuery(keyspace, se,
													   cs, le);
				sliceQuery.setColumnFamily("rollup5m");
				sliceQuery.setKey(RowName + "/1454284800");

				Composite startRange = new Composite();
				startRange.add(0, 0L);

				Composite endRange = new Composite();
				endRange.add(0, 1000000000000L);

				sliceQuery.setRange(startRange, endRange, false, 2);

				QueryResult<ColumnSlice<Composite, Long>> r = sliceQuery.execute();
				assertNotNull(r);

				log.debug("r is: " + r);

				ColumnSlice<Composite, Long> cs = r.get();
				assertNotNull(cs);

				List<HColumn<Composite, Long>> columns = cs.getColumns();

				assertTrue("columns.size = " + columns.size(), columns.size() == 2);

				//
				// braces trigger lexical scoping of $col
				//
				{
					HColumn<Composite, Long> col = columns.get(0);

					assertTrue("value: " + col.getValue(), col.getValue() == 1L);
					assertTrue("col timestamp: " + col.getName().get(0, le),
						   col.getName().get(0, le) == 1454284800L);
					assertTrue("col subcolumn: " + col.getName().get(1, se),
						   col.getName().get(1, se).equals("1"));
				}

				for (HColumn<Composite, Long> col : cs.getColumns())
				{
					log.debug("row: " + RowName + "/1454284800, column: " + col.getName().get(0,
														  le) + ":" + col.getName().get(
						1, se) + " = " + col.getValue());
					// System.out.println(col.getName().getComponents());
					log.debug(col.getName().get(0, LongSerializer.get()));
					log.debug(col.getName().get(1, StringSerializer.get()));
				}
			}

			//
			// now reversed with a limit
			// IMPORTANT NOTE: start and end in the setRange must be flipped for this to work, else
			//	you'll get an exception!!!
			// ANOTHER NOTE:  this only returns "row", 10:"3" = 3, which is nice and all,
			// 	but it might be more valuable to look for "row", 10:*, ie. all of the columns
			//	matching the last timestamp in the Range provided, but that would require
			//	knowing exactly how many columns to ask for.
			// This is still valuable for our pagination stuff, and the limit is still on how
			//	many timestamps worth to return, not on how many subcolumns.
			//
			// It is important to note that the end is EXCLUSIVE, not INCLUSIVE, so asking for 0-10 reversed
			// means 9-0
			//
			{
				SliceQuery<String, Composite, Long> sliceQuery = HFactory.createSliceQuery(keyspace, se,
													   cs, le);
				sliceQuery.setColumnFamily("rollup5m");
				sliceQuery.setKey(RowName + "/1454284800");

				Composite startRange = new Composite();
				startRange.add(0, 1454285101L);

				Composite endRange = new Composite();
				endRange.add(0, 1454284800L);

				sliceQuery.setRange(startRange, endRange, true, 1);

				QueryResult<ColumnSlice<Composite, Long>> r = sliceQuery.execute();
				assertNotNull(r);

				log.debug("r is: " + r);

				ColumnSlice<Composite, Long> cs = r.get();
				assertNotNull(cs);

				List<HColumn<Composite, Long>> columns = cs.getColumns();

				assertTrue("columns.size = " + columns.size(), columns.size() == 1);

				for (HColumn<Composite, Long> col : cs.getColumns())
				{
					log.debug("row: " + RowName + "/1454284800, column: " + col.getName().get(0,
														  le) + ":" + col.getName().get(
						1, se) + " = " + col.getValue());
					// System.out.println(col.getName().getComponents());
				}

				//
				// braces trigger lexical scoping of $col
				//
				{
					HColumn<Composite, Long> col = columns.get(0);

					//
					// last value in last column should be first entry in resultset
					//
					assertTrue("value: " + col.getValue(), col.getValue() == 3L);
					assertTrue("col timestamp: " + col.getName().get(0, le),
						   col.getName().get(0, le) == 1454285100L);
					assertTrue("col subcolumn: " + col.getName().get(1, se),
						   col.getName().get(1, se).equals("3"));
				}

			}

			//
			// yeah, ok, now for a couple range queries
			//

			//
			// limits referred to herein are timestamp limits, not individual record limits
			//

			//
			// 1.  limit of 1
			// 2.  limit of 1, reversed
			// 3.  no limit, a - b, get everything
			// 4.  no limit, a - b, with a sub col specified (2)
			// 5.  query from a - b should not include b
			//

			String query;

			try
			{
				log.debug(
					"starting query for 1454284799 - end, limit 1, I expect to get back timestamp 1454284800 => [1, 2, 3]");
				query = "start=1454284799&end=10000000000&limit=1";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				log.debug("response message is " + conn.getResponseMessage());
				response = getResponse(conn);
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 1);
				metaData.put("has_more", true);
				metaData.put("cake", Boolean.FALSE);
				metaData.put("next_start",
					     "1454284801");		// because limit is 1, next start offset should be 1s
				// after the last ts returned

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();
				data.put("1454284800", new TreeMap<String, Long>());
				data.get("1454284800").put("1", 1L);
				data.get("1454284800").put("2", 2L);
				data.get("1454284800").put("3", 3L);

				jsonObjectTree.put("data", data);

				log.debug("metaData as TreeMap: " + jsonObjectTree);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				TreeMap jsonObject = mapper.readValue(expected, TreeMap.class);

				log.debug("metaData as json string: " + expected);
				log.debug("metaData as json object: " + jsonObject);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				assertTrue(e.getMessage(), false);
			}

			try
			{
				//
				// this has to be smart, because the end can be so high, and there are soooooo many rows
				// to query for...
				//
				log.debug("starting query for end - 0, limit 1, reversed");
				query = "start=0&end=1454457900&limit=1&reverse=true";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				log.debug("response message is " + conn.getResponseMessage());
				response = getResponse(conn);
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				//
				// because it's reversed, it counts down to the 0th entry, so value is 0
				//
				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 1);
				metaData.put("has_more", true);
				metaData.put("cake", Boolean.FALSE);
				metaData.put("next_end", "1454457899");

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();
				data.put("1454457900", new TreeMap<String, Long>());
				data.get("1454457900").put("1", 1L);
				data.get("1454457900").put("2", 2L);
				data.get("1454457900").put("3", 3L);

				jsonObjectTree.put("data", data);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				log.debug("metaData as json string: " + expected);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				assertTrue(e.getMessage(), false);
			}

			//
			// query to get all timestamps
			//
			try
			{
				query = "start=1454284800&end=1454457900";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				log.debug("response message is " + conn.getResponseMessage());
				response = getResponse(conn);
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 6);
				metaData.put("has_more", false);
				metaData.put("cake", Boolean.FALSE);

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();

				ArrayList<String> timestamps = new ArrayList<String>();
				timestamps.add("1454284800");
				timestamps.add("1454285100");
				timestamps.add("1454371200");
				timestamps.add("1454371500");
				timestamps.add("1454457600");
				timestamps.add("1454457900");

				for (String ts : timestamps)
				{
					data.put(ts, new TreeMap<String, Long>());
					data.get(ts).put("1", 1L);
					data.get(ts).put("2", 2L);
					data.get(ts).put("3", 3L);
				}

				jsonObjectTree.put("data", data);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				log.debug("expectation as json string: " + expected);
				log.debug("actual result as json string: " + response);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				assertTrue(e.getMessage(), false);
			}

			//
			// fetch only subcol 2
			//
			try
			{
				query = "start=1454284800&end=1459469101";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				log.debug("response message is " + conn.getResponseMessage());
				response = getResponse(conn);
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 6);
				metaData.put("has_more", false);
				metaData.put("cake", Boolean.FALSE);

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();

				ArrayList<String> timestamps = new ArrayList<String>();
				timestamps.add("1454284800");
				timestamps.add("1454285100");
				timestamps.add("1454371200");
				timestamps.add("1454371500");
				timestamps.add("1454457600");
				timestamps.add("1454457900");

				for (String ts : timestamps)
				{
					data.put(ts, new TreeMap<String, Long>());
					data.get(ts).put("2", 2L);
				}

				jsonObjectTree.put("data", data);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				log.debug("expectation as json string: " + expected);
				log.debug("actual result as json string: " + response);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.toString(), false);
			}

			//
			// now throw some curve balls
			//
			// 1.  missing start
			// 2.  missing end
			// 3.  missing both
			// 4.  missing ks
			// 5.  missing cf
			// 6.  too few fields in general
			// 7.  query such that the results returned is empty
			// 8.  query a range that is too big, and would cause many many cassandra hits
			//
			try
			{
				log.debug("Sending request with no start");
				query = "end=10000000000";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"start or end value did not convert to a Long properly"));
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with bad start value");
				query = "start=foobar";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"start or end value did not convert to a Long properly"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with no end");
				query = "start=0";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"start or end value did not convert to a Long properly"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with no query string");
				query = "";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct",
					   conn.getResponseMessage().contains("missing query string"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with no keyspace");
				query = "start=0&end=10000000000";
				u = new URL(
					new URI("http", null, "localhost", 19913, "//rollup5m/" + RowName + "/2", query,
						null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"keyspace was not specified or had a string length of 0"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with no CF");
				query = "start=0&end=10000000000";
				u = new URL(new URI("http", null, "localhost", 19913, "/" + ks + "//" + RowName + "/2",
						    query, null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"column family was not specified or had a string length of 0"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request with too few fields");
				query = "start=0&end=10000000000";
				u = new URL(
					new URI("http", null, "localhost", 19913, "/rollup5m/" + RowName + "/", query,
						null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct", conn.getResponseMessage().contains(
					"Too few fields in the URI to be a valid API request"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending the strange");
				query = "start=0&end=10000000000";
				u = new URL(
					new URI("http", null, "localhost", 19913, "/rollup5m/" + RowName + "/2", query,
						null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 400, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 400);
				assertTrue("response isn't correct",
					   conn.getResponseMessage().contains("unknown cf: row"));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			try
			{
				log.debug("Sending request for something that shouldn't match anything");
				query = "start=100&end=200";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				response = null;
				try
				{
					response = getResponse(conn);
				}
				catch (Exception e)
				{
				}
				log.debug("response message is " + conn.getResponseMessage());
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 0);
				metaData.put("has_more", false);
				metaData.put("cake", Boolean.FALSE);

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();
				jsonObjectTree.put("data", data);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				log.debug("metaData as json string: " + expected);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.getMessage(), false);
			}

			//
			// submit a query that would do more queries than the queries_per_request limit
			// to trigger the queries per request protection reaction
			//
			try
			{
				log.debug("Sending a query for a range that would cause too many queries");
				query = "start=1454284800&end=10000000000";
				u = new URL(new URI("http", null, "localhost", 19913,
						    "/" + ks + "/rollup5m/" + RowName + "/2", query,
						    null).toASCIIString());
				conn = (HttpURLConnection) u.openConnection();
				conn.setRequestProperty("CassandraAPIVersion", "1");
				conn.connect();
				log.debug("response message is " + conn.getResponseMessage());
				response = getResponse(conn);
				log.debug("response is " + response);
				assertTrue("expected 200, but instead got " + conn.getResponseCode(),
					   conn.getResponseCode() == 200);

				mapper = new ObjectMapper();
				TreeMap<String, Object> jsonObjectTree = new TreeMap<String, Object>();
				TreeMap<String, Object> metaData = new TreeMap<String, Object>();

				metaData.put("version", 1);
				metaData.put("count", 6);
				metaData.put("has_more", true);
				metaData.put("cake", Boolean.FALSE);
				metaData.put("next_start",
					     "1463011200");	// 100 days after the row timestamp for the start parameter

				jsonObjectTree.put("metadata", metaData);

				TreeMap<String, Map<String, Long>> data = new TreeMap<String, Map<String, Long>>();

				ArrayList<String> timestamps = new ArrayList<String>();
				timestamps.add("1454284800");
				timestamps.add("1454285100");
				timestamps.add("1454371200");
				timestamps.add("1454371500");
				timestamps.add("1454457600");
				timestamps.add("1454457900");

				for (String ts : timestamps)
				{
					data.put(ts, new TreeMap<String, Long>());
					data.get(ts).put("2", 2L);
				}

				jsonObjectTree.put("data", data);

				String expected = mapper.writeValueAsString(jsonObjectTree);
				log.debug("expectation as json string: " + expected);
				log.debug("actual result as json string: " + response);

				assertTrue("response not as expected: " + response, response.equals(expected));
				conn.disconnect();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				assertTrue(e.toString(), false);
			}

			server.stop();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			assertTrue(false);
		}
	}

	String getResponse(@NotNull HttpURLConnection conn) throws Exception
	{
		byte[] buffer = new byte[1024];
		StringBuilder sb = new StringBuilder();
		BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
		while (in.read(buffer, 0, buffer.length) != -1)
		{
			sb.append(new String(buffer).trim());
			Arrays.fill(buffer, (byte) 0);
		}
		in.close();
		return sb.toString();
	}

	void printHeaders(@NotNull HttpURLConnection conn)
	{
		Map<String, List<String>> headers = conn.getHeaderFields();
		for (String key1 : headers.keySet())
		{
			if (null == key1)
			{
				continue;
			}

			for (String value : headers.get(key1))
			{
				if (null == value)
				{
					continue;
				}
				try
				{
					if (log.isDebugEnabled())
					{
						log.debug(key1 + " = " + value);
					}
				}
				catch (NullPointerException e)
				{
				}
			}
		}
	}
}
