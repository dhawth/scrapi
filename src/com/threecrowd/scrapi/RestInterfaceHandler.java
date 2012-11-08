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

import com.threecrowd.*;
import com.threecrowd.scrapi.models.*;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.net.URLDecoder;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.JmxMonitor;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.beans.CounterSuperSlice;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SuperSliceCounterQuery;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class RestInterfaceHandler extends AbstractHandler
{
	@NotNull
	private final Logger log = Logger.getLogger(RestInterfaceHandler.class);
	@NotNull
	private final StatsObject so = StatsObject.getInstance();
	@NotNull
	private final HashMap<Long, String> usage = new HashMap<Long, String>();	// version (e.g. 1) => usage string

	//
	// variables related to hector/cassandra
	//

	@Nullable
	private Cluster cluster = null;
	@Nullable
	private Keyspace keyspace = null;
	@Nullable
	private String keyspace_string = null;
	@NotNull
	static final private StringSerializer se = new StringSerializer();
	@NotNull
	static final private LongSerializer le = new LongSerializer();
	@Nullable
	private NodeConfig config = null;

	@NotNull
	private static final ThreadLocal<Gson> gsonTL =
		new ThreadLocal<Gson>()
		{
			@NotNull
			@Override
			protected Gson initialValue()
			{
				return new Gson();
			}
		};

	@NotNull
	private static final ThreadLocal<Calendar> calendarTL =
		new ThreadLocal<Calendar>()
		{
			@NotNull
			@Override
			protected Calendar initialValue()
			{
				return new GregorianCalendar(TimeZone.getTimeZone("UTC"));
			}
		};

	@NotNull
	private static final ThreadLocal<ObjectMapper> mapperTL =
		new ThreadLocal<ObjectMapper>()
		{
			@NotNull
			@Override
			protected ObjectMapper initialValue()
			{
				return new ObjectMapper();
			}
		};

	@NotNull
	private static final ThreadLocal<byte[]> byteArrayBuffer =
		new ThreadLocal<byte[]>()
		{
			@NotNull
			@Override
			protected byte[] initialValue()
			{
				return new byte[8192];
			}
		};


	RestInterfaceHandler(@NotNull final NodeConfig c) throws Exception
	{
		//noinspection ConstantConditions
		if (null == c)
		{
			throw new IllegalArgumentException(
				"Cannot initialize RestInterfaceHandler with a null config argument");
		}

		config = c;
		usage.put((long) 1, "usage: GET /keyspace/column_family/row[/subcolum_name][/?start=X&end=Y]");
	}

	public void handle(final String target,
			   @NotNull final Request baseRequest,
			   final HttpServletRequest httpRequest,
			   @NotNull final HttpServletResponse httpResponse) throws IOException, ServletException
	{
		int status = 403;
		HashMap<String, String> log_fields = new HashMap<String, String>();
		OutputStream out = httpResponse.getOutputStream();

		try
		{
			if (null == out)
			{
				throw new IOException("null output stream");
			}

			TreeMap<String, Object> metaData = new TreeMap<String, Object>();

			// decode the URI to determine the command requested.
			// we decode the URI because the row names can have spaces
			String uri = URLDecoder.decode(baseRequest.getRequestURI(), "UTF-8");	// /path/too/foo
			String method = baseRequest.getMethod();		// GET, POST, etc
			String queryString = baseRequest.getQueryString();	// ?(.*)

			log_fields.put("uri", uri);
			log_fields.put("query_string", queryString);
			log_fields.put("method", method);
			log_fields.put("body_length", "0");

			//
			// shouldn't need the message body for anything, but read it in and dispose of it anyway.
			//
			InputStream in = baseRequest.getInputStream();
			try
			{
				byte[] buffer = byteArrayBuffer.get();
				long length = 0;
				int len = 0;
				//noinspection NestedAssignment
				while ((len = in.read(buffer)) > 0)
				{
					length += len;
				}
				log_fields.put("body_length", Long.toString(length));
				so.update(StatsObject.ValueType.SUM, "RestInterfaceHandler.total_input_bytes_read",
					  length);
			}
			catch (IOException e)
			{
				log.debug(e.toString());
			}
			finally
			{
				in.close();
			}

			so.update(StatsObject.ValueType.SUM, "RestInterfaceHandler.total_hits", 1);

			//
			// switch here on the method, then based on the uri components.
			//

			if (uri.startsWith("/"))
			{
				uri = uri.substring(1);
			}
			log.debug("uri is " + uri);

			//
			// this block exists solely so we can break out of it once we're done
			// handling the request.  I did this instead of calling return so we could
			// log all the requests in one place instead of all over the place
			//
			do
			{
				so.update(StatsObject.ValueType.SUM,
					  "RestInterfaceHandler.total_hits_for_method_" + method, 1);
				if (method.equals("GET"))
				{
					String[] uri_fields = uri.split("/");

					// GET operations node status, configuration, anything from cassy
					if (uri_fields[0].equals("node"))
					{
						if (uri_fields[1].equals("stats"))
						{
							so.update(StatsObject.ValueType.SUM,
								  "RestInterfaceHandler.total_node_stats_requests", 1);
							HashMap<String, Long> statsMap = so.getMap();
							HashMap<String, String> stringMap = new HashMap<String, String>();
							for (String key : statsMap.keySet())
							{
								stringMap.put(key, statsMap.get(key).toString());
							}
							sendHashMap(httpResponse, stringMap, 1);
							status = 200;

						}
						else if (uri_fields[1].equals("config"))
						{
							so.update(StatsObject.ValueType.SUM,
								  "RestInterfaceHandler.total_node_config_requests", 1);
							sendString(httpResponse, config.toString());
							status = 200;
						}
						else
						{
							httpResponse.sendError(500, "unsupported GET operation");
							status = 500;
						}

						break;
					}

					String versionString = baseRequest.getHeader("CassandraAPIVersion");

					//
					// set a default version of 1 to be nice
					//
					if (null == versionString && queryString != null && !queryString.equals(""))
					{
						for (String kv : queryString.split("&"))
						{
							if (kv.contains("="))
							{
								String[] kv_pair = kv.split("=");
								if (kv_pair.length == 2)
								{
									if (kv_pair[0].equals("CassandraAPIVersion"))
									{
										versionString = kv_pair[1];
									}
								}
							}
						}
					}

					log_fields.put("api_version", versionString);


					//
					// Version 1 API:
					//

					//
					// else there MUST be a CassandraAPIVersion set
					//
					if (versionString == null)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.GET_requests_with_no_api_version", 1);
						httpResponse.sendError(400, "Unsupported Cassandra API Version");
						status = 400;
						break;
					}

					Long version = null;

					try
					{
						version = Long.parseLong(versionString);
						metaData.put("version", version);
					}
					catch (NumberFormatException e)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.GET_requests_with_NFE_api", 1);
						httpResponse.sendError(400, "Unsupported Cassandra API Version");
						status = 400;
						break;
					}

					//
					// so we can watch the stats logfile and see what people are doing with each
					// API version
					//
					so.update(StatsObject.ValueType.SUM,
						  "RestInterfaceHandler.total_hits_for_api_version_" + version, 1);
					so.update(StatsObject.ValueType.SUM,
						  "RestInterfaceHandler.total_" + method + "_hits_for_api_version_" + version,
						  1);

					if (1 != version)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_GET_requests_for_unsupported_api",
							  1);
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_GET_requests_for_unsupported_api_version_" + version,
							  1);
						httpResponse.sendError(400, "Unsupported Cassandra API Version");
						status = 400;
						break;
					}

					if (uri_fields[0].equals("usage"))
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_usage_requests_for_api_version_" + version,
							  1);
						out.write(usage.get(version).getBytes());
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bytes_sent",
							  usage.get(version).getBytes().length);
						status = 200;
						break;
					}

					//
					// we require a keyspace, column family, row name minimum
					//
					if (uri_fields.length < 3)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "Too few fields in the URI to be a valid API request");
						status = 400;
						break;
					}

					if (queryString == null || queryString.equals(""))
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400, "missing query string");
						status = 400;
						break;
					}

					String ks = uri_fields[0];
					String cf = uri_fields[1];
					String row = uri_fields[2];
					String subcol = null;
					String start = null;
					String end = null;
					boolean reversed = false;
					int limit = 1000;

					if (uri_fields.length == 4)
					{
						subcol = uri_fields[3];
					}

					for (String kv : queryString.split("&"))
					{
						if (kv.contains("="))
						{
							String[] kv_pair = kv.split("=");
							if (kv_pair.length == 2)
							{
								if (kv_pair[0].equals("start"))
								{
									start = kv_pair[1];
								}
								else if (kv_pair[0].equals("end"))
								{
									end = kv_pair[1];
								}
								else if (kv_pair[0].equals("limit"))
								{
									limit = Integer.parseInt(kv_pair[1]);
									if (limit > 1000)
									{
										so.update(StatsObject.ValueType.SUM,
											  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
											  1);
										httpResponse.sendError(400,
												       "limit must be <= 1000");
										status = 400;
										break;
									}
								}
								else if (kv_pair[0].equals("reverse") &&
									kv_pair[1].equals("true"))
								{
									reversed = true;
								}
							}
							else
							{
								// silent ignore if it's missing the = and value
							}
						}
						else
						{
							//
							// in case we ever want to have things like &flatten&etc
							// where there's no k=v format to the argument
							//
						}
					}

					if (ks.length() == 0)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "keyspace was not specified or had a string length of 0\n" + usage.get(
									       version));
						status = 400;
						break;
					}

					if (cf.length() == 0)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "column family was not specified or had a string length of 0\n" + usage.get(
									       version));
						status = 400;
						break;
					}

					if (row.length() == 0)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "row was not specified or had a string length of 0\n" + usage.get(
									       version));
						status = 400;
						break;
					}

					if (subcol != null && subcol.length() == 0)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "sub column was specified but it had a string length of 0\n" + usage.get(
									       version));
						status = 400;
						break;
					}

					Long startLong = null;
					Long endLong = null;

					try
					{
						startLong = Long.parseLong(start);
						endLong = Long.parseLong(end);
					}
					catch (NumberFormatException e)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "start or end value did not convert to a Long properly");
						status = 400;
						break;
					}

					if (startLong > endLong)
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_bad_requests_for_api_version_" + version,
							  1);
						httpResponse.sendError(400,
								       "column range " + start + " - " + end + " is an error, I detected that start is > end\n" +
									       usage.get(version));
						status = 400;
						break;
					}

					if (!config.column_families.containsKey(cf))
					{
						so.update(StatsObject.ValueType.SUM,
							  "RestInterfaceHandler.total_requests_for_unknown_cf", 1);
						httpResponse.sendError(400, "unknown cf: " + cf);
						status = 400;
						break;
					}

					//
					// dump out a debug line
					// this is efficient because we're using slf4j
					//
					if (log.isDebugEnabled())
					{
						HashMap<String, String> args = new HashMap<String, String>();
						args.put("keyspace", ks);
						args.put("cf", cf);
						args.put("row", row);
						args.put("subcol", subcol);
						args.put("start", start);
						args.put("end", end);
						args.put("reversed", Boolean.toString(reversed));
						args.put("limit", Long.toString(limit));
						log.debug("arguments: " + args.toString());
					}

					//
					// put together a cassandra request
					//
					if (cluster == null)
					{
						log.debug("cluster is null, fetching cluster");

						CassandraHostConfigurator cassandraHostConfigurator =
							new CassandraHostConfigurator(config.cassandra_host);

						//
						// The maximum amount of time to wait if there are no clients available
						//
						cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(200);

						//
						// max timeout for any operation
						//
						cassandraHostConfigurator.setCassandraThriftSocketTimeout(2000);

						//
						// default time to wait between attempts to contact a known down server again
						//
						cassandraHostConfigurator.setRetryDownedHostsDelayInSeconds(1);

						//
						// max number of connections to hold open to this host
						//
						cassandraHostConfigurator.setMaxActive(20);

						//
						// user-specified overrides from the config file
						//
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

							if (config.cassandra_config.connection_retry_time != null)
							{
								cassandraHostConfigurator.setRetryDownedHostsDelayInSeconds(
									config.cassandra_config.connection_retry_time);
							}

						}

						log.debug("hostconfigurator: " + cassandraHostConfigurator);
						cluster = HFactory.getOrCreateCluster("CassandraCluster",
										      cassandraHostConfigurator);
						log.debug("successfully got cluster");
					}

					if (!ks.equals(keyspace_string))
					{
						log.debug("getting keyspace for keyspace " + ks);
						keyspace_string = ks;
						keyspace = HFactory.createKeyspace(ks, cluster);
						log.debug("successfully created keyspace");
					}

					//
					// results[super column name] = array(column name => column value)
					// this map stores the results for a given timestamp, the value of which
					// is stored in last_ts.  it is shipped whole when we see the next
					// timestamp.
					//
					TreeMap<String, Long> results = new TreeMap<String, Long>();

					metaData.put("has_more", Boolean.FALSE);
					metaData.put("cake", false);

					long startRowTimestamp = getRowTimestamp(cf, startLong);
					long endRowTimestamp = getRowTimestamp(cf, endLong);

					if (log.isDebugEnabled())
					{
						log.debug(
							"for time range [" + startLong + " - " + endLong + "], I have row timestamps: " +
								"[" + startRowTimestamp + " - " + endRowTimestamp + "]");
					}

					Composite startRange = new Composite();
					Composite endRange = new Composite();

					startRange.add(0, startLong);

					int num_results = 0;
					Long ts = 0L;
					Long lastTs = null;
					boolean has_more = false;
					long count = 0;
					int num_rows_queried = 0;

					ObjectMapper mapper = mapperTL.get();

					//
					// send opening '{ "data" : {'
					//
					out.write("{\"data\":{".getBytes());

					if (reversed)
					{
						//
						// +1 to end because the end of the range is exclusive
						//
						endRange.add(0, endLong + 1);

						//
						// query backwards, from end to start, by decrementing endRowTimestamp until it
						// equals startRowTimestamp
						//
						// foreach query:
						//	foreach column / ts:
						//		if ts != previous ts 
						//			if there are results to send,
						//				send them
						//			if the number of timestamps seen matches the limit
						//				of how many we will return,
						//				break;
						//	send whatever results are left over
						//	send metadata
						//
						// times to break out of a query:
						//	user-supplied limit of how many timestamps to fetch has been reached
						//	system limit on how many queries to execute (rows to ask for) has been reached
						//	a ts < start_ts has been reached
						//
ROW_QUERY_LOOP:

						while (endRowTimestamp >= startRowTimestamp &&
							num_rows_queried++ < config.cassandra_config.queries_per_request)
						{
							if (log.isDebugEnabled())
							{
								log.debug(
									"querying for row timestamp: " + endRowTimestamp + ", lastTs = " + lastTs +
										", count = " + count + ", num_rows_queried = " + num_rows_queried);
							}

							String localRow = row + "/" + endRowTimestamp;

							CompositeQueryIterator iter =
								new CompositeQueryIterator(keyspace, cf, localRow,
											   startRange, endRange, true);

							for (HColumn<Composite, Long> column : iter)
							{
								ts = column.getName().get(0, le);

								if (ts < startLong)
								{
									if (log.isDebugEnabled())
									{
										log.debug(
											"leaving query loop because ts " + ts + " is < the start param (" + startLong + ")");
									}
									break ROW_QUERY_LOOP;
								}

								//
								// if we've found the next timestamp, send the results for the previous one
								// assuming there are any for the previous one, and that there was a previous
								// one.
								//
								if (lastTs != null && !ts.equals(
									lastTs) && results.size() > 0)
								{
									log.debug(
										"sending results because we found a new timestamp: " + ts + " != " + lastTs);

									if (count++ > 0)
									{
										//
										// we've already sent one result timestamp hashmap,
										// so we need to prepend this one with a comma
										//
										out.write(",".getBytes());
									}

									out.write("\"".getBytes());
									out.write(lastTs.toString().getBytes());
									out.write("\":".getBytes());

									//
									// send "{ key: value, key: value, ... }"
									//
									out.write(mapper.writeValueAsString(
										results).getBytes());
									results.clear();

									if (count == limit)
									{
										break ROW_QUERY_LOOP;
									}
								}
								//
								// end of send results if this timestamp != previous timestamp
								//

								lastTs = ts;

								//
								// if a subcolumn was specified and this column doesn't match it, skip
								// to the next column
								//
								if (subcol != null && !subcol.equals(
									column.getName().get(1, se)))
								{
									continue;
								}

								results.put(column.getName().get(1, se),
									    column.getValue());
							}
							//
							// end of foreach column in row
							//

							//
							// we've reached the end of this row, send results that we have
							//
							if (results.size() > 0)
							{
								if (count++ > 0)
								{
									//
									// we've already sent one result timestamp hashmap,
									// so we need to prepend this one with a comma
									//
									out.write(",".getBytes());
								}

								out.write("\"".getBytes());
								out.write(lastTs.toString().getBytes());
								out.write("\":".getBytes());

								//
								// send "{ key: value, key: value, ... }"
								//
								out.write(
									mapper.writeValueAsString(results).getBytes());
								results.clear();
							}

							//
							// update endRowTimestamp to point to the previous month
							// because we're querying backwards
							//
							endRowTimestamp = getPreviousRowTimestamp(cf, endRowTimestamp);
						}
						//
						// end of foreach row
						//

						//
						// send any results left over
						//
						if (results.size() > 0)
						{
							if (count++ > 0)
							{
								//
								// we've already sent one result timestamp hashmap,
								// so we need to prepend this one with a comma
								//
								out.write(",".getBytes());
							}

							out.write("\"".getBytes());
							out.write(lastTs.toString().getBytes());
							out.write("\":".getBytes());

							//
							// send "{ key: value, key: value, ... }"
							//
							out.write(mapper.writeValueAsString(results).getBytes());
							results.clear();
						}

						//
						// if we've reached the limit on the number of results we can send
						// we need to send the client the ts to use for the next query, which
						// should always be the same ts we stopped with this time.
						// next time we'll +1 it when we set up the Composite range and we'll
						// just send the same results a second time.
						// ... OR if we've reached the user-supplied limit, also set has_more to
						// true and tell them where to start the next query at (in terms of the
						// end, since this is reversed)
						//
						if (count == limit)
						{
							has_more = true;
							metaData.put("next_end", Long.toString(lastTs - 1));
						}
						else if (num_rows_queried > config.cassandra_config.queries_per_request)
						{
							has_more = true;

							metaData.put("next_start", startLong.toString());

							if (null == lastTs || endRowTimestamp < lastTs)
							{
								metaData.put("next_end",
									     Long.toString(endRowTimestamp - 1));
							}
							else
							{
								metaData.put("next_end", Long.toString(lastTs - 1));
							}
						}
					}

					//
					// end of reversed query logic
					//

					else
					{
						//
						// +1 to end because the end of the range is always exclusive
						//
						endRange.add(0, endLong + 1);

						//
						// query forwards, from start to end, by incrementing startRowTimestamp until it
						// equals endRowTimestamp
						//
						// foreach query:
						//	foreach column / ts:
						//		if ts != previous ts and there are results to send, send them
						//	send whatever results are left over
						//	send metadata
						//
						// times to break out of a query:
						//	user-supplied limit of how many timestamps to fetch has been reached
						//	system limit on how many queries to execute (rows to ask for) has been reached
						//	a ts > end_ts has been reached (which btw isn't possible with
						//		composite column range queries)
						//
ROW_QUERY_LOOP:

						while (startRowTimestamp <= endRowTimestamp &&
							num_rows_queried++ < config.cassandra_config.queries_per_request)
						{
							if (log.isDebugEnabled())
							{
								log.debug(
									"querying for row timestamp: " + startRowTimestamp + ", lastTs = " + lastTs +
										", count = " + count + ", num_rows_queried = " + num_rows_queried);
							}

							String localRow = row + "/" + startRowTimestamp;

							CompositeQueryIterator iter =
								new CompositeQueryIterator(keyspace, cf, localRow,
											   startRange, endRange, false);

							for (HColumn<Composite, Long> column : iter)
							{
								ts = column.getName().get(0, le);

								if (ts > endLong)
								{
									if (log.isDebugEnabled())
									{
										log.debug(
											"leaving query loop because ts " + ts + " is > the end param (" + endLong + ")");
									}
									break ROW_QUERY_LOOP;
								}

								//
								// if we've found the next timestamp, send the results for the previous one
								// assuming there are any for the previous one, and that there was a previous
								// one.
								//
								if (lastTs != null && !ts.equals(
									lastTs) && results.size() > 0)
								{
									log.debug(
										"sending results because we found a new timestamp: " + ts + " != " + lastTs);

									if (count++ > 0)
									{
										//
										// we've already sent one result timestamp hashmap,
										// so we need to prepend this one with a comma
										//
										out.write(",".getBytes());
									}

									out.write("\"".getBytes());
									out.write(lastTs.toString().getBytes());
									out.write("\":".getBytes());

									//
									// send "{ key: value, key: value, ... }"
									//
									out.write(mapper.writeValueAsString(
										results).getBytes());
									results.clear();

									if (count == limit)
									{
										break ROW_QUERY_LOOP;
									}
								}
								//
								// end of send results if this timestamp != previous timestamp
								//

								lastTs = ts;

								//
								// if a subcolumn was specified and this column doesn't match it, skip
								// to the next column
								//
								if (subcol != null && !subcol.equals(
									column.getName().get(1, se)))
								{
									continue;
								}

								results.put(column.getName().get(1, se),
									    column.getValue());
							}
							//
							// end of foreach column in row
							//

							//
							// we've reached the end of this row, send results that we have
							//
							if (results.size() > 0)
							{
								if (count++ > 0)
								{
									//
									// we've already sent one result timestamp hashmap,
									// so we need to prepend this one with a comma
									//
									out.write(",".getBytes());
								}

								out.write("\"".getBytes());
								out.write(lastTs.toString().getBytes());
								out.write("\":".getBytes());

								//
								// send "{ key: value, key: value, ... }"
								//
								out.write(
									mapper.writeValueAsString(results).getBytes());
								results.clear();
							}

							//
							// update startRowTimestamp to point at the row for the next month
							//
							startRowTimestamp = getNextRowTimestamp(cf, startRowTimestamp);
						}
						//
						// end of foreach row
						//

						//
						// send any results left over
						//
						if (results.size() > 0)
						{
							if (count++ > 0)
							{
								//
								// we've already sent one result timestamp hashmap,
								// so we need to prepend this one with a comma
								//
								out.write(",".getBytes());
							}

							out.write("\"".getBytes());
							out.write(lastTs.toString().getBytes());
							out.write("\":".getBytes());

							//
							// send "{ key: value, key: value, ... }"
							//
							out.write(mapper.writeValueAsString(results).getBytes());
							results.clear();
						}

						//
						// if we've reached the limit on the number of results we can send
						// (we can no longer reach the limit of the number of queries per
						// request because they are hidden from us by hector's Slice Iterator)
						// we need to send the client the ts to use for the next query, which
						// should always be the same ts we stopped with this time.
						// next time we'll +1 it when we set up the Composite range and we'll
						// just send the same results a second time.
						// ... OR if we've reached the user-supplied limit, also set has_more to
						// true and tell them where to start the next query at (in terms of the
						// end, since this is reversed)
						//
						if (count == limit)
						{
							has_more = true;
							metaData.put("next_start", Long.toString(lastTs + 1));
						}
						else if (num_rows_queried > config.cassandra_config.queries_per_request)
						{
							has_more = true;

							//
							// let's say we have rows for ts 0 - 100 but our query went far
							// beyond that before it reached the queries_per_request limit.
							// we don't want to backtrack and re-do the 100 - limit queries
							// a second time, and we already know there's no data up to
							// the current startRowTimestamp, so we wend that
							//

							//
							// if we found no data in $limit queries, we send the next start param
							// as the next startRowTimestamp
							// if the startRowTimestamp is > the lastTs found, that just means we
							// found some data for older timestamps but there were gaps and we're
							// in the middle of processing a gap, so also send the next startRowTimestamp
							// else we aren't in the middle of a gap and we do have prior data and
							// we just reached a limit in the number of timestamps we're allowed to
							// return so just say to query the next timestamp (lastTs + 1).
							//

							if (null == lastTs || startRowTimestamp > lastTs)
							{
								metaData.put("next_start", Long.toString(
									getNextRowTimestamp(cf, startRowTimestamp)));
							}
							else
							{
								metaData.put("next_start", Long.toString(lastTs + 1));
							}
						}
					}

					//
					// end of not-reversed query logic
					//

					//
					// send closing } at the end of data section and ,
					//
					out.write("},\"metadata\":".getBytes());

					if (has_more)
					{
						metaData.put("has_more", Boolean.TRUE);
					}
					else
					{
						metaData.put("has_more", Boolean.FALSE);
					}
					metaData.put("count", count);

					//
					// send metaData as hashmap
					//
					out.write(mapper.writeValueAsString(metaData).getBytes());

					//
					// send closing } at end of answer
					//
					out.write("}".getBytes());

					//
					// results may be empty, that's ok.
					//
					status = 200;
					break;
				}
				else
				{
					// errrr, dunno what to do with this request.
					httpResponse.sendError(500, "unknown HTTP method " + method);
					status = 500;
					break;
				}
			}
			while (false);
			//
			// end of poor-man's goto
			//
		}
		catch (Exception e)
		{
			log.error("Error doing the needful: " + e.getMessage());
			//
			// in any event, log the message to the log file.
			//
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw, true);
			e.printStackTrace(pw);
			pw.flush();
			sw.flush();
			log.error(sw.toString());

			httpResponse.sendError(500, e.getMessage());
			status = 500;
		}
		finally
		{
			so.update(StatsObject.ValueType.SUM,
				  "RestInterfaceHandler.total_responses_with_status_code_" + status, 1);
			log_fields.put("return_status", Integer.toString(status));

			if (200 == status)
			{
				httpResponse.setStatus(status);
			}

			baseRequest.setHandled(true);

			//
			// Log here
			//
			log.debug("access_log: " + log_fields.toString());

			//
			// close the output stream if it is still opened
			//
			if (out != null)
			{
				try
				{
					out.close();
				}
				catch (IOException x)
				{
					log.debug(x.toString());
				}
			}
		}

	}

	private void sendResults(@NotNull final HttpServletResponse httpResponse, @NotNull final TreeMap results, final TreeMap metaData)
	{
		OutputStream out = null;
		log.debug("Sending map: " + results.toString());
		try
		{
			ObjectMapper mapper = mapperTL.get();
			TreeMap<String, Object> jsonObject = new TreeMap<String, Object>();

			jsonObject.put("metadata", metaData);
			jsonObject.put("data", results);

			String jsonData = mapper.writeValueAsString(jsonObject);
			log.debug("json data to send: " + jsonData);

			out = httpResponse.getOutputStream();
			if (out == null)
			{
				log.debug("null output stream???");
			}
			else
			{
				out.write(jsonData.getBytes());
				out.write("\n".getBytes());
				so.update(StatsObject.ValueType.SUM, "RestInterfaceHandler.total_bytes_sent",
					  jsonData.getBytes().length + 1);
			}
		}
		catch (IOException e)
		{
			log.error(e.toString());
		}
		finally
		{
			if (out != null)
			{
				try
				{
					out.close();
				}
				catch (IOException x)
				{
					log.debug(x.toString());
				}
			}
		}
	}

	private void sendHashMap(@NotNull final HttpServletResponse httpResponse, @NotNull final HashMap map, final int depth)
	{
		OutputStream out = null;
		Gson gson = gsonTL.get();
		log.debug("Sending map: " + map.toString());
		try
		{
			String jsonData = null;
			if (depth == 1)
			{
				jsonData = gson.toJson(map, new TypeToken<HashMap<String, String>>()
				{
				}.getType());
			}
			else if (depth == 2)
			{
				jsonData = gson.toJson(map, new TypeToken<HashMap<String, HashMap<String, String>>>()
				{
				}.getType());
			}
			else if (depth == 3)
			{
				jsonData = gson.toJson(map,
						       new TypeToken<HashMap<String, HashMap<String, HashMap<String, String>>>>()
						       {
						       }.getType());
			}
			if (jsonData != null)
			{
				log.debug("json data to send: " + jsonData);
				out = httpResponse.getOutputStream();
				if (out == null)
				{
					log.debug("null output stream???");
				}
				else
				{
					out.write(jsonData.getBytes());
					so.update(StatsObject.ValueType.SUM, "RestInterfaceHandler.total_bytes_sent",
						  jsonData.getBytes().length);

				}
			}
			else
			{
				log.debug("null json data");
			}
		}
		catch (IOException e)
		{
			log.error(e.toString());
		}
		finally
		{
			if (out != null)
			{
				try
				{
					out.close();
				}
				catch (IOException x)
				{
					log.debug(x.toString());
				}
			}
		}
	}

	private void sendString(@NotNull final HttpServletResponse httpResponse, @NotNull final String contents)
	{
		OutputStream out = null;
		try
		{
			out = httpResponse.getOutputStream();
			if (out == null)
			{
				log.debug("null output stream???");
			}
			else
			{
				out.write(contents.getBytes());
				so.update(StatsObject.ValueType.SUM, "RestInterfaceHandler.total_bytes_sent",
					  contents.getBytes().length);
			}
		}
		catch (IOException e)
		{
			log.error(e.toString());
		}
		finally
		{
			if (out != null)
			{
				try
				{
					out.close();
				}
				catch (IOException x)
				{
					log.debug(x.toString());
				}
			}
		}
	}

	private long getRowTimestamp(final String cf, final long ts)
	{
		Calendar calendar = calendarTL.get();

		//
		// set initial time
		//
		calendar.setTimeInMillis(ts * 1000);

		//
		// strip out HMS
		//
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);

		if ("rollup5m".equals(cf))
		{
		}
		else if ("rollup1h".equals(cf))
		{
			calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		}
		else if ("rollup1d".equals(cf))
		{
			calendar.set(Calendar.DAY_OF_MONTH, 1);
			int month = calendar.get(Calendar.MONTH);
			calendar.set(Calendar.MONTH, month - (month % 4));
		}
		else if ("rollup1M".equals(cf))
		{
			calendar.set(Calendar.DAY_OF_YEAR, 1);
			int year = calendar.get(Calendar.YEAR);
			calendar.set(calendar.YEAR, year - (year % 10));
		}
		else
		{
			throw new IllegalArgumentException("unknown cf: " + cf);
		}

		if (log.isDebugEnabled())
		{
			log.debug(
				"getRowTimestamp converted ts " + ts + " into row ts " + (calendar.getTimeInMillis() / 1000) +
					" for cf: " + cf);
		}

		return calendar.getTimeInMillis() / 1000;
	}

	private long getNextRowTimestamp(final String cf, final long previousRowTs)
	{
		Calendar calendar = calendarTL.get();

		//
		// set initial time
		//
		calendar.setTimeInMillis(previousRowTs * 1000);

		if ("rollup5m".equals(cf))
		{
			calendar.add(Calendar.HOUR, 24);
		}
		else if ("rollup1h".equals(cf))
		{
			calendar.add(Calendar.HOUR, 7 * 24);
		}
		else if ("rollup1d".equals(cf))
		{
			calendar.add(Calendar.MONTH, 4);
		}
		else if ("rollup1M".equals(cf))
		{
			calendar.add(Calendar.YEAR, 10);
		}
		else
		{
			throw new IllegalArgumentException("unknown cf: " + cf);
		}

		if (log.isDebugEnabled())
		{
			log.debug("nextRowTimestamp for cf " + cf + " from ts " + previousRowTs +
					  " is: " + (calendar.getTimeInMillis() / 1000));
		}

		return calendar.getTimeInMillis() / 1000;
	}

	private long getPreviousRowTimestamp(final String cf, final long currentRowTs)
	{
		Calendar calendar = calendarTL.get();

		//
		// set initial time
		//
		calendar.setTimeInMillis(currentRowTs * 1000);

		if ("rollup5m".equals(cf))
		{
			calendar.add(Calendar.HOUR, -24);
		}
		else if ("rollup1h".equals(cf))
		{
			calendar.add(Calendar.HOUR, -7 * 24);
		}
		else if ("rollup1d".equals(cf))
		{
			calendar.add(Calendar.MONTH, -4);
		}
		else if ("rollup1M".equals(cf))
		{
			calendar.add(Calendar.YEAR, -10);
		}
		else
		{
			throw new IllegalArgumentException("unknown cf: " + cf);
		}

		if (log.isDebugEnabled())
		{
			log.debug("previousRowTimestamp for cf " + cf + " from ts " + currentRowTs +
					  " is: " + (calendar.getTimeInMillis() / 1000));
		}

		return calendar.getTimeInMillis() / 1000;
	}


}
