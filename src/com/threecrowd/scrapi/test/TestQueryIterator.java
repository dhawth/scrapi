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

//
// This is an example for how to use the column slice iterator and composite query iterator
//

import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
import java.util.UUID;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Iterator;
import java.util.HashSet;

//
// jackson json parser
//
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;

import com.threecrowd.*;

// these are the includes from the test keyspace file
// import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;

import me.prettyprint.cassandra.service.ThriftColumnDef;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;

import me.prettyprint.hector.api.beans.*;
import me.prettyprint.cassandra.model.HColumnImpl;

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
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ResultStatus;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;


import org.codehaus.jackson.map.ObjectMapper;

import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class TestQueryIterator extends TestCase
{
	@Nullable
	private Logger log = null;
	private Cluster cluster;
	@NotNull
	static final private StringSerializer se = new StringSerializer();
	@NotNull
	static final private LongSerializer le = new LongSerializer();
	@NotNull
	static final private CompositeSerializer cs = new CompositeSerializer();

	public void testQueryIterator()
	{
		Properties logProperties = new Properties();

		logProperties.put("log4j.rootLogger", "INFO, stdout");
		logProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		logProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.EnhancedPatternLayout");
		logProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%d [%F:%L] [%p] %C{1}: %m%n");
		logProperties.put("log4j.appender.stdout.immediateFlush", "true");

		logProperties.put("log4j.appender.null", "org.apache.log4j.varia.NullAppender");
		logProperties.put("log4j.category.me.prettyprint.cassandra.connection.HThriftClient", "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.cassandra.connection.CassandraHostRetryService",
				  "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.cassandra.connection.HConnectionManager",
				  "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.cassandra.service.JmxMonitor", "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.cassandra.connection.ConcurrentHClientPool",
				  "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.cassandra.hector.TimingLogger", "WARN, stdout");
		logProperties.put("log4j.category.me.prettyprint.hector.TimingLogger", "WARN, stdout");

		logProperties.put("log4j.category.org.eclipse.jetty.util.log", "ERROR, null");

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
			EmbeddedCassandraService cassandraServer = new EmbeddedCassandraService();
			cassandraServer.start();

			Keyspace keyspace = null;
			Cluster cluster = null;
			ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
			ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
			ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);

			cluster = HFactory.getOrCreateCluster("TutorialCluster", "127.0.0.1:9161");
			keyspace = HFactory.createKeyspace("TEST", cluster, ccl);

/*
	                Mutator<String> mutator = HFactory.createMutator(keyspace, se);
	
	                for (long j = 0; j < 10; j++)
	                {
	                        for (int i = 0; i < 1000; i++)
	                        {
	                                HColumnImpl<Composite, Long> column = new HColumnImpl<Composite, Long>(cs, le);
	                                column.setClock(keyspace.createClock());
	                                Composite dc = new Composite();
	                                dc.add(0, j);
	                                dc.add(1, "a" + i);
	                                column.setName(dc);
	                                column.setValue(0L);
	                                mutator.addInsertion("TX:512", "rollup10s", column);
	                        }
	                }
	
	                mutator.execute();
*/

			long start = System.currentTimeMillis();

			Composite startRange = new Composite();
			Composite endRange = new Composite();

			//
			// range [0 - 10)
			// endRange a212 ignored, startRange a12 not ignored, but also only applied lexically,
			// so it skipped a1, a10, a11, and that's it.  a2, etc, still showed up.
			//
			//startRange.add(0, 0L);
			startRange.addComponent(0L, le, "LongType",
						AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
			// startRange.add(1, "a12");
			// endRange.add(0, 10L);
			endRange.addComponent(10L, le, "LongType", AbstractComposite.ComponentEquality.EQUAL);
			// endRange.add(1, "a212");

			System.out.println("startRange: " + startRange + ", endRange: " + endRange);

			//
			// [0-10) gives 0-9, when not reversed
			// [0-10) gives 9-0, when reversed
			// [0.>= - 10.*) gives 1-9, when reversed or not
			//
			CompositeQueryIterator iter = new CompositeQueryIterator(keyspace, "rollup10s", "TX:512",
										 startRange, endRange, false);

			int count = 0;
			HashSet<Long> timestamps = new HashSet<Long>();

			for (HColumn<Composite, Long> column : iter)
			{
				/*
				log.info(
	                                "Timestamp: " + column.getName().get(0, le) +
	                                ", field: " + column.getName().get(1, se) +
	                                " = " + column.getValue());
				*/
				timestamps.add(column.getName().get(0, le));
				count++;
			}

			long duration = System.currentTimeMillis() - start;

			log.info("Found " + count + " columns in " + duration + " ms");
			log.info(timestamps);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			assertTrue(false);
		}
	}

	/*
	Goal in CQL:
	
	create column family rollup10s
	    with comparator = 'CompositeType(LongType,UTF8Type)'
	    and key_validation_class = 'UTF8Type'
	    and default_validation_class = 'LongType';
	*/

	void makeKeyspace(final String ks_name) throws Exception
	{
		List<ColumnDef> columns = new ArrayList<ColumnDef>();
		List<ColumnDefinition> columnMetadata = ThriftColumnDef.fromThriftList(columns);
		ColumnFamilyDefinition cf_def = HFactory.createColumnFamilyDefinition(ks_name, "rollup10s",
										      ComparatorType.COMPOSITETYPE,
										      columnMetadata);
		cf_def.setComparatorTypeAlias("(LongType, UTF8Type)");

		//
		// cf_def is actually a ThriftCfDef, which is settable
		//
		ThriftCfDef thriftCfDef = new ThriftCfDef(cf_def);
		thriftCfDef.setColumnType(ColumnType.STANDARD);
		thriftCfDef.setDefaultValidationClass("LongType");				// type of values
		thriftCfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());	// type of keys

		//
		// set gc grace to 1 second so maybe our counter deletes will take effect immediately
		//
		thriftCfDef.setGcGraceSeconds(1);

		List<ColumnFamilyDefinition> cf_defs = new ArrayList<ColumnFamilyDefinition>();
		cf_defs.add(thriftCfDef);

		log.debug("creating keyspace: " + ks_name);
		KeyspaceDefinition ks_def = HFactory.createKeyspaceDefinition(ks_name,
									      "org.apache.cassandra.locator.SimpleStrategy",
									      1, cf_defs);
		cluster.addKeyspace(ks_def);
		log.info("created keyspace: " + ks_name);
		log.debug("keyspace: " + ks_def);
		log.debug("cfDef: " + thriftCfDef);
	}
}
