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

package com.threecrowd.cassy;

import java.lang.CloneNotSupportedException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;

import org.apache.log4j.*;

import java.io.*;
import java.lang.Math;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.*;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;

import me.prettyprint.cassandra.connection.*;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.*;

import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.*;
import me.prettyprint.hector.api.query.*;

final public class CassandraSetup
{
	private static Logger log = Logger.getLogger(CassandraSetup.class);

	//
	// hector api vars
	//
	private Cluster cluster = null;
	private Keyspace keyspace = null;

	//
	// thrift vars for executing raw cql
	//
	private String host = null;
	private int port = 9160;
	private TTransport tr = null;
	private TProtocol proto = null;
	private Cassandra.Client client = null;

	public CassandraSetup(String host) throws Exception
	{
		//
		// set up hector objects
		//
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(host);

		cassandraHostConfigurator.setMaxActive(1);
		cassandraHostConfigurator.setCassandraThriftSocketTimeout(10000);

		log.debug("hostconfigurator: " + cassandraHostConfigurator);

		cluster = HFactory.getOrCreateCluster("CassandraSetupCluster", cassandraHostConfigurator);

		//
		// set up direct Thrift connection
		//
		String[] fields = host.split(":");

		if (fields.length == 2)
		{
			port = Integer.parseInt(fields[1]);
			host = fields[0];
		}

		this.host = host;

		tr = new TFramedTransport(new TSocket(host, port));
		proto = new TBinaryProtocol(tr);
		client = new Cassandra.Client(proto);

		tr.open();

		client.set_cql_version("3.0.0-beta1");
	}

	//
	// returns map of keyspace => array(column family names)
	//
	public HashMap<String, ArrayList<String>> getKeyspaceInfo() throws Exception
	{
		HashMap<String, ArrayList<String>> results = new HashMap<String, ArrayList<String>>();

		List<KeyspaceDefinition> ksDefs = cluster.describeKeyspaces();

		if (ksDefs == null)
		{
			return null;
		}

		for (KeyspaceDefinition ksDef : ksDefs)
		{
			results.put(ksDef.getName(), new ArrayList<String>());

			for (ColumnFamilyDefinition cfDef : ksDef.getCfDefs())
			{
				results.get(ksDef.getName()).add(cfDef.getName());
			}
		}

		log.debug("clusterDef: " + results.toString());

		return results;
	}

	public void createCF(final String ks, final ArrayList<String> cfs, int RF) throws Exception
	{
		if (null == ks || ks.isEmpty())
		{
			throw new IllegalArgumentException("keyspace argument is null");
		}

		if (cfs.isEmpty())
		{
			throw new IllegalArgumentException("empty list of cfs given");
		}

		for (String cf : cfs)
		{
			if (null == cf || cf.isEmpty())
			{
				throw new IllegalArgumentException("column family argument is null");
			}
		}

		HashMap<String, ArrayList<String>> clusterDef = getKeyspaceInfo();

		if (!clusterDef.containsKey(ks))
		{
			createKeyspace(ks, RF);
		}

		selectKeyspace(ks);

		clusterDef = getKeyspaceInfo();

		//
		// create any CFs that DNE
		//

		for (String cf : cfs)
		{
			if (clusterDef.get(ks).contains(cf))
			{
				continue;
			}

			log.debug("Creating missing CF: " + cf);

/*
under 1.0.10 this is what the CF definition looks like:

create column family rollup5m
  with column_type = 'Standard'
  and comparator = 'CompositeType(org.apache.cassandra.db.marshal.LongType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)'
  and default_validation_class = 'LongType'
  and key_validation_class = 'UTF8Type'
  and rows_cached = 5000.0
  and row_cache_save_period = 0
  and row_cache_keys_to_save = 0
  and keys_cached = 400000.0
  and key_cache_save_period = 14400
  and read_repair_chance = 1.0
  and gc_grace = 864000
  and min_compaction_threshold = 4
  and max_compaction_threshold = 32
  and replicate_on_write = true
  and row_cache_provider = 'ConcurrentLinkedHashCacheProvider'
  and compaction_strategy = 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
  and compaction_strategy_options = {'sstable_size_in_mb' : '10'};
*/

			//
			// cql doesn't work for this for various reasons, unfortunately,
			// so for this operation we have to go back to using thrift.
			//

			List<ColumnDef> columns = new ArrayList<ColumnDef>();
			List<ColumnDefinition> columnMetadata = ThriftColumnDef.fromThriftList(columns);
			ColumnFamilyDefinition cf_def = HFactory.createColumnFamilyDefinition(ks, cf,
											      ComparatorType.COMPOSITETYPE,
											      columnMetadata);
			cf_def.setComparatorTypeAlias("(LongType, UTF8Type, UTF8Type)");

			//
			// cf_def is actually a ThriftCfDef, which is settable
			//
			ThriftCfDef thriftCfDef = new ThriftCfDef(cf_def);
			thriftCfDef.setColumnType(ColumnType.STANDARD);
			thriftCfDef.setDefaultValidationClass(
				"LongType");			      // type of values
			thriftCfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());      // type of keys
			thriftCfDef.setCompactionStrategy(
				"org.apache.cassandra.db.compaction.LeveledCompactionStrategy");
			HashMap<String, String> options = new HashMap<String, String>();
			options.put("sstable_size_in_mb", "32");
			thriftCfDef.setCompactionStrategyOptions(options);
			thriftCfDef.setReplicateOnWrite(true);

			//
			// and is convertable back to a ColumnFamilyDefinition
			//
			cf_def = thriftCfDef;

			cluster.addColumnFamily(cf_def, true);
		}
	}

	public void createCF(final String ks, final String cf, int RF) throws Exception
	{
		ArrayList<String> cfs = new ArrayList<String>();
		cfs.add(cf);

		createCF(ks, cfs, RF);
	}

	public void createKeyspace(String ks, int RF) throws Exception
	{
		if (ks == null || ks.length() == 0)
		{
			throw new IllegalArgumentException("keyspace argument is null");
		}

		//
		// attempt to make sure all the column families specified in the column_families list
		// exist.  populate MissingCFs with a list of all CFs we want based on the config we
		// have and then remove the ones we find already in cassandra.  The ones that are left
		// are the ones that we need to create.
		//
		boolean found_keyspace = false;

		HashMap<String, ArrayList<String>> clusterDef = getKeyspaceInfo();

		if (clusterDef.containsKey(ks))
		{
			return;
		}

		log.debug("keyspace does not exist, creating keyspace: " + ks);

		String cql = "";

		cql = "CREATE keyspace " + ks + " WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = '" + RF + "';";
		client.execute_cql_query(ByteBuffer.wrap(cql.getBytes()), Compression.NONE);

		log.info("created keyspace: " + ks);

		try
		{
			Thread.sleep(5000);
		}
		catch (Exception e)
		{
		}
	}

	public void selectKeyspace(final String ks) throws Exception
	{
		keyspace = HFactory.createKeyspace(ks, cluster);

		String cql = "USE " + ks + ";";
		client.execute_cql_query(ByteBuffer.wrap(cql.getBytes()), Compression.NONE);
	}
}

