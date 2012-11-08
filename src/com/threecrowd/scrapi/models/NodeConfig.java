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

package com.threecrowd.scrapi.models;

import com.google.gson.annotations.SerializedName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;

//
// This class describes the fields in the configuration files and must
// be updated if that configuration format changes.
//

//
// Node configuration for a CassandraAPI node
//
final public class NodeConfig
{
	private static ObjectMapper mapper = new ObjectMapper();

	@SerializedName("cassandra_host")
	public String cassandra_host;

	@SerializedName("local_port")
	public Integer local_port;

	@SerializedName("local_address")
	public String local_address;

	@SerializedName("client_idle_timeout")
	public Integer client_idle_timeout;

	@Nullable
	@SerializedName("statsd_config")
	public StatsdConfig statsd_config = null;

	@NotNull
	@SerializedName("jetty_config")
	public JettyConfig jetty_config = new JettyConfig();

	@NotNull
	@SerializedName("cassandra_config")
	public CassandraConfig cassandra_config = new CassandraConfig();

	@NotNull
	@SerializedName("column_families")
	public HashMap<String, Integer> column_families = new HashMap<String, Integer>();

	public String toString()
	{
		synchronized (mapper)
		{
			try
			{
				return mapper.writeValueAsString(this);
			}
			catch (IOException e)
			{
				return "unable to write value as string: " + e.getMessage();
			}
		}
	}
}
