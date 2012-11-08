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

final public class CassandraConfig
{
	private static ObjectMapper mapper = new ObjectMapper();

	//
	// this is a per-host maximum right now due to bugs in hector
	//
	@NotNull
	public Integer max_connections = 5;

	//
	// socket timeout for cassandra connections, in milliseconds
	// note that this is the timeout for ALL operations, not just connecting to the cluster.
	// as such, it should be high enough that no reads will time out, and we just have to eat
	// the long connection timeouts for now.  The hector client library does not support
	// setting a separate timeout on connections.
	// This may need to be raised, but with a care for how long we want customers to have to
	// wait to find out their graph isn't going to load.
	// default: 2s
	//
	@NotNull
	public Integer thrift_socket_timeout = 5000;

	//
	// set the retry interval in seconds for attempting to reconnect to a down
	// cassandra host
	// default: 1 second
	//
	@NotNull
	public Integer connection_retry_time = 2;

	//
	// queries_per_request limits the number of queries against cassandra done per
	// request before we hand them back a has_more = true and a next_start or a 
	// next_end (if the query is reversed)
	// note that this has been changed a little, since we are now using an aspect
	// of the client library for cassandra that hides the actual number of queries
	// done.  This now represents the number of unique ROWs we will query to satisfy
	// the request, even though each row may trigger as many queries as necessary.
	//
	@NotNull
	public Integer queries_per_request = 100;

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
