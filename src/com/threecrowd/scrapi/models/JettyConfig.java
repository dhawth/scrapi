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

final public class JettyConfig
{
	private static ObjectMapper mapper = new ObjectMapper();

	public Boolean single_threaded = Boolean.FALSE;

	@NotNull
	public Integer number_of_acceptors = 2;

	@NotNull
	public Integer accept_queue_size = 100;

	@NotNull
	public Integer thread_pool_size = 256;

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
