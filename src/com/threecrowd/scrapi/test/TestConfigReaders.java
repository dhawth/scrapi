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

import java.util.List;

import com.threecrowd.scrapi.models.*;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;

public final class TestConfigReaders extends TestCase
{

	@SuppressWarnings({"AssignmentToNull"})
	public void testReadNodeConfig()
	{
		NodeConfig config = null;

		try
		{
			config = ConfigReader.ReadNodeConfig("test_data/configs/non_existent_file.conf");
			assertTrue(false);
		}
		catch (Exception e)
		{
			assertTrue(e.getMessage(), e.getMessage().equals(
				"test_data/configs/non_existent_file.conf (No such file or directory)"));
		}

		try
		{
			config = ConfigReader.ReadNodeConfig("test_data/configs/blank_file.conf");
			assertTrue(false);
		}
		catch (Exception e)
		{
			assertTrue(e.getMessage(), e.getMessage().equals("config file is empty"));
		}

		//
		// next test should succeed
		//

		try
		{
			config = ConfigReader.ReadNodeConfig("test_data/configs/json_with_comments.conf");
		}
		catch (Exception e)
		{
			assertTrue(e.getMessage(), false);
		}

		//
		// try to read in a sample NodeConfig object
		//
		config = null;

		NodeConfig node_config = null;

		try
		{
			node_config = ConfigReader.ReadNodeConfig("test_data/configs/sample_node_config.conf");
			assertNotNull(node_config);
		}
		catch (Exception e)
		{
			assertTrue(e.getMessage(), false);
		}
	}
}
