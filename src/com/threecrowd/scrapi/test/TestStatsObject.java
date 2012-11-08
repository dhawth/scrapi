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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

public final class TestStatsObject extends TestCase
{

	public void testStatsObject() throws IOException
	{
		Properties logProperties = new Properties();

		logProperties.put("log4j.rootLogger", "DEBUG, stdout");
		logProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		logProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.EnhancedPatternLayout");
		logProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%d [%F:%L] [%p] %C{1}: %m%n");
		logProperties.put("log4j.appender.stdout.immediateFlush", "true");
		logProperties.put("log4j.appender.null", "org.apache.log4j.varia.NullAppender");

		BasicConfigurator.resetConfiguration();
		PropertyConfigurator.configure(logProperties);

		Logger log = Logger.getLogger(com.threecrowd.scrapi.TestStatsObject.class);

		//
		// check raw log output
		//

		StatsObject so = StatsObject.getInstance();

		so.update(StatsObject.ValueType.SUM, "sum_test", 50L);
		so.update(StatsObject.ValueType.SUM, "sum_test", 100L);
		so.update(StatsObject.ValueType.MIN, "min_test", 100L);
		so.update(StatsObject.ValueType.MIN, "min_test", 50L);
		so.update(StatsObject.ValueType.MAX, "max_test", 100L);
		so.update(StatsObject.ValueType.MAX, "max_test", 500L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 10L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 20L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 30L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 40L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 50L);

		try
		{
			Thread.sleep(1000);
		}
		catch (Exception e)
		{
		}

		HashMap<String, Long> results = so.getMap();
		assertTrue("results map is null", results != null);

		assertTrue("results[sum_test] is null: " + results.toString(), results.get("sum_test") != null);
		assertTrue("results[min_test] is null", results.get("min_test") != null);
		assertTrue("results[max_test] is null", results.get("max_test") != null);
		assertTrue("results[avg_test] is null", results.get("avg_test") != null);

		assertTrue("sum_test != 150 (sum_test = " + results.get("sum_test") + ")",
			   results.get("sum_test") == 150L);
		assertTrue("min_test != 50", results.get("min_test") == 50L);
		assertTrue("max_test != 500", results.get("max_test") == 500L);
		assertTrue("avg_test != 30 (avg_test = " + results.get("avg_test") + ")",
			   results.get("avg_test") == 30L);

		//
		// test the getValueByName function and make sure they agree with expectations
		//
		assertTrue("getValueByName(sum_test) != 150", so.getValueByName("sum_test") == 150L);
		assertTrue("getValueByName(avg_test) != 150", so.getValueByName("avg_test") == 30L);

		//
		// give it time to get written to the output log
		//
		try
		{
			//noinspection BusyWait
			Thread.sleep(2000);
		}
		catch (InterruptedException x)
		{
			log.debug(x.toString());
		}

		//
		// manually clear the map now that the default behavior is to not clear it
		//
		so.clearMap();

		results = so.getMap();
		assertTrue("results map is null", results != null);
		assertTrue("results map is not empty", results.size() == 0);

		//
		// reinsert the same data but don't write it out to logfile, clear it and
		// make sure it was cleared
		//
		so.update(StatsObject.ValueType.SUM, "sum_test", 50L);
		so.update(StatsObject.ValueType.SUM, "sum_test", 100L);
		so.update(StatsObject.ValueType.MIN, "min_test", 100L);
		so.update(StatsObject.ValueType.MIN, "min_test", 50L);
		so.update(StatsObject.ValueType.MAX, "max_test", 100L);
		so.update(StatsObject.ValueType.MAX, "max_test", 500L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 10L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 20L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 30L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 40L);
		so.update(StatsObject.ValueType.AVG, "avg_test", 50L);

		try
		{
			Thread.sleep(1000);
		}
		catch (Exception e)
		{
		}

		results = so.getMap();
		assertTrue("results map is null", results != null);

		assertTrue("results[sum_test] is null: " + results.toString(), results.get("sum_test") != null);
		assertTrue("results[min_test] is null", results.get("min_test") != null);
		assertTrue("results[max_test] is null", results.get("max_test") != null);
		assertTrue("results[avg_test] is null", results.get("avg_test") != null);

		assertTrue("sum_test != 150 (sum_test = " + results.get("sum_test") + ")",
			   results.get("sum_test") == 150L);
		assertTrue("min_test != 50", results.get("min_test") == 50L);
		assertTrue("max_test != 500", results.get("max_test") == 500L);
		assertTrue("avg_test != 30 (avg_test = " + results.get("avg_test") + ")",
			   results.get("avg_test") == 30L);
	}
}
