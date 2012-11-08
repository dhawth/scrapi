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
// this class ships stats to statsd from the StatsObject instance every $period seconds
//

import org.apache.log4j.*;

import java.util.HashMap;

import java.io.StringWriter;
import java.io.PrintWriter;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.threecrowd.scrapi.models.*;

final class StatsdShipper implements Runnable
{
	@Nullable
	private Logger log = null;
	@Nullable
	private NodeConfig config = null;
	@Nullable
	private StatsObject so = null;
	@Nullable
	private StatsdClient client = null;

	public StatsdShipper(@NotNull NodeConfig c)
		throws Exception
	{
		//
		// intellij thinks this statement is unnecessary because it has proven that
		// c can never be null.  I'm leaving it in anyway.
		//
		//noinspection ConstantConditions
		if (null == c)
		{
			throw new IllegalArgumentException("config argument is null");
		}
		if (null == c.statsd_config)
		{
			throw new IllegalArgumentException("statsd_config block is missing from config");
		}
		if (null == c.statsd_config.hostname || c.statsd_config.hostname.length() == 0)
		{
			throw new IllegalArgumentException("statsd_config hostname is missing or is 0 length");
		}
		if (null == c.statsd_config.port || c.statsd_config.port < 1)
		{
			throw new IllegalArgumentException("statsd_config port is missing or is < 1");
		}
		if (null == c.statsd_config.period || c.statsd_config.period < 1)
		{
			throw new IllegalArgumentException("statsd_config period is missing or is < 1");
		}
		if (null == c.statsd_config.prepend_strings || c.statsd_config.prepend_strings.size() == 0)
		{
			throw new IllegalArgumentException("statsd_config prepend_strings is missing or is 0 length");
		}

		log = Logger.getLogger(StatsdShipper.class);
		config = c;
		so = StatsObject.getInstance();
		client = new StatsdClient(c.statsd_config.hostname, c.statsd_config.port);
	}

	public void run()
	{
		while (true)
		{
			try
			{
				//
				// intellij thinks config could be null here.  It can't.
				//
				//noinspection ConstantConditions
				Thread.sleep(config.statsd_config.period * 1000);

				HashMap<String, Long> stats_map = so.getMapAndClear();

				if (null == stats_map || stats_map.size() == 0)
				{
					continue;
				}

				log.debug("shipping " + stats_map.size() + " stats to statsd");

				for (String key : stats_map.keySet())
				{
					//
					// prepend the keys with the prepend value from the config
					//
					for (String prepend : config.statsd_config.prepend_strings)
					{
						String real_key = prepend + "." + key;

						//
						// a little fixup: replace spaces with underscores to help out the
						// destination names from the config
						//
						real_key = real_key.replaceAll(" ", "_");

						//
						// ship off to statsd
						//
						client.increment(real_key, stats_map.get(key).intValue());
					}
				}
			}
			catch (Exception e)
			{
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				String stacktrace = sw.toString();
				log.warn(e);
				log.warn(stacktrace);
			}
		}
	}
}
