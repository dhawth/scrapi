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

import org.apache.log4j.*;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;


/**
 * StatsObject
 * Accepts "update" commands from the RestInterfaceHandler and keeps track of
 * various statistics.
 */

final class StatsObject
{
	//
	// We use a separate logger for the stats for testing purposes, since we don't
	// have a stats object or anything to forward that stuff to yet
	//
	@NotNull
	private Logger log = Logger.getLogger(StatsObject.class);
	private final HashMap<String, StatObject> currentValues = new HashMap<String, StatObject>();
	private Gson gson = new Gson();

	enum ValueType
	{
		MIN, MAX, AVG, SUM
	}

	private static class SingletonHolder
	{
		@NotNull
		public static final StatsObject INSTANCE = new StatsObject();
	}

	@NotNull
	public static StatsObject getInstance()
	{
		return SingletonHolder.INSTANCE;
	}

	private StatsObject()
	{
	}

	/*
	 * getMap returns a String -> Long map of the currentValues hashmap,
	 * translating the StatObjects into their Long representations
	 */

	@NotNull
	HashMap<String, Long> getMap()
	{
		HashMap<String, Long> map = new HashMap<String, Long>();
		synchronized (currentValues)
		{
			for (StatObject s : currentValues.values())
			{
				if (null == s)
				{
					continue;
				}
				switch (s.getType())
				{
					case AVG:
						map.put(s.getKey(), s.getValue() / s.getCount());
						break;
					case SUM:
					case MIN:
					case MAX:
						map.put(s.getKey(), s.getValue());
					default:
						break;
				}
			}
		}
		return map;
	}

	@NotNull
	HashMap<String, Long> getMapAndClear()
	{
		HashMap<String, Long> map = new HashMap<String, Long>();
		synchronized (currentValues)
		{
			for (StatObject s : currentValues.values())
			{
				if (null == s)
				{
					continue;
				}
				switch (s.getType())
				{
					case AVG:
						map.put(s.getKey(), s.getValue() / s.getCount());
						break;
					case SUM:
					case MIN:
					case MAX:
						map.put(s.getKey(), s.getValue());
					default:
						break;
				}
			}
			currentValues.clear();
		}
		return map;
	}

	public String toString()
	{
		Map<String, Long> sortedMap = new TreeMap<String, Long>(getMap());
		return sortedMap.toString();
	}

	@Nullable
	Long getValueByName(String name)
	{
		StatObject s = null;

		synchronized (currentValues)
		{
			s = currentValues.get(name);
		}

		if (null == s)
		{
			return null;
		}

		switch (s.getType())
		{
			case AVG:
				return s.getValue() / s.getCount();
			case SUM:
			case MIN:
			case MAX:
				return s.getValue();
			default:
				return null;
		}
		//
		// notreached
		//
	}

	void clear()
	{
		synchronized (currentValues)
		{
			currentValues.clear();
		}
	}

	void clearMap()
	{
		clear();
	}

	void clear(final String key)
	{
		synchronized (currentValues)
		{
			currentValues.remove(key);
		}
	}

	/**
	 * update
	 * the update() method is the main external interface point.
	 * usage: update(StatsObject.ValueType.SUM, "some value that represents a sum", 5)
	 */

	void update(@NotNull final ValueType type, @Nullable final String name, final long value)
	{
		if (name == null || value <= 0)
		{
			return;
		}

		synchronized (currentValues)
		{
			StatObject s = currentValues.get(name);
			if (s == null)
			{
				s = new StatObject(type, name, value);
				currentValues.put(name, s);
				return;
			}

			switch (type)
			{
				case SUM:
					s.setValue(s.getValue() + value);
					break;
				case MIN:
					if (value < s.getValue())
					{
						s.setValue(value);
					}
					break;
				case MAX:
					if (value > s.getValue())
					{
						s.setValue(value);
					}
					break;
				case AVG:
					s.incrementCount();
					s.setValue(s.getValue() + value);
					break;
				default:
					break;
			}
		}

	}

	private final static class StatObject
	{
		private final String key;
		private final ValueType type;
		Long value;
		Long count;

		StatObject(final ValueType t, final String k, final Long v)
		{
			type = t;
			key = k;
			value = v;
			count = 1L;
		}

		protected StatObject(final ValueType t, final String k, final Long v, final Long c)
		{
			type = t;
			key = k;
			value = v;
			count = c;
		}

		String getKey()
		{
			return key;
		}

		ValueType getType()
		{
			return type;
		}

		void setValue(final Long v)
		{
			value = v;
		}

		Long getValue()
		{
			return value;
		}

		protected void setCount(final Long c)
		{
			count = c;
		}

		Long getCount()
		{
			return count;
		}

		void incrementCount()
		{
			count++;
		}
	}
}
