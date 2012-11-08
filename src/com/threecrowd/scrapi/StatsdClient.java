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
// this was copied wholesale from https://raw.github.com/etsy/statsd/master/StatsdClient.java
// which is a reference implementation of a statsd client in java
//

/**
 * StatsdClient.java
 *
 * (C) 2011 Meetup, Inc.
 * Author: Andrew Gwozdziewycz <andrew@meetup.com>, @apgwoz
 *
 *
 *
 * Example usage:
 *
 *    StatsdClient client = new StatsdClient("statsd.example.com", 8125);
 *    // increment by 1
 *    client.increment("foo.bar.baz");
 *    // increment by 10
 *    client.increment("foo.bar.baz", 10);
 *    // sample rate
 *    client.increment("foo.bar.baz", 10, .1);
 *    // increment multiple keys by 1
 *    client.increment("foo.bar.baz", "foo.bar.boo", "foo.baz.bar");
 *    // increment multiple keys by 10 -- yeah, it's "backwards"
 *    client.increment(10, "foo.bar.baz", "foo.bar.boo", "foo.baz.bar");
 *    // multiple keys with a sample rate
 *    client.increment(10, .1, "foo.bar.baz", "foo.bar.boo", "foo.baz.bar");
 *
 * Note: For best results, and greater availability, you'll probably want to 
 * create a wrapper class which creates a static client and proxies to it.
 *
 * You know... the "Java way."
 */

import java.util.Random;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

class StatsdClient
{
	@NotNull
	private static final Random RNG = new Random();
	private static final Logger log = Logger.getLogger(StatsdClient.class.getName());

	private final InetAddress _host;
	private final int _port;

	private DatagramSocket _sock;

	public StatsdClient(String host, int port) throws UnknownHostException, SocketException
	{
		this(InetAddress.getByName(host), port);
	}

	private StatsdClient(InetAddress host, int port) throws SocketException
	{
		_host = host;
		_port = port;
		_sock = new DatagramSocket();
	}

	public boolean timing(String key, int value)
	{
		return timing(key, value, 1.0);
	}

	boolean timing(String key, int value, double sampleRate)
	{
		return send(sampleRate, String.format("%s:%d|ms", key, value));
	}

	public boolean decrement(String key)
	{
		return increment(key, -1, 1.0);
	}

	public boolean decrement(String key, int magnitude)
	{
		return decrement(key, magnitude, 1.0);
	}

	boolean decrement(String key, int magnitude, double sampleRate)
	{
		int magnitude1 = magnitude < 0 ? magnitude : -magnitude;
		return increment(key, magnitude1, sampleRate);
	}

	public boolean decrement(String... keys)
	{
		return increment(-1, 1.0, keys);
	}

	public boolean decrement(int magnitude, String... keys)
	{
		int magnitude1 = magnitude < 0 ? magnitude : -magnitude;
		return increment(magnitude1, 1.0, keys);
	}

	public boolean decrement(int magnitude, double sampleRate, String... keys)
	{
		int magnitude1 = magnitude < 0 ? magnitude : -magnitude;
		return increment(magnitude1, sampleRate, keys);
	}

	public boolean increment(String key)
	{
		return increment(key, 1, 1.0);
	}

	public boolean increment(String key, int magnitude)
	{
		return increment(key, magnitude, 1.0);
	}

	boolean increment(String key, int magnitude, double sampleRate)
	{
		String stat = String.format("%s:%s|c", key, magnitude);
		return send(stat, sampleRate);
	}

	boolean increment(int magnitude, double sampleRate, @NotNull String... keys)
	{
		String[] stats = new String[keys.length];
		for (int i = 0; i < keys.length; i++)
		{
			stats[i] = String.format("%s:%s|c", keys[i], magnitude);
		}
		return send(sampleRate, stats);
	}

	private boolean send(String stat, double sampleRate)
	{
		return send(sampleRate, stat);
	}

	private boolean send(double sampleRate, @NotNull String... stats)
	{

		boolean retval = false; // didn't send anything
		if (sampleRate < 1.0)
		{
			for (String stat : stats)
			{
				if (RNG.nextDouble() <= sampleRate)
				{
					stat = String.format("%s|@%f", stat, sampleRate);
					if (doSend(stat))
					{
						retval = true;
					}
				}
			}
		}
		else
		{
			for (String stat : stats)
			{
				if (doSend(stat))
				{
					retval = true;
				}
			}
		}

		return retval;
	}

	private boolean doSend(@NotNull String stat)
	{
		try
		{
			byte[] data = stat.getBytes();
			_sock.send(new DatagramPacket(data, data.length, _host, _port));
			return true;
		}
		catch (IOException e)
		{
			log.error(String.format("Could not send stat %s to host %s:%d", stat, _host, _port), e);
		}
		return false;
	}
}
