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

import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.CloneNotSupportedException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.io.File;
import java.net.URISyntaxException;

import org.apache.log4j.*;

import java.lang.Math;
import java.io.*;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

final class CreateCF
{
	private static CassandraSetup cassandraSetup = null;

	private CreateCF()
	{
		throw new UnsupportedOperationException();
	}

	private static void check_args(final String[] args, int i, final String flag)
		throws Exception
	{
		if (args.length - 1 < i + 1)
		{
			throw new IllegalArgumentException("missing argument after argument flag " + flag);
		}
	}

	public static void main(final String[] args) throws IOException, InterruptedException, URISyntaxException
	{
		boolean debug = false;
		String keyspace = null;
		String host = null;
		boolean delete = false;
		boolean force = false;
		boolean list = false;
		ArrayList<String> cfs = new ArrayList<String>();
		Integer RF = 3;

		BasicConfigurator.configure();

		try
		{
			for (int i = 0; i < args.length; i++)
			{
				if (args[i].trim().equals("-h"))
				{
					check_args(args, i, "-h");
					host = args[++i];
				}
				else if (args[i].trim().equals("-f"))
				{
					force = true;
				}
				else if (args[i].trim().equals("-D"))
				{
					debug = true;
				}
				else if (args[i].trim().equals("-k"))
				{
					check_args(args, i, "-k");
					keyspace = args[++i];
				}
				else if (args[i].trim().equals("-c"))
				{
					check_args(args, i, "-c");
					cfs.add(args[++i]);
				}
				else if (args[i].trim().equals("-d"))
				{
					delete = true;
				}
				else if (args[i].trim().equals("-l"))
				{
					list = true;
				}
				else if (args[i].trim().equals("-rf"))
				{
					RF = new Integer(args[++i]);
				}
			}

			cassandraSetup = new CassandraSetup(host);

			if (list)
			{
				HashMap<String, ArrayList<String>> results = cassandraSetup.getKeyspaceInfo();

				for (String k : results.keySet())
				{
					System.out.println("keyspace " + k);
					for (String c : results.get(k))
					{
						System.out.println("\t" + c);
					}
				}
				System.exit(0);
			}

			if (!cfs.isEmpty())
			{
				//
				// create column family
				//
				cassandraSetup.createCF(keyspace, cfs, RF);
				System.exit(0);
			}
			else
			{
				//
				// create keyspace
				//
				cassandraSetup.createKeyspace(keyspace, RF);
				System.exit(0);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		System.exit(1);
	}
}
