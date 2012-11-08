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
import org.apache.log4j.xml.DOMConfigurator;

import org.eclipse.jetty.http.security.Constraint;
import org.eclipse.jetty.http.security.Password;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.AbstractConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.BlockingChannelConnector;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

//
// these are necessary to add jmx support to jetty
//
import org.eclipse.jetty.jmx.MBeanContainer;

import java.lang.management.ManagementFactory;

import java.lang.management.*;
import javax.management.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.security.auth.module.UnixSystem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.threecrowd.scrapi.models.*;

/**
 * com.threecrowd.scrapi.Scrapi
 * Java-based HTTP API layer for talking to cassandra
 */

final class Scrapi implements Runnable
{
	/**
	 * The main container for the Scrapi Server.
	 */

	@NotNull
	private Logger log = Logger.getLogger(Scrapi.class);
	private boolean stop = false;
	@Nullable
	private NodeConfig config = null;
	private boolean debug = false;
	private boolean testing = false;

	/**
	 * constructor
	 * Called from ScrapiDaemonLauncher.
	 */

	Scrapi(@NotNull final String[] args)
		throws Exception
	{
		//
		// use console for this quick check
		//
		BasicConfigurator.configure();				// log4j sets up a console logger

		Properties props = System.getProperties();
		props.setProperty("java.net.preferIPv4Stack", "true");

		//
		// the default config file is located at "/etc/Scrapi.conf"
		//
		String configFile = null;
		String log4jConfigFile = null;

		//
		// check to see if an alternate config file location is specified via -f
		// use a non-foreach for loop here so that we can use the indices to get the filename.
		//

		for (int i = 0; i < args.length; i++)
		{
			if (args[i].trim().equals("-c"))
			{
				//
				// we found it.  Use the i+1'th arg as the filename.
				// leave the args on the command line, as they'll get ignored by the rest
				// of the processing.
				//
				configFile = args[i + 1];
				//noinspection AssignmentToForLoopParameter
				i++;
			}
			else if (args[i].trim().equals("-l"))
			{
				//
				// we found it.  Use the i+1'th arg as the filename.
				// leave the args on the command line, as they'll get ignored by the rest
				// of the processing.
				//
				log4jConfigFile = args[i + 1];
				//noinspection AssignmentToForLoopParameter
				i++;
			}
			else if (args[i].trim().equals("-D"))
			{
				debug = true;
			}
			else if (args[i].trim().equals("-t"))
			{
				testing = true;
			}
		}

		if (null == configFile)
		{
			throw new IllegalArgumentException("no configuration file specified on command line");
		}

		log.debug("reading config file " + configFile);

		//
		// read in data from a json formatted config file.
		//
		config = ConfigReader.ReadNodeConfig(configFile);

		if (null == config)
		{
			throw new IOException("got null from ReadNodeConfig for config file " + configFile);
		}

		//
		// set up the logger to go to our configured log files
		//
		setupLogging(log4jConfigFile);

		//noinspection ConstantConditions
		if (config.local_port == null)
		{
			config.local_port = 80;
		}

		if (config.local_port < 1024)
		{
			UnixSystem ux = new UnixSystem();
			Long uid = ux.getUid();
			if (uid != 0L)
			{
				log.error(
					"cannot start server on a privileged port because we are not root (uid = " + uid + ")");
				Runtime.getRuntime().exit(1);
			}
		}

		String version = determineVersion();	// get version number from build.number which is packaged with the jar file
		log.info(
			"SCRAPI Server build " + version + " started on port " + config.local_port + " by " + System.getProperty(
				"user.name"));
	}

	/**
	 * run
	 * The main run loop/entry point for cache node operation.
	 * When a cache node starts up, this is where the actual execution starts.
	 */

	public void run()
	{
		try
		{
			//
			// kick off the statsd shipper thread
			//
			Thread statsdShipperThread = new Thread(new StatsdShipper(config));
			statsdShipperThread.start();

			//
			// set up the main server instance to handle incoming content requests.
			// this is the scaffolding which the cache processing code will hang upon.
			//
			Server server = serverSetup();

			//
			// These lines are all that's required to make jetty expose JMX data!
			// Combine this with the standard JVM jmxremote configuration and you're done.
			//
			MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
			server.getContainer().addEventListener(mbContainer);
			server.addBean(mbContainer);
			mbContainer.addBean(Logger.getLogger(
				"mbean_server"));	 // it just needs an object, dunno if it has to be a logger

			//
			// construct the handlers and contexts the server will use to service requests.
			//
			ContextHandlerCollection cc = new ContextHandlerCollection();
			RestInterfaceHandler restHandler = buildHandlers(server, cc, config);

			//
			// now that we've laid all the ground work, start the server.
			//
			startServer(server, cc);

			//
			// for testing purposes, enable an immediate shutdown
			//
			if (testing)
			{
				shutdown();
			}

			//
			// call the runLoop method.  This method will not return until we're shutting down.
			//
			runLoop();

			log.debug("shutting down");

			//
			// stop the madness.  Shut down all the threads and servers we've started to this point.
			//
			stopServices(server);
		}
		catch (NoClassDefFoundError e)
		{
			//
			// in any event, log the message to the log file.
			//
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw, true);
			e.printStackTrace(pw);
			pw.flush();
			sw.flush();
			log.error(sw.toString());
			Runtime.getRuntime().exit(1);
		}
		catch (Exception e)
		{
			//
			// if we haven't yet closed stderr, put the stack trace out to the console.
			//
			e.printStackTrace();
			//
			// in any event, log the message to the log file.
			//
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw, true);
			e.printStackTrace(pw);
			pw.flush();
			sw.flush();
			log.error(sw.toString());
			Runtime.getRuntime().exit(1);
		}
		finally
		{
			log.info("exit");
		}
	}

	//
	// private methods
	//

	@Nullable
	private RestInterfaceHandler buildHandlers(Server server, @NotNull ContextHandlerCollection cc, final NodeConfig config)
		throws Exception
	{
		RestInterfaceHandler restHandler = null;
		restHandler = new RestInterfaceHandler(config);
		restHandler.setServer(server);
		ContextHandler restHandlerContext = cc.addContext("/", ".");
		restHandlerContext.setAllowNullPathInfo(true);
		restHandlerContext.setHandler(restHandler);
		return restHandler;
	}

	private void startServer(@NotNull Server server, ContextHandlerCollection cc) throws Exception
	{
		HandlerCollection handlers = new HandlerCollection();
		handlers.setHandlers(new Handler[]{cc});
		server.setHandler(handlers);
		server.setSendDateHeader(true);
		server.start();
	}

	@NotNull
	private Server serverSetup()
	{
		InetSocketAddress cacheServerAddress;
		cacheServerAddress = setServerAddress();
		return mainServerInitialSetup(cacheServerAddress);
	}

	private InetSocketAddress setServerAddress()
	{
		InetSocketAddress cacheServerAddress;

		//noinspection ConstantConditions
		if (config.local_port == null)
		{
			config.local_port = 80;
		}

		if (config.local_address != null)
		{
			cacheServerAddress = new InetSocketAddress(config.local_address, config.local_port);
		}
		else
		{
			cacheServerAddress = new InetSocketAddress(config.local_port);
		}
		log.debug("cacheServerAddress: " + cacheServerAddress.toString());
		return cacheServerAddress;
	}

	@NotNull
	private Server mainServerInitialSetup(@NotNull InetSocketAddress cacheServerAddress)
	{
		Server server = new Server();

		AbstractConnector connector;

		connector = new BlockingChannelConnector();
		connector.setHost(cacheServerAddress.getHostName());
		connector.setPort(cacheServerAddress.getPort());

		//noinspection ConstantConditions
		if (config.client_idle_timeout != null && config.client_idle_timeout > 0)
		{
			log.debug("setting maxIdleTime to " + config.client_idle_timeout + " seconds");
			connector.setMaxIdleTime(1000 * config.client_idle_timeout);
		}
		else
		{
			connector.setMaxIdleTime(-1);
		}
		connector.setLowResourcesMaxIdleTime(1);

		//
		// so_linger (SO_LINGER) is a bad idea, according to this faq:
		// http://developerweb.net/viewtopic.php?id=2982
		//
		// so_linger = false (default) means that close() returns immediately and the OS
		// 	does the needful as far as sending remaining data in the socket to the client
		//	and handling the graceful close of the connection
		// so_linger = true, so_linger_time = 0 results in close() returning immediately but
		//	the OS closes the socket immediately as well, so any packets sent by the client
		//	to continue fetching any data remaining in the socket's outbound buffer, or in
		//	trying to gracefully close the connection, will result in the server sending
		//	RST to the client (bad behavior for a webserver)
		// so_linger = true, so_linger_time > 0 will result in close() blocking on sending the
		//	rest of the data and gracefully closing the connection, or will result in the
		//	close call returning EWOULDBLOCK if it's an async socket.  either way, this
		//	requires more handling on our side and isn't what we want.  We certainly wouldn't
		//	want to block waiting for one client to finish popping his data off the socket.
		//
		// SO_LINGER support removed.  Default is false.
		//

		connector.setAcceptQueueSize(config.jetty_config.accept_queue_size);
		connector.setAcceptors(config.jetty_config.number_of_acceptors);

		//
		// if single_threaded is set to true in the jetty_config section,
		// don't use any threads for handling requests.  This is here for debugging.
		//
		if (!config.jetty_config.single_threaded)
		{
			ArrayBlockingQueue<Runnable> tpeq = new ArrayBlockingQueue<Runnable>(
				config.jetty_config.thread_pool_size);
			int threadPoolSize = config.jetty_config.thread_pool_size;
			ThreadPoolExecutor tpe = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 1,
									TimeUnit.SECONDS, tpeq);
			ExecutorThreadPool q = new ExecutorThreadPool(tpe);
			connector.setThreadPool(q);
		}
		server.addConnector(connector);
		return server;
	}

	private String determineVersion()
	{
		BufferedInputStream versionInputStream = new BufferedInputStream(
			Scrapi.class.getResourceAsStream("/resources/build.number"));
		byte[] versionBuf = new byte[256];
		try
		{
			//noinspection ResultOfMethodCallIgnored
			versionInputStream.read(versionBuf, 0, versionBuf.length);
			versionInputStream.close();
			//noinspection AssignmentToNull
			versionInputStream = null;
		}
		catch (IOException e)
		{
			log.debug("error reading version info.  Setting version to 0.");
			versionBuf = "0".getBytes();
		}
		finally
		{
			if (versionInputStream != null)
			{
				try
				{
					versionInputStream.close();
				}
				catch (IOException e)
				{
					// nothing.
				}
			}
		}
		String version = new String(versionBuf);
		version = version.substring(version.lastIndexOf('=') + 1).trim();
		return version;
	}

	private void stopServices(@NotNull Server server) throws Exception
	{
		server.stop();
	}

	private void runLoop()
	{
		while (!stop)
		{
			try
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException x)
			{
			}
		}
	}

	/**
	 * setupLogging
	 * Configure the log4j subsystem.
	 */
	private void setupLogging(@Nullable final String log4j_config) throws Exception
	{
		Properties props = new Properties();

		//
		// load default included in jar file
		//
		if (null == log4j_config)
		{
			log.info("loading /resources/log4j.conf");
			props.load(Scrapi.class.getResourceAsStream("/resources/log4j.conf"));
			log.debug("properties from stored log4j are: " + props);
		}
		else
		{
			log.info("loading " + log4j_config);
			props.load(new FileInputStream(log4j_config));
			log.debug("properties from " + log4j_config + " are: " + props);
		}

		BasicConfigurator.resetConfiguration();
		PropertyConfigurator.configure(props);

		//
		// if -D argument passed on command line, enable debugging
		//

		if (debug)
		{
			Logger.getRootLogger().setLevel(Level.toLevel("DEBUG"));
		}

		log = Logger.getLogger(Scrapi.class);
	}


	/**
	 * shutdown
	 * stop the madness!
	 */

	void shutdown()
	{
		stop = true;
	}

}
