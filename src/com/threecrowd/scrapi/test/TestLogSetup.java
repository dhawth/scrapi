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

import java.util.Properties;

public final class TestLogSetup extends TestCase
{
	public void testLogSetup()
	{
		Properties logProperties = new Properties();

		logProperties.put("log4j.rootLogger", "DEBUG, stdout");
		logProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		logProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.EnhancedPatternLayout");
		logProperties.put("log4j.appender.stdout.layout.ConversionPattern", "%d [%F:%L] [%p] %C{1}: %m%n");
		logProperties.put("log4j.appender.stdout.immediateFlush", "true");

		logProperties.put("log4j.appender.null", "org.apache.log4j.varia.NullAppender");
		logProperties.put("log4j.category.me.prettyprint.hector.TimingLogger", "ERROR, null");
		logProperties.put("log4j.additifity.me.prettyprint.hector.TimingLogger", "false");

		logProperties.put("log4j.category.org.eclipse.log", "ERROR, null");

		BasicConfigurator.resetConfiguration();
		PropertyConfigurator.configure(logProperties);
	}
}
