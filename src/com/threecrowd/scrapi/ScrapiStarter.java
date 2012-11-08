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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.CloneNotSupportedException;

/**
 * ScrapiStarter
 * Intermediate step towards launching a daemon instance.
 */

final class ScrapiStarter
{
	/**
	 * Constructor
	 * Do not use.
	 */

	private ScrapiStarter()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * main
	 */

	public static void main(@NotNull final String[] args) throws Exception
	{
		Thread ccThread = new Thread(new Scrapi(args));
		ccThread.start();
	}
}
