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

package com.threecrowd;

//
// This source code was copied from a datastax example
//

import me.prettyprint.cassandra.model.*;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.*;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

import org.apache.log4j.*;
import org.apache.log4j.xml.DOMConfigurator;
import org.jetbrains.annotations.NotNull;

import java.lang.management.*;
import javax.management.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class CompositeQueryIterator implements Iterable<HColumn<Composite, Long>>
{
	@NotNull
	private final ColumnSliceIterator<String, Composite, Long> sliceIterator;
	@NotNull
	private final StringSerializer se = new StringSerializer();
	@NotNull
	private final LongSerializer le = new LongSerializer();
	@NotNull
	private final CompositeSerializer ce = new CompositeSerializer();

	public CompositeQueryIterator(Keyspace ks, String cf, String key, Composite start, Composite end, boolean reversed)
	{
		SliceQuery<String, Composite, Long> sliceQuery = HFactory.createSliceQuery(ks, se, ce, le);

		sliceQuery.setColumnFamily(cf);
		sliceQuery.setKey(key);

		if (reversed)
		{
			sliceIterator = new ColumnSliceIterator(sliceQuery, end, start, true);
		}
		else
		{
			sliceIterator = new ColumnSliceIterator(sliceQuery, start, end, reversed);
		}
	}

	@NotNull
	public Iterator<HColumn<Composite, Long>> iterator()
	{
		return sliceIterator;
	}
}

