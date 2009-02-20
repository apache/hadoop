/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.extraction.engine.datasource;

import java.util.HashMap;

import org.apache.hadoop.chukwa.extraction.engine.datasource.database.DatabaseDS;
import org.apache.hadoop.chukwa.extraction.engine.datasource.record.RecordDS;

public class DataSourceFactory
{
	private static Object lock = new Object();
	private static DataSourceFactory factory = null;
	private HashMap<String, DataSource> dataSources = new HashMap<String, DataSource>();
	
	private DataSourceFactory()
	{
		// TODO load from config Name + class + threadSafe? 
		
		DataSource databaseDS = new DatabaseDS();
		dataSources.put("MRJob", databaseDS);
		dataSources.put("HodJob", databaseDS);
		dataSources.put("QueueInfo", databaseDS);
		
		DataSource recordDS = new RecordDS();
		dataSources.put("NameNode", recordDS);
		dataSources.put("ChukwaLocalAgent", recordDS);
	}
	
	public static DataSourceFactory getInstance()
	{
		synchronized(lock)
		{
			if ( factory == null)
			{
				factory = new DataSourceFactory();
			}
		}
		return factory;
	}
	
	public DataSource getDataSource(String datasourceName)
	throws DataSourceException
	{
		if (dataSources.containsKey(datasourceName))
		{
			return dataSources.get(datasourceName);
		}
		else
		{
			DataSource hsdfsDS = new RecordDS();
			dataSources.put(datasourceName, hsdfsDS);
			return hsdfsDS;
			//TODO proto only!
			// throw new DataSourceException("Unknown datasource");
		}	
	}
}
