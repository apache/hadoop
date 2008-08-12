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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DsDirectory
{
	private static Object lock = new Object();
	private static DsDirectory dsDirectory = null;
	private static final String[] emptyArray = new String[0];
	
	
	private String rootFolder = null;
	private DataConfig dataConfig = null;
	
	private static FileSystem fs = null;
	private static Configuration conf = null;
	
	private DsDirectory()
	{
		dataConfig = new DataConfig();
		conf = new Configuration();
		try
		{
			fs = FileSystem.get(conf);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		rootFolder = dataConfig.get("chukwa.engine.dsDirectory.rootFolder");
	}
	
	public static DsDirectory getInstance()
	{
		synchronized(lock)
		{
			if (dsDirectory == null)
			{
				dsDirectory = new DsDirectory();
			}
		}
		return dsDirectory;
	}
	
	public String[] list(String cluster)
	throws DataSourceException
	{
		List<String> datasources = new ArrayList<String>();
		try
		{
			FileStatus[] fileStat = fs.listStatus(new Path(rootFolder+cluster));
			
			for (FileStatus fstat : fileStat)
			{
				if (fstat.isDir())
				{
					datasources.add(fstat.getPath().getName());
				}
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
			throw new DataSourceException(e);
		}
		return datasources.toArray(emptyArray);
	}
	
	public static void main(String[] args) throws DataSourceException
	{
		DsDirectory dsd = DsDirectory.getInstance();
		String[] dss = dsd.list("localhost");
		for (String d : dss)
		{
			System.out.println(d);
		}
	}
}
