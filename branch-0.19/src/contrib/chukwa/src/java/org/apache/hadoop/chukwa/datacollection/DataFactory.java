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

package org.apache.hadoop.chukwa.datacollection;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;

import org.apache.hadoop.chukwa.datacollection.agent.*;
import org.apache.log4j.Logger;

public class DataFactory
{
  static Logger log = Logger.getLogger(DataFactory.class);
	static final int QUEUE_SIZE_KB = 10 * 1024;
	private static DataFactory dataFactory = null;
	private ChunkQueue chunkQueue = new MemLimitQueue(QUEUE_SIZE_KB * 1024);

	static 
	{
		dataFactory = new DataFactory();
	}

	private DataFactory()
	{}

	public static DataFactory getInstance() {
		return dataFactory;
	}

	public ChunkQueue getEventQueue() {
		return chunkQueue;
	}

	/**
	 * @return empty list if file does not exist
	 * @throws IOException on other error
	 */
	public Iterator<String> getCollectors() throws IOException
	{
	  String chukwaHome = System.getenv("CHUKWA_HOME");
	  if (chukwaHome == null){
	    chukwaHome = ".";
	  }
	  log.info("setting up collectors file: " + chukwaHome + "/conf/collectors");
		File collectors = new File(chukwaHome + "/conf/collectors");
		try{
		  return new RetryListOfCollectors(collectors, 1000 * 15);//time is ms between tries
		} catch(java.io.IOException e) {
			log.error("failed to read collectors file: ", e);
			throw e;
		}
	}
}
