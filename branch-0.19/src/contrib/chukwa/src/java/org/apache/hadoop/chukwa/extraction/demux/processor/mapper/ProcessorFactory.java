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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.HashMap;

import org.apache.log4j.Logger;



public class ProcessorFactory
{
	static Logger log = Logger.getLogger(ProcessorFactory.class);
	
	// TODO
	//	add new mapper package at the end.
	//	We should have a more generic way to do this.
	//	Ex: read from config
	//	list of alias
	//	and
	//	alias -> processor class
	
	
	private static HashMap<String,ChunkProcessor > processors =
	    new HashMap<String, ChunkProcessor>(); // registry
		
	private ProcessorFactory()
	{}
	
	public static ChunkProcessor getProcessor(String recordType)
	 throws UnknownRecordTypeException
	{
		String path = "org.apache.hadoop.chukwa.extraction.demux.processor.mapper"+recordType;
		if (processors.containsKey(recordType)) {
			return processors.get(recordType);
		} else {
			ChunkProcessor processor = null;
			try {
				processor = (ChunkProcessor)Class.forName(path).getConstructor().newInstance();
			} catch(ClassNotFoundException e) {
				throw new UnknownRecordTypeException("Unknown recordType:" + recordType, e);			
			} catch(Exception e) {
			  throw new UnknownRecordTypeException("error constructing processor", e);
			}
			
			//TODO using a ThreadSafe/reuse flag to actually decide if we want 
			// to reuse the same processor again and again
			register(recordType,processor);
			return processor;
		}
	}
	
	  /** Register a specific parser for a {@link ChunkProcessor}
	   * implementation. */
	  public static synchronized void register(String recordType,
	                                         ChunkProcessor processor) 
	  {
		  log.info("register " + processor.getClass().getName() + " for this recordType :" + recordType);
		  if (processors.containsKey(recordType))
			{
			  throw new DuplicateProcessorException("Duplicate processor for recordType:" + recordType);
			}
		  ProcessorFactory.processors.put(recordType, processor);
	  }

}
