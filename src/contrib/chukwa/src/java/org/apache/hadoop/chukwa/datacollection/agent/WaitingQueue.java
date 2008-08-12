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

package org.apache.hadoop.chukwa.datacollection.agent;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.log4j.Logger;

public class WaitingQueue implements ChunkQueue
{

	static Logger log = Logger.getLogger(WaitingQueue.class);
	private BlockingQueue<Chunk> queue = new LinkedBlockingQueue<Chunk>(5);
	
	public void add(Chunk event)
	{
	  try
	  {
		this.queue.put(event);
	  }
	  catch(InterruptedException e)
	  {}//return upwards
	}

	public void add(List<Chunk> events)
	{
		this.queue.addAll(events);
  
	}

	public void collect(List<Chunk> events,int maxCount)
	{
		// Workaround to block on the queue
		try
		{
			events.add(this.queue.take());
		} 
		catch (InterruptedException e)
		{}
		this.queue.drainTo(events,maxCount-1);

		System.out.println("collect [" + Thread.currentThread().getName() + "] [" + events.size() + "]");

		if (log.isDebugEnabled())
		{
			log.debug("WaitingQueue.inQueueCount:" + queue.size() + "\tWaitingQueue.collectCount:" + events.size());
		}
	}
	
	 public int size(){
	    return queue.size();
	  }

}
