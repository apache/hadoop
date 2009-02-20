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

import java.util.*;
//import java.util.concurrent.*;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.log4j.Logger;

/**
 * An event queue that blocks once a fixed upper limit of data is enqueued.
 * 
 * For now, uses the size of the data field.  Should really use estimatedSerializedSize()?
 * 
 */
public class MemLimitQueue implements ChunkQueue
{

	static Logger log = Logger.getLogger(WaitingQueue.class);
	
	private Queue<Chunk> queue = new LinkedList<Chunk>();
	private long dataSize = 0;
	private final long MAX_MEM_USAGE;

  public MemLimitQueue(int limit) {
    MAX_MEM_USAGE = limit;
  }
	
	/**
	 * @see org.apache.hadoop.chukwa.datacollection.ChunkQueue#add(org.apache.hadoop.chukwa.Chunk)
	 */
	public void add(Chunk event) throws InterruptedException
	{
	  assert event != null: "can't enqueue null chunks";
    synchronized(this) {
      while(event.getData().length  + dataSize > MAX_MEM_USAGE)
        this.wait();
      
      dataSize += event.getData().length;
      queue.add(event);
      this.notifyAll();
    }
	 
	}

	/**
	 * @see org.apache.hadoop.chukwa.datacollection.ChunkQueue#collect(java.util.List, int)
	 */
	public void collect(List<Chunk> events,int maxCount) throws InterruptedException
	{
		synchronized(this) {
		  //we can't just say queue.take() here, since we're holding a lock.
		  while(queue.isEmpty()){
		    this.wait();
		  }
		  
		  int i = 0;
		  while(!queue.isEmpty() && (i++ < maxCount)) { 
		    Chunk e = this.queue.remove();
		    dataSize -= e.getData().length;
		    events.add(e);
		  }
		  this.notifyAll();
		} 

		if (log.isDebugEnabled()) 	{
			log.debug("WaitingQueue.inQueueCount:" + queue.size() + "\tWaitingQueue.collectCount:" + events.size());
		}
	}

	public int size(){
	  return queue.size();
	}
}
