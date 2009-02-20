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

package org.apache.hadoop.chukwa.datacollection.test;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.*;
import org.apache.hadoop.chukwa.datacollection.agent.*;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;

import java.util.*;

/**
 * Output events to stdout.
 * Intended for debugging use.
 *
 */
public class ConsoleOutConnector extends Thread implements Connector {
  
  final ChukwaAgent agent;
  volatile boolean shutdown;
  final boolean silent;
  

  public ConsoleOutConnector(ChukwaAgent a) {
    this(a, false);
  }
  
  public ConsoleOutConnector(ChukwaAgent a, boolean silent)
  {
    agent = a;
    this.silent = silent;
  }
  
  public void run()
  {
    try{
      System.out.println("console connector started");
      ChunkQueue eventQueue = DataFactory.getInstance().getEventQueue();
      if(!silent)
        System.out.println("-------------------");
      
      while(!shutdown)
      {
        List<Chunk> evts = new ArrayList<Chunk>();
        eventQueue.collect(evts, 1);
        
        for(Chunk e: evts)
        {
          if(!silent) {
            System.out.println("Console out connector got event at offset " + e.getSeqID());
            System.out.println("data type was " + e.getDataType());
            if(e.getData().length > 1000)
              System.out.println("data length was " + e.getData().length+ ", not printing");
            else
              System.out.println(new String(e.getData()));
          }
          
          agent.reportCommit(e.getInitiator(), e.getSeqID());
         
          if(!silent)
            System.out.println("-------------------");
        }
      }
    }
    catch(InterruptedException e)
    {} //thread is about to exit anyway
  }

  public void shutdown()
  {
    shutdown = true;
    this.interrupt();
  }

}
