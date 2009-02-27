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

package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * A shared thread used by all FileTailingAdaptors. 
 * 
 * For now, it tries each file in succession. If it gets through every
 * file within two seconds, and no more data remains, it will sleep.
 * 
 * If there was still data available in any file, the adaptor will loop again.
 *
 */
class FileTailer extends Thread {
  static Logger log = Logger.getLogger(FileTailer.class);
  
  private  List<FileTailingAdaptor> adaptors;
  private volatile boolean isRunning = true;
  ChunkQueue eq; //not private -- useful for file tailing adaptor classes
  
  /**
   * How often to tail each file.
   */
  int DEFAULT_SAMPLE_PERIOD_MS = 1000* 2;
  int SAMPLE_PERIOD_MS = DEFAULT_SAMPLE_PERIOD_MS;
  private static Configuration conf = null;
  
  FileTailer() {
	if (conf == null) {
	  ChukwaAgent agent = ChukwaAgent.getAgent();
	  if (agent != null) {
		conf = agent.getConfiguration();
    	if (conf != null) {
    	  SAMPLE_PERIOD_MS= conf.getInt("chukwaAgent.adaptor.context.switch.time", DEFAULT_SAMPLE_PERIOD_MS);
        }
	  }
	}
    eq = DataFactory.getInstance().getEventQueue();
     
    //iterations are much more common than adding a new adaptor
    adaptors = new CopyOnWriteArrayList<FileTailingAdaptor>();

    this.setDaemon(true);
    start();//start the file-tailing thread
  }
   
  //called by FileTailingAdaptor, only
   void startWatchingFile(FileTailingAdaptor f) {
     adaptors.add(f);
   }

   //called by FileTailingAdaptor, only
   void stopWatchingFile(FileTailingAdaptor f) {
     adaptors.remove(f);
   }
   
  public void run() {
    while (isRunning) {
      try {
        boolean shouldISleep = true;
        long startTime = System.currentTimeMillis();
        for (FileTailingAdaptor f : adaptors) {
          boolean hasMoreData = f.tailFile(eq);
          shouldISleep &= !hasMoreData;
        }
        long timeToReadFiles = System.currentTimeMillis() - startTime;
        if (timeToReadFiles < SAMPLE_PERIOD_MS || shouldISleep) {
          Thread.sleep(SAMPLE_PERIOD_MS);
        }
      } catch (Throwable e) {
        log.warn("Exception in FileTailer, while loop", e);
        e.printStackTrace();
      }
    }
  }
  
  
}
