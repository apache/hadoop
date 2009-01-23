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

package org.apache.hadoop.chukwa.datacollection.connector.http;

/**
 * This class is responsible for setting up a {@link HttpConnectorClient} with a  collectors
 * and then repeatedly calling its send function which encapsulates the work of setting up the
 * connection with the appropriate collector and then collecting and sending the {@link Chunk}s 
 * from the global {@link ChunkQueue} which where added by {@link Adaptors}. We want to separate
 * the long living (i.e. looping) behavior from the ConnectorClient because we also want to be able
 * to use the HttpConnectorClient for its add and send API in arbitrary applications that want to send
 * chunks without an {@link LocalAgent} daemon.
 * 
 * * <p>
 * On error, tries the list of available collectors, pauses for a minute, and then repeats.
 * </p>
 * <p> Will wait forever for collectors to come up. </p>
 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.sender.*;
import org.apache.log4j.Logger;


public class HttpConnector implements Connector, Runnable  {
  
	static Logger log = Logger.getLogger(HttpConnector.class);

  static Timer statTimer = null;
  static volatile int chunkCount = 0;
  static final int MAX_SIZE_PER_POST = 2*1024*1024;
  static final int MIN_POST_INTERVAL= 5 * 1000;
  static ChunkQueue chunkQueue;
  
  ChukwaAgent agent;
  String argDestination = null;
  
  private volatile boolean stopMe = false;
  private boolean reloadConfiguration = false;
  private Iterator<String> collectors = null;
  protected ChukwaSender connectorClient = null;
  
  static{
    statTimer = new Timer();
    chunkQueue = DataFactory.getInstance().getEventQueue();
    statTimer.schedule(new TimerTask() {
      public void run() {
        int count = chunkCount;
        chunkCount = 0;           
        log.info("# http chunks ACK'ed since last report: " + count );
      }
    }, 100,60*1000);
  }
  
	public HttpConnector(ChukwaAgent agent)	{
		this.agent = agent;
	}

	 public HttpConnector(ChukwaAgent agent, String destination) {
	    this.agent = agent;
	    this.argDestination = destination;

      log.info("Setting HTTP Connector URL manually using arg passed to Agent: " + destination);
	  }
	
	public void start() 	{
		(new Thread(this, "HTTP post thread")).start();
	}
	
	public void shutdown(){
	  stopMe = true;
	}
	
	public void run(){
		log.info("HttpConnector started at time:" + System.currentTimeMillis());

		Iterator<String> destinations = null;

		// build a list of our destinations from collectors
		try{
			destinations = DataFactory.getInstance().getCollectorURLs();
		} catch (IOException e){
			log.error("Failed to retreive list of collectors from " +
					"conf/collectors file", e);
		}

		connectorClient = new ChukwaHttpSender(agent.getConfiguration());

		if (argDestination != null) 
		{
			ArrayList<String> tmp = new ArrayList<String>();
			tmp.add(argDestination);
			collectors = tmp.iterator();
			connectorClient.setCollectors(collectors);
			log.info("using collector specified at agent runtime: " + argDestination);
		} 
		else if (destinations != null && destinations.hasNext()) 
		{
			collectors = destinations;
			connectorClient.setCollectors(destinations);
			log.info("using collectors from collectors file");
		} 
		else {
			log.error("No collectors specified, exiting (and taking agent with us).");
			agent.shutdown(true);//error is unrecoverable, so stop hard.
			return;
		}

		try {
			long lastPost = System.currentTimeMillis();
			while(!stopMe) {
				List<Chunk> newQueue = new ArrayList<Chunk>();
				try {
					//get all ready chunks from the chunkQueue to be sent
					chunkQueue.collect(newQueue,MAX_SIZE_PER_POST); //FIXME: should really do this by size

				} catch(InterruptedException e) {
					System.out.println("thread interrupted during addChunks(ChunkQueue)");
					Thread.currentThread().interrupt();
					break;
				}
				int toSend = newQueue.size();
				List<ChukwaHttpSender.CommitListEntry> results = connectorClient.send(newQueue);
				log.info("sent " +toSend + " chunks, got back " + results.size() + " acks");
				//checkpoint the chunks which were committed
				for(ChukwaHttpSender.CommitListEntry cle : results) {
					agent.reportCommit(cle.adaptor, cle.uuid);
					chunkCount++;
				}

				if (reloadConfiguration)
				{
					connectorClient.setCollectors(collectors);
					log.info("Resetting colectors");
					reloadConfiguration = false;
				}

				long now = System.currentTimeMillis();
				if( now - lastPost < MIN_POST_INTERVAL )  
					Thread.sleep(now - lastPost);  //wait for stuff to accumulate
				lastPost = now;
			} //end of try forever loop
			log.info("received stop() command so exiting run() loop to shutdown connector");
		} catch(OutOfMemoryError e) {
			log.warn("Bailing out",e);
			System.exit(-1);
		} catch(InterruptedException e) {
			//do nothing, let thread die.
			log.warn("Bailing out",e);
			System.exit(-1);
		}catch(java.io.IOException e) {
			log.error("connector failed; shutting down agent");
			agent.shutdown(true);
		}
	}

	@Override
	public void reloadConfiguration()
	{
		reloadConfiguration = true;
		Iterator<String> destinations = null;
		  
	 	// build a list of our destinations from collectors
	 	try{
	    destinations = DataFactory.getInstance().getCollectorURLs();
	  } catch (IOException e){
	    log.error("Failed to retreive list of collectors from conf/collectors file", e);
	  }
	  if (destinations != null && destinations.hasNext()) 
	  {
		  collectors = destinations;
	  }
    
	}
}
