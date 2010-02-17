/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ipc.ReplicationRegionInterface;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.ReplicationConnectionManager;
import org.apache.hadoop.hbase.replication.ReplicationZookeeperHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that handles the source of a replication stream
 * Currently does not handle more than 1 slave
 * For each slave cluster it selects a random number of peers
 * using a replication ratio. For example, if replication ration = 0.1
 * and slave cluster has 100 region servers, 10 will be selected.
 */
public class ReplicationSource extends Chore implements HConstants {

  static final Log LOG = LogFactory.getLog(ReplicationSource.class);
  private final LinkedBlockingQueue<HLog.Entry> queue =
      new LinkedBlockingQueue<HLog.Entry>();
  private final List<HLog.Entry> tempArray = new ArrayList<HLog.Entry>();
  private final HLog.Entry[] dummyArray = new HLog.Entry[0];
  private final ReplicationConnectionManager conn;
  private final ReplicationZookeeperHelper zkHelper;
  private final Configuration conf;
  private final float ratio;
  private final Random random;
  private final AtomicBoolean isReplicating;
  private final byte clusterId;

  private List<HServerAddress> currentPeers;

  /**
   * Constructor used by region servers
   * @param server the region server specialized in replication
   * @param stopper the atomic boolean to use to stop the cluster
   * @param isReplicating the atomic boolean that starts/stops replication
   * @throws IOException
   */
  public ReplicationSource(final ReplicationRegionServer server,
                           final AtomicBoolean stopper,
                           final AtomicBoolean isReplicating)
                           throws IOException {
    super(server.getThreadWakeFrequency(), stopper);
    this.conf = server.getConfiguration();
    this.conn = new ReplicationConnectionManager(this.conf);
    this.zkHelper = server.getZkHelper();
    this.ratio = this.conf.getFloat("replication.ratio", 0.1f);
    currentPeers = new ArrayList<HServerAddress>();
    this.random = new Random();
    this.clusterId = zkHelper.getClusterId();
    this.isReplicating = isReplicating;
  }

  @Override
   protected boolean initialChore() {
    this.chooseSinksForPeer(0);
    return currentPeers.size() > 0;
  }

  /**
   * Select a number of peers at random using the ratio. Mininum 1.
   * @param index
   */
  private void chooseSinksForPeer(int index) {
    this.currentPeers.clear();
    List<HServerAddress> addresses = this.zkHelper.getPeersAddresses(index);
    Map<String, HServerAddress> mapOfAdr =
        new HashMap<String, HServerAddress>();
    LOG.info("Considering " + addresses.size() +
        " rs, with ratio " + ratio);
    int nbPeers = (int)(Math.ceil (addresses.size()*ratio));
    LOG.info("Getting " + nbPeers + " rs from peer cluster # " + index);
    for(int i = 0; i < nbPeers; i++) {
      HServerAddress adr =
          addresses.get(this.random.nextInt(addresses.size()));
      while(mapOfAdr.containsKey(adr.toString())) {
        adr = addresses.get(this.random.nextInt(addresses.size()));
      }
      LOG.info("Choosing peer " + adr.toString());
      mapOfAdr.put(adr.toString(), adr);
    }
    this.currentPeers.addAll(mapOfAdr.values());
  }

  /**
   * Put a log entry in a replication queue if replication is enabled
   * @param logEntry
   */
  public void enqueueLog(HLog.Entry logEntry) {
    if(this.isReplicating.get()) {
      logEntry.getKey().setClusterId(this.clusterId);
      this.queue.add(logEntry);
    }
  }

  @Override
  protected void chore() {
    while(!super.stop.get()) {
      // Drain the edits accumulated in the queue, select a node at random
      // and send the edits. If it fails, get a new set of nodes and chose
      // a new one to replicate to.
      try {
        this.queue.drainTo(this.tempArray);
        if(this.tempArray.size() > 0) {
          HServerAddress adr =
              currentPeers.get(random.nextInt(this.currentPeers.size()));
          ReplicationRegionInterface rrs = this.conn.getHRegionConnection(adr);
          LOG.debug("Replicating " + this.tempArray.size()
              + " to " + adr.toString());
          rrs.replicateLogEntries(this.tempArray.toArray(dummyArray));
          this.tempArray.clear();
        }
        return;
      }
      catch (IOException ioe) {
        LOG.warn("Unable to replicate, retrying with a new node", ioe);

        try{
          Thread.sleep(1000);
        } catch (InterruptedException e){
          // continue
        }

        // Should wait in a backoff fashion?
        // make sure we don't retry with the same node
        chooseSinksForPeer(0);
      }
    }
  }

 
}
