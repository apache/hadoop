/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * ZooKeeper watcher for the master address. Used by the HMaster to wait for
 * the event when master address ZNode gets deleted. When multiple masters are
 * brought up, they race to become master by writing to write their address to
 * ZooKeeper. Whoever wins becomes the master, and the rest wait for that
 * ephemeral node in ZooKeeper to get deleted (meaning the master went down), at
 * which point they try to write to it again.
 */
public class ZKMasterAddressWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKMasterAddressWatcher.class);

  private final ZooKeeperWrapper zooKeeper;
  private final HMaster master;

  /**
   * Create a watcher with a ZooKeeperWrapper instance.
   * @param master The master.
   */
  public ZKMasterAddressWatcher(HMaster master) {
    this.master = master;
    this.zooKeeper = master.getZooKeeperWrapper();
  }

  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public synchronized void process(WatchedEvent event) {
    EventType type = event.getType();
    LOG.debug(("Got event " + type + " with path " + event.getPath()));
    if (type.equals(EventType.NodeDeleted)) {
      if(event.getPath().equals(this.zooKeeper.clusterStateZNode)) {
        LOG.info("The cluster was shutdown while waiting, shutting down" +
            " this master.");
        master.shutdownRequested.set(true);
      }
      else {
        LOG.debug("Master address ZNode deleted, notifying waiting masters");
        notifyAll();
      }
    }
    else if(type.equals(EventType.NodeCreated) && 
        event.getPath().equals(this.zooKeeper.clusterStateZNode)) {
      LOG.debug("Resetting the watch on the cluster state node.");
      this.zooKeeper.setClusterStateWatch(this);
    }
  }

  /**
   * Wait for master address to be available. This sets a watch in ZooKeeper and
   * blocks until the master address ZNode gets deleted.
   */
  public synchronized void waitForMasterAddressAvailability() {
    while (zooKeeper.readMasterAddress(this) != null) {
      try {
        LOG.debug("Waiting for master address ZNode to be deleted " +
            "and watching the cluster state node");
        this.zooKeeper.setClusterStateWatch(this);
        wait();
      } catch (InterruptedException e) {
      }
    }
  }
}
