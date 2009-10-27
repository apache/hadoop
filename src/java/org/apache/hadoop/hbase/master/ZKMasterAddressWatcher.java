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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;


/**
 * ZooKeeper watcher for the master address.  Also watches the cluster state
 * flag so will shutdown this master if cluster has been shutdown.
 * <p>Used by the Master.  Waits on the master address ZNode delete event.  When
 * multiple masters are brought up, they race to become master by writing their
 * address to ZooKeeper. Whoever wins becomes the master, and the rest wait for
 * that ephemeral node in ZooKeeper to evaporate (meaning the master went down),
 * at which point they try to write their own address to become the new master.
 */
class ZKMasterAddressWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKMasterAddressWatcher.class);
  private final ZooKeeperWrapper zookeeper;
  private final AtomicBoolean requestShutdown;

  /**
   * Create this watcher using passed ZooKeeperWrapper instance.
   * @param zk ZooKeeper
   * @param requestShutdown Flag to set to request shutdown.
   */
  ZKMasterAddressWatcher(final ZooKeeperWrapper zk, final AtomicBoolean flag) {
    this.requestShutdown = flag;
    this.zookeeper = zk;
  }

  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public synchronized void process (WatchedEvent event) {
    EventType type = event.getType();
    LOG.debug(("Got event " + type + " with path " + event.getPath()));
    if (type.equals(EventType.NodeDeleted)) {
      if (event.getPath().equals(this.zookeeper.clusterStateZNode)) {
        LOG.info("Cluster shutdown while waiting, shutting down" +
          " this master.");
        this.requestShutdown.set(true);
      } else {
        LOG.debug("Master address ZNode deleted, notifying waiting masters");
        notifyAll();
      }
    } else if(type.equals(EventType.NodeCreated) && 
        event.getPath().equals(this.zookeeper.clusterStateZNode)) {
      LOG.debug("Resetting watch on cluster state node.");
      this.zookeeper.setClusterStateWatch(this);
    }
  }

  /**
   * Wait for master address to be available. This sets a watch in ZooKeeper and
   * blocks until the master address ZNode gets deleted.
   */
  public synchronized void waitForMasterAddressAvailability() {
    while (zookeeper.readMasterAddress(this) != null) {
      try {
        LOG.debug("Waiting for master address ZNode to be deleted " +
          "(Also watching cluster state node)");
        this.zookeeper.setClusterStateWatch(this);
        wait();
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Write address to zookeeper.  Parks here until we successfully write our
   * address (or until cluster shutdown).
   * @param address Address whose format is HServerAddress.toString
   */
  void writeAddressToZooKeeper(final HServerAddress address) {
    while (true) {
      waitForMasterAddressAvailability();
      // Check if we need to shutdown instead of taking control
      if (this.requestShutdown.get()) return;
      if(this.zookeeper.writeMasterAddress(address)) {
        this.zookeeper.setClusterState(true);
        // Watch our own node
        this.zookeeper.readMasterAddress(this);
        return;
      }
    }
  }
}