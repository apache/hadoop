/**
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
package org.apache.hadoop.hbase.master;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Handles everything on master-side related to master election.
 *
 * <p>Listens and responds to ZooKeeper notifications on the master znode,
 * both <code>nodeCreated</code> and <code>nodeDeleted</code>.
 *
 * <p>Contains blocking methods which will hold up backup masters, waiting
 * for the active master to fail.
 *
 * <p>This class is instantiated in the HMaster constructor and the method
 * {@link #blockUntilBecomingActiveMaster()} is called to wait until becoming
 * the active master of the cluster.
 */
class ActiveMasterManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(ActiveMasterManager.class);

  final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);

  private final HServerAddress address;
  private final Server master;

  ActiveMasterManager(ZooKeeperWatcher watcher, HServerAddress address,
      Server master) {
    super(watcher);
    this.address = address;
    this.master = master;
  }

  @Override
  public void nodeCreated(String path) {
    if(path.equals(watcher.masterAddressZNode) && !master.isStopped()) {
      handleMasterNodeChange();
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if(path.equals(watcher.masterAddressZNode) && !master.isStopped()) {
      handleMasterNodeChange();
    }
  }

  /**
   * Handle a change in the master node.  Doesn't matter whether this was called
   * from a nodeCreated or nodeDeleted event because there are no guarantees
   * that the current state of the master node matches the event at the time of
   * our next ZK request.
   *
   * <p>Uses the watchAndCheckExists method which watches the master address node
   * regardless of whether it exists or not.  If it does exist (there is an
   * active master), it returns true.  Otherwise it returns false.
   *
   * <p>A watcher is set which guarantees that this method will get called again if
   * there is another change in the master node.
   */
  private void handleMasterNodeChange() {
    // Watch the node and check if it exists.
    try {
      synchronized(clusterHasActiveMaster) {
        if(ZKUtil.watchAndCheckExists(watcher, watcher.masterAddressZNode)) {
          // A master node exists, there is an active master
          LOG.debug("A master is now available");
          clusterHasActiveMaster.set(true);
        } else {
          // Node is no longer there, cluster does not have an active master
          LOG.debug("No master available. notifying waiting threads");
          clusterHasActiveMaster.set(false);
          // Notify any thread waiting to become the active master
          clusterHasActiveMaster.notifyAll();
        }
      }
    } catch (KeeperException ke) {
      master.abort("Received an unexpected KeeperException, aborting", ke);
    }
  }

  /**
   * Block until becoming the active master.
   *
   * Method blocks until there is not another active master and our attempt
   * to become the new active master is successful.
   *
   * This also makes sure that we are watching the master znode so will be
   * notified if another master dies.
   * @return False if we did not start up this cluster, another
   * master did, or if a problem (zookeeper, stop flag has been set on this
   * Master)
   */
  boolean blockUntilBecomingActiveMaster() {
    boolean thisMasterStartedCluster = true;
    // Try to become the active master, watch if there is another master
    try {
      if(ZKUtil.setAddressAndWatch(watcher, watcher.masterAddressZNode,
          address)) {
        // We are the master, return
        clusterHasActiveMaster.set(true);
        return thisMasterStartedCluster;
      }
    } catch (KeeperException ke) {
      master.abort("Received an unexpected KeeperException, aborting", ke);
      return false;
    }
    // There is another active master, this is not a cluster startup
    // and we must wait until the active master dies
    LOG.info("Another master is already the active master, waiting to become " +
      "the next active master");
    clusterHasActiveMaster.set(true);
    thisMasterStartedCluster = false;
    synchronized(clusterHasActiveMaster) {
      while(clusterHasActiveMaster.get() && !master.isStopped()) {
        try {
          clusterHasActiveMaster.wait();
        } catch (InterruptedException e) {
          // We expect to be interrupted when a master dies, will fall out if so
          LOG.debug("Interrupted waiting for master to die", e);
        }
      }
      if(master.isStopped()) {
        return thisMasterStartedCluster;
      }
      // Try to become active master again now that there is no active master
      blockUntilBecomingActiveMaster();
    }
    return thisMasterStartedCluster;
  }

  /**
   * @return True if cluster has an active master.
   */
  public boolean isActiveMaster() {
    return this.clusterHasActiveMaster.get();
  }

  public void stop() {
    try {
      // If our address is in ZK, delete it on our way out
      HServerAddress zkAddress =
        ZKUtil.getDataAsAddress(watcher, watcher.masterAddressZNode);
      // TODO: redo this to make it atomic (only added for tests)
      if(zkAddress != null &&
          zkAddress.equals(address)) {
        ZKUtil.deleteNode(watcher, watcher.masterAddressZNode);
      }
    } catch (KeeperException e) {
      watcher.error("Error deleting our own master address node", e);
    }
  }
}