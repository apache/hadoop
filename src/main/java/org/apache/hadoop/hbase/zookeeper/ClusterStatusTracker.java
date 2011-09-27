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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Tracker on cluster settings up in zookeeper.
 * This is not related to {@link ClusterStatus}.  That class is a data structure
 * that holds snapshot of current view on cluster.  This class is about tracking
 * cluster attributes up in zookeeper.
 *
 */
public class ClusterStatusTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(ClusterStatusTracker.class);

  /**
   * Creates a cluster status tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public ClusterStatusTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.clusterStateZNode, abortable);
  }

  /**
   * Checks if cluster is up.
   * @return true if root region location is available, false if not
   */
  public boolean isClusterUp() {
    return super.getData(false) != null;
  }

  /**
   * Sets the cluster as up.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterUp()
  throws KeeperException {
    byte [] upData = Bytes.toBytes(new java.util.Date().toString());
    try {
      ZKUtil.createAndWatch(watcher, watcher.clusterStateZNode, upData);
    } catch(KeeperException.NodeExistsException nee) {
      ZKUtil.setData(watcher, watcher.clusterStateZNode, upData);
    }
  }

  /**
   * Sets the cluster as down by deleting the znode.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterDown()
  throws KeeperException {
    try {
      ZKUtil.deleteNode(watcher, watcher.clusterStateZNode);
    } catch(KeeperException.NoNodeException nne) {
      LOG.warn("Attempted to set cluster as down but already down, cluster " +
          "state node (" + watcher.clusterStateZNode + ") not found");
    }
  }
}
