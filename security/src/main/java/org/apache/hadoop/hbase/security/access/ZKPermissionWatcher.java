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

package org.apache.hadoop.hbase.security.access;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * Handles synchronization of access control list entries and updates
 * throughout all nodes in the cluster.  The {@link AccessController} instance
 * on the {@code _acl_} table regions, creates a znode for each table as
 * {@code /hbase/acl/tablename}, with the znode data containing a serialized
 * list of the permissions granted for the table.  The {@code AccessController}
 * instances on all other cluster hosts watch the znodes for updates, which
 * trigger updates in the {@link TableAuthManager} permission cache.
 */
public class ZKPermissionWatcher extends ZooKeeperListener {
  private static Log LOG = LogFactory.getLog(ZKPermissionWatcher.class);
  // parent node for permissions lists
  static final String ACL_NODE = "acl";
  TableAuthManager authManager;
  String aclZNode;

  public ZKPermissionWatcher(ZooKeeperWatcher watcher,
      TableAuthManager authManager, Configuration conf) {
    super(watcher);
    this.authManager = authManager;
    String aclZnodeParent = conf.get("zookeeper.znode.acl.parent", ACL_NODE);
    this.aclZNode = ZKUtil.joinZNode(watcher.baseZNode, aclZnodeParent);
  }

  public void start() throws KeeperException {
    watcher.registerListener(this);
    if (ZKUtil.watchAndCheckExists(watcher, aclZNode)) {
      List<ZKUtil.NodeAndData> existing =
          ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode);
      if (existing != null) {
        refreshNodes(existing);
      }
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(aclZNode)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper", ke);
        // only option is to abort
        watcher.abort("Zookeeper error obtaining acl node children", ke);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (aclZNode.equals(ZKUtil.getParent(path))) {
      String table = ZKUtil.getNodeName(path);
      authManager.remove(Bytes.toBytes(table));
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (aclZNode.equals(ZKUtil.getParent(path))) {
      // update cache on an existing table node
      String table = ZKUtil.getNodeName(path);
      try {
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        authManager.refreshCacheFromWritable(Bytes.toBytes(table), data);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for node "+table, ke);
        // only option is to abort
        watcher.abort("Zookeeper error getting data for node " + table, ke);
      } catch (IOException ioe) {
        LOG.error("Error reading permissions writables", ioe);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(aclZNode)) {
      // table permissions changed
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, aclZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for path "+path, ke);
        watcher.abort("Zookeeper error get node children for path "+path, ke);
      }
    }
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) {
    for (ZKUtil.NodeAndData n : nodes) {
      if (n.isEmpty()) continue;
      String path = n.getNode();
      String table = ZKUtil.getNodeName(path);
      try {
        byte[] nodeData = n.getData();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updating permissions cache from node "+table+" with data: "+
              Bytes.toStringBinary(nodeData));
        }
        authManager.refreshCacheFromWritable(Bytes.toBytes(table),
          nodeData);
      } catch (IOException ioe) {
        LOG.error("Failed parsing permissions for table '" + table +
            "' from zk", ioe);
      }
    }
  }

  /***
   * Write a table's access controls to the permissions mirror in zookeeper
   * @param tableName
   * @param permsData
   */
  public void writeToZookeeper(String tableName, 
      byte[] permsData) {
    String zkNode =
        ZKUtil.joinZNode(ZKUtil.joinZNode(watcher.baseZNode, ACL_NODE),
          tableName);
    try {
      ZKUtil.createWithParents(watcher, zkNode);
      ZKUtil.updateExistingNodeData(watcher, zkNode,
        permsData, -1);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for table '" + tableName +
          "'", e);
      watcher.abort("Failed writing node "+zkNode+" to zookeeper", e);
    }
  }
}
