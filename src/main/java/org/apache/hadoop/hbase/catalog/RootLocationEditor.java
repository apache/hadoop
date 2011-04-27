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
package org.apache.hadoop.hbase.catalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Makes changes to the location of <code>-ROOT-</code> in ZooKeeper.
 */
public class RootLocationEditor {
  private static final Log LOG = LogFactory.getLog(RootLocationEditor.class);

  /**
   * Deletes the location of <code>-ROOT-</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void deleteRootLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    LOG.info("Unsetting ROOT region location in ZooKeeper");
    try {
      // Just delete the node.  Don't need any watches, only we will create it.
      ZKUtil.deleteNode(zookeeper, zookeeper.rootServerZNode);
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }

  /**
   * Sets the location of <code>-ROOT-</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param location The server hosting <code>-ROOT-</code>
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setRootLocation(ZooKeeperWatcher zookeeper,
      final ServerName location)
  throws KeeperException {
    LOG.info("Setting ROOT region location in ZooKeeper as " + location);
    try {
      ZKUtil.createAndWatch(zookeeper, zookeeper.rootServerZNode,
        Bytes.toBytes(location.toString()));
    } catch(KeeperException.NodeExistsException nee) {
      LOG.debug("ROOT region location already existed, updated location");
      ZKUtil.setData(zookeeper, zookeeper.rootServerZNode,
          Bytes.toBytes(location.toString()));
    }
  }
}