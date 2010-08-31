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

import java.util.List;

import org.apache.zookeeper.KeeperException;

/**
 * Helper class for table disable tracking in zookeeper.
 * <p>
 * The node /disabled will contain a child node for every table which should be
 * disabled, for example, /disabled/table.
 */
public class ZKTableDisable {

  /**
   * Sets the specified table as disabled in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param zkw
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void disableTable(ZooKeeperWatcher zkw, String tableName)
  throws KeeperException {
    ZKUtil.createAndFailSilent(zkw, ZKUtil.joinZNode(zkw.tableZNode,
        tableName));
  }

  /**
   * Unsets the specified table as disabled in zookeeper.  Fails silently if the
   * table is not currently disabled in zookeeper.  Sets no watches.
   * @param zkw
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void undisableTable(ZooKeeperWatcher zkw, String tableName)
  throws KeeperException {
    ZKUtil.deleteNodeFailSilent(zkw, ZKUtil.joinZNode(zkw.tableZNode,
        tableName));
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @param zkw
   * @return list of disabled tables, empty list if none
   * @throws KeeperException
   */
  public static List<String> getDisabledTables(ZooKeeperWatcher zkw)
  throws KeeperException {
    return ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
  }
}