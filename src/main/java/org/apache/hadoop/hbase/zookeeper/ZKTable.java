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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Helper class for table state tracking for use by {@link AssignmentManager}.
 * Reads, caches and sets state up in zookeeper.  If multiple read/write
 * clients, will make for confusion.  Read-only clients other than
 * AssignmentManager interested in learning table state can use the
 * read-only utility methods {@link #isEnabledTable(ZooKeeperWatcher, String)}
 * and {@link #isDisabledTable(ZooKeeperWatcher, String)}.
 * 
 * <p>To save on trips to the zookeeper ensemble, internally we cache table
 * state.
 */
public class ZKTable {
  // A znode will exist under the table directory if it is in any of the
  // following states: {@link TableState#ENABLING} , {@link TableState#DISABLING},
  // or {@link TableState#DISABLED}.  If {@link TableState#ENABLED}, there will
  // be no entry for a table in zk.  Thats how it currently works.

  private static final Log LOG = LogFactory.getLog(ZKTable.class);
  private final ZooKeeperWatcher watcher;

  /**
   * Cache of what we found in zookeeper so we don't have to go to zk ensemble
   * for every query.  Synchronize access rather than use concurrent Map because
   * synchronization needs to span query of zk.
   */
  private final Map<String, TableState> cache =
    new HashMap<String, TableState>();

  // TODO: Make it so always a table znode. Put table schema here as well as table state.
  // Have watcher on table znode so all are notified of state or schema change.
  /**
   * States a Table can be in.
   * {@link TableState#ENABLED} is not used currently; its the absence of state
   * in zookeeper that indicates an enabled table currently.
   */
  public static enum TableState {
    ENABLED,
    DISABLED,
    DISABLING,
    ENABLING
  };

  public ZKTable(final ZooKeeperWatcher zkw) throws KeeperException {
    super();
    this.watcher = zkw;
    populateTableStates();
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @param zkw
   * @return list of disabled tables, empty list if none
   * @throws KeeperException
   */
  private void populateTableStates()
  throws KeeperException {
    synchronized (this.cache) {
      List<String> children =
        ZKUtil.listChildrenNoWatch(this.watcher, this.watcher.tableZNode);
      for (String child: children) {
        TableState state = getTableState(this.watcher, child);
        if (state != null) this.cache.put(child, state);
      }
    }
  }

  /**
   * @param zkw
   * @param child
   * @return Null or {@link TableState} found in znode.
   * @throws KeeperException
   */
  private static TableState getTableState(final ZooKeeperWatcher zkw,
      final String child)
  throws KeeperException {
    String znode = ZKUtil.joinZNode(zkw.tableZNode, child);
    byte [] data = ZKUtil.getData(zkw, znode);
    if (data == null || data.length <= 0) {
      // Null if table is enabled.
      return null;
    }
    String str = Bytes.toString(data);
    try {
      return TableState.valueOf(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str);
    }
  }

  /**
   * Sets the specified table as DISABLED in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisabledTable(String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisablingOrDisabledTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabled but was " +
          "not first in disabling state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.DISABLED);
    }
  }

  /**
   * Sets the specified table as DISABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setDisablingTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isEnabledOrDisablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to disabling but was " +
          "not first in enabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.DISABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper.  Fails silently if the
   * table is already disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setEnablingTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledOrEnablingTable(tableName)) {
        LOG.warn("Moving table " + tableName + " state to enabling but was " +
          "not first in disabled state: " + this.cache.get(tableName));
      }
      setTableState(tableName, TableState.ENABLING);
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table is already in ENABLING state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkAndSetEnablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (isEnablingTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.ENABLING);
      return true;
    }
  }

  /**
   * Sets the specified table as ENABLING in zookeeper atomically
   * If the table isn't in DISABLED state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkDisabledAndSetEnablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (!isDisabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.ENABLING);
      return true;
    }
  }

  /**
   * Sets the specified table as DISABLING in zookeeper atomically
   * If the table isn't in ENABLED state, no operation is performed
   * @param tableName
   * @return if the operation succeeds or not
   * @throws KeeperException unexpected zookeeper exception
   */
  public boolean checkEnabledAndSetDisablingTable(final String tableName)
    throws KeeperException {
    synchronized (this.cache) {
      if (!isEnabledTable(tableName)) {
        return false;
      }
      setTableState(tableName, TableState.DISABLING);
      return true;
    }
  }

  private void setTableState(final String tableName, final TableState state)
  throws KeeperException {
    String znode = ZKUtil.joinZNode(this.watcher.tableZNode, tableName);
    if (ZKUtil.checkExists(this.watcher, znode) == -1) {
      ZKUtil.createAndFailSilent(this.watcher, znode);
    }
    synchronized (this.cache) {
      ZKUtil.setData(this.watcher, znode, Bytes.toBytes(state.toString()));
      this.cache.put(tableName, state);
    }
  }

  public boolean isDisabledTable(final String tableName) {
    return isTableState(tableName, TableState.DISABLED);
  }

  /**
   * Go to zookeeper and see if state of table is {@link TableState#DISABLED}.
   * This method does not use cache as {@link #isDisabledTable(String)} does.
   * This method is for clients other than {@link AssignmentManager}
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisabledTable(final ZooKeeperWatcher zkw,
      final String tableName)
  throws KeeperException {
    TableState state = getTableState(zkw, tableName);
    return isTableState(TableState.DISABLED, state);
  }

  public boolean isDisablingTable(final String tableName) {
    return isTableState(tableName, TableState.DISABLING);
  }

  public boolean isEnablingTable(final String tableName) {
    return isTableState(tableName, TableState.ENABLING);
  }

  public boolean isEnabledTable(String tableName) {
    synchronized (this.cache) {
      // No entry in cache means enabled table.
      return !this.cache.containsKey(tableName);
    }
  }

  /**
   * Go to zookeeper and see if state of table is {@link TableState#ENABLED}.
   * This method does not use cache as {@link #isEnabledTable(String)} does.
   * This method is for clients other than {@link AssignmentManager}
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isEnabledTable(final ZooKeeperWatcher zkw,
      final String tableName)
  throws KeeperException {
    return getTableState(zkw, tableName) == null;
  }

  public boolean isDisablingOrDisabledTable(final String tableName) {
    synchronized (this.cache) {
      return isDisablingTable(tableName) || isDisabledTable(tableName);
    }
  }

  /**
   * Go to zookeeper and see if state of table is {@link TableState#DISABLING}
   * of {@link TableState#DISABLED}.
   * This method does not use cache as {@link #isEnabledTable(String)} does.
   * This method is for clients other than {@link AssignmentManager}.
   * @param zkw
   * @param tableName
   * @return True if table is enabled.
   * @throws KeeperException
   */
  public static boolean isDisablingOrDisabledTable(final ZooKeeperWatcher zkw,
      final String tableName)
  throws KeeperException {
    TableState state = getTableState(zkw, tableName);
    return isTableState(TableState.DISABLING, state) ||
      isTableState(TableState.DISABLED, state);
  }

  public boolean isEnabledOrDisablingTable(final String tableName) {
    synchronized (this.cache) {
      return isEnabledTable(tableName) || isDisablingTable(tableName);
    }
  }

  public boolean isDisabledOrEnablingTable(final String tableName) {
    synchronized (this.cache) {
      return isDisabledTable(tableName) || isEnablingTable(tableName);
    }
  }

  private boolean isTableState(final String tableName, final TableState state) {
    synchronized (this.cache) {
      TableState currentState = this.cache.get(tableName);
      return isTableState(currentState, state);
    }
  }

  private static boolean isTableState(final TableState expectedState,
      final TableState currentState) {
    return currentState != null && currentState.equals(expectedState);
  }

  /**
   * Enables the table in zookeeper.  Fails silently if the
   * table is not currently disabled in zookeeper.  Sets no watches.
   * @param tableName
   * @throws KeeperException unexpected zookeeper exception
   */
  public void setEnabledTable(final String tableName)
  throws KeeperException {
    synchronized (this.cache) {
      if (this.cache.remove(tableName) == null) {
        LOG.warn("Moving table " + tableName + " state to enabled but was " +
          "already enabled");
      }
      ZKUtil.deleteNodeFailSilent(this.watcher,
        ZKUtil.joinZNode(this.watcher.tableZNode, tableName));
    }
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   */
  public Set<String> getDisabledTables() {
    Set<String> disabledTables = new HashSet<String>();
    synchronized (this.cache) {
      Set<String> tables = this.cache.keySet();
      for (String table: tables) {
        if (isDisabledTable(table)) disabledTables.add(table);
      }
    }
    return disabledTables;
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException 
   */
  public static Set<String> getDisabledTables(ZooKeeperWatcher zkw)
  throws KeeperException {
    Set<String> disabledTables = new HashSet<String>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    for (String child: children) {
      TableState state = getTableState(zkw, child);
      if (state == TableState.DISABLED) disabledTables.add(child);
    }
    return disabledTables;
  }

  /**
   * Gets a list of all the tables set as disabled in zookeeper.
   * @return Set of disabled tables, empty Set if none
   * @throws KeeperException 
   */
  public static Set<String> getDisabledOrDisablingTables(ZooKeeperWatcher zkw)
  throws KeeperException {
    Set<String> disabledTables = new HashSet<String>();
    List<String> children =
      ZKUtil.listChildrenNoWatch(zkw, zkw.tableZNode);
    for (String child: children) {
      TableState state = getTableState(zkw, child);
      if (state == TableState.DISABLED || state == TableState.DISABLING)
        disabledTables.add(child);
    }
    return disabledTables;
  }
}
