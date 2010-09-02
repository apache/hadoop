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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Cluster connection.
 * {@link HConnectionManager} manages instances of this class.
 */
public interface HConnection {
  /**
   * Retrieve ZooKeeperWatcher used by the connection.
   * @return ZooKeeperWatcher handle being used by the connection.
   * @throws IOException if a remote or network exception occurs
   */
  public ZooKeeperWatcher getZooKeeperWatcher() throws IOException;

  /**
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public HMasterInterface getMaster()
  throws MasterNotRunningException, ZooKeeperConnectionException;

  /** @return - true if the master server is running */
  public boolean isMasterRunning()
  throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * A table that isTableEnabled == false and isTableDisabled == false
   * is possible. This happens when a table has a lot of regions
   * that must be processed.
   * @param tableName table name
   * @return true if the table is enabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException;

  /**
   * @param tableName table name
   * @return true if the table is disabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException;

  /**
   * @param tableName table name
   * @return true if all regions of the table are available, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException;

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException;

  /**
   * @param tableName table name
   * @return table metadata
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getHTableDescriptor(byte[] tableName)
  throws IOException;

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation locateRegion(final byte [] tableName,
      final byte [] row)
  throws IOException;

  /**
   * Allows flushing the region cache.
   */
  public void clearRegionCache();

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation relocateRegion(final byte [] tableName,
      final byte [] row)
  throws IOException;

  /**
   * Gets the location of the region of <i>regionName</i>.
   * @param regionName name of the region to locate
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation locateRegion(final byte [] regionName)
  throws IOException;

  /**
   * Gets the locations of all regions in the specified table, <i>tableName</i>.
   * @param tableName table to get regions of
   * @return list of region locations for all regions of table
   * @throws IOException
   */
  public List<HRegionLocation> locateRegions(byte[] tableName)
  throws IOException;

  /**
   * Establishes a connection to the region server at the specified address.
   * @param regionServer - the server to connect to
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionInterface getHRegionConnection(HServerAddress regionServer)
  throws IOException;

  /**
   * Establishes a connection to the region server at the specified address.
   * @param regionServer - the server to connect to
   * @param getMaster - do we check if master is alive
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionInterface getHRegionConnection(
      HServerAddress regionServer, boolean getMaster)
  throws IOException;

  /**
   * Find region location hosting passed row
   * @param tableName table name
   * @param row Row to find.
   * @param reload If true do not use cache, otherwise bypass.
   * @return Location of row.
   * @throws IOException if a remote or network exception occurs
   */
  HRegionLocation getRegionLocation(byte [] tableName, byte [] row,
    boolean reload)
  throws IOException;

  /**
   * Pass in a ServerCallable with your particular bit of logic defined and
   * this method will manage the process of doing retries with timed waits
   * and refinds of missing regions.
   *
   * @param <T> the type of the return value
   * @param callable callable to run
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
  throws IOException, RuntimeException;

  /**
   * Pass in a ServerCallable with your particular bit of logic defined and
   * this method will pass it to the defined region server.
   * @param <T> the type of the return value
   * @param callable callable to run
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
  throws IOException, RuntimeException;


  /**
   * Process a batch of Puts. Does the retries.
   * @param list A batch of Puts to process.
   * @param tableName The name of the table
   * @return Count of committed Puts.  On fault, < list.size().
   * @throws IOException if a remote or network exception occurs
   */
  public int processBatchOfRows(ArrayList<Put> list, byte[] tableName)
  throws IOException;

  /**
   * Process a batch of Deletes. Does the retries.
   * @param list A batch of Deletes to process.
   * @return Count of committed Deletes. On fault, < list.size().
   * @param tableName The name of the table
   * @throws IOException if a remote or network exception occurs
   */
  public int processBatchOfDeletes(List<Delete> list, byte[] tableName)
  throws IOException;

  public void processBatchOfPuts(List<Put> list,
                                 final byte[] tableName, ExecutorService pool) throws IOException;

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances within this
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch.
   */
  public void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable);

  /**
   * Check whether region cache prefetch is enabled or not.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   */
  public boolean getRegionCachePrefetch(final byte[] tableName);

  /**
   * Load the region map and warm up the global region cache for the table.
   * @param tableName name of the table to perform region cache prewarm.
   * @param regions a region map.
   */
  public void prewarmRegionCache(final byte[] tableName,
      final Map<HRegionInfo, HServerAddress> regions);
}
