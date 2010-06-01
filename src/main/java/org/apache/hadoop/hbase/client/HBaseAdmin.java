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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Provides administrative functions for HBase
 */
public class HBaseAdmin {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
//  private final HConnection connection;
  final HConnection connection;
  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  private volatile HMasterInterface master;

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   */
  public HBaseAdmin(Configuration conf) throws MasterNotRunningException {
    this.connection = HConnectionManager.getConnection(conf);
    this.conf = conf;
    this.pause = conf.getLong("hbase.client.pause", 30 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    this.master = connection.getMaster();
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    return connection;
  }

  /**
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException if the master is not running
   */
  public HMasterInterface getMaster() throws MasterNotRunningException{
    return this.connection.getMaster();
  }

  /** @return - true if the master server is running */
  public boolean isMasterRunning() {
    return this.connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException if the master is not running
   */
  public boolean tableExists(final String tableName)
  throws MasterNotRunningException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException if the master is not running
   */
  public boolean tableExists(final byte [] tableName)
  throws MasterNotRunningException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    return connection.tableExists(tableName);
  }

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
  public HTableDescriptor[] listTables() throws IOException {
    return this.connection.listTables();
  }


  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
  throws IOException {
    return this.connection.getHTableDescriptor(tableName);
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length)
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  /**
   * Creates a new table.
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new table with the specified number of regions.  The start key
   * specified will become the end key of the first region of the table, and
   * the end key specified will become the start key of the last region of the
   * table (the first region has a null start key and the last region has a
   * null end key).
   *
   * BigInteger math will be used to divide the key range specified into
   * enough segments to make the required number of total regions.
   *
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [] startKey,
      byte [] endKey, int numRegions)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if(Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if(splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the
   * specified split keys.  The total number of regions created will be the
   * number of split keys plus one (the first region has a null start key and
   * the last region has a null end key).
   * Synchronous operation.
   *
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   *
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    if(splitKeys != null && splitKeys.length > 1) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte [] lastKey = null;
      for(byte [] splitKey : splitKeys) {
        if(lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, found duplicate");
        }
        lastKey = splitKey;
      }
    }
    createTableAsync(desc, splitKeys);
    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // Wait for new table to come on-line
        connection.locateRegion(desc.getName(), HConstants.EMPTY_START_ROW);
        break;

      } catch (RegionException e) {
        if (tries == numRetries - 1) {
          // Ran out of tries
          throw e;
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation.
   *
   * @param desc table descriptor for table
   *
   * @throws IllegalArgumentException Bad table name.
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(desc.getName());
    try {
      this.master.createTable(desc, splitKeys);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table.
   * Synchronous operation.
   *
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    try {
      this.master.deleteTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
    final int batchCount = this.conf.getInt("hbase.admin.scanner.caching", 10);
    // Wait until first region is deleted
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());
    HRegionInfo info = new HRegionInfo();
    for (int tries = 0; tries < numRetries; tries++) {
      long scannerId = -1L;
      try {
        Scan scan = new Scan().addColumn(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
        scannerId = server.openScanner(
          firstMetaServer.getRegionInfo().getRegionName(), scan);
        // Get a batch at a time.
        Result [] values = server.next(scannerId, batchCount);
        if (values == null || values.length == 0) {
          break;
        }
        boolean found = false;
        for (Result r : values) {
          NavigableMap<byte[], byte[]> infoValues =
              r.getFamilyMap(HConstants.CATALOG_FAMILY);
          for (Map.Entry<byte[], byte[]> e : infoValues.entrySet()) {
            if (Bytes.equals(e.getKey(), HConstants.REGIONINFO_QUALIFIER)) {
              info = (HRegionInfo) Writables.getWritable(e.getValue(), info);
              if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
                found = true;
              } else {
                found = false;
                break;
              }
            }
          }
        }
        if (!found) {
          break;
        }
      } catch (IOException ex) {
        if(tries == numRetries - 1) {           // no more tries left
          if (ex instanceof RemoteException) {
            ex = RemoteExceptionHandler.decodeRemoteException((RemoteException) ex);
          }
          throw ex;
        }
      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
          } catch (Exception ex) {
            LOG.warn(ex);
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }
    // Delete cached information to prevent clients from using old locations
    HConnectionManager.deleteConnectionInfo(conf, false);
    LOG.info("Deleted " + Bytes.toString(tableName));
  }



  /**
   * Brings a table on-line (enables it).
   * Synchronous operation.
   *
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void enableTable(final String tableName) throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it).
   * Synchronous operation.
   *
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void enableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }

    // Wait until all regions are enabled
    boolean enabled = false;
    for (int tries = 0; tries < this.numRetries; tries++) {

      try {
        this.master.enableTable(tableName);
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }
      enabled = isTableEnabled(tableName);
      if (enabled) break;
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep + "ms, waiting for all regions to be " +
          "enabled in " + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for all regions to be enabled from " +
          Bytes.toString(tableName));
      }
    }
    if (!enabled)
      throw new IOException("Unable to enable table " +
        Bytes.toString(tableName));
    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * Synchronous operation.
   *
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   */
  public void disableTable(final String tableName) throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * Synchronous operation.
   *
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   */
  public void disableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }

    // Wait until all regions are disabled
    boolean disabled = false;
    for (int tries = 0; tries < this.numRetries; tries++) {
      try {
        this.master.disableTable(tableName);
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }
      disabled = isTableDisabled(tableName);
      if (disabled) break;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for all regions to be disabled from " +
          Bytes.toString(tableName));
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for all regions to be disabled from " +
          Bytes.toString(tableName));
      }
    }
    if (!disabled) {
      throw new RegionException("Retries exhausted, it took too long to wait"+
        " for the table " + Bytes.toString(tableName) + " to be disabled.");
    }
    LOG.info("Disabled " + Bytes.toString(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return connection.isTableEnabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return connection.isTableDisabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return connection.isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column)
  throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    try {
      this.master.addColumn(tableName, column);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName)
  throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    try {
      this.master.deleteColumn(tableName, columnName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, final String columnName,
      HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName),
      descriptor);
  }

  /**
   * Modify an existing column family on a table.
   * Asynchronous operation.
   *
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(final byte [] tableName, final byte [] columnName,
    HColumnDescriptor descriptor)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    try {
      this.master.modifyColumn(tableName, columnName, descriptor);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Close a region. For expert-admins.
   * Asynchronous operation.
   *
   * @param regionname region name to close
   * @param args Optional server name.  Otherwise, we'll send close to the
   * server registered in .META.
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final Object... args)
  throws IOException {
    closeRegion(Bytes.toBytes(regionname), args);
  }

  /**
   * Close a region.  For expert-admins.
   * Asynchronous operation.
   *
   * @param regionname region name to close
   * @param args Optional server name.  Otherwise, we'll send close to the
   * server registered in .META.
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte [] regionname, final Object... args)
  throws IOException {
    // Be careful. Must match the handler over in HMaster at MODIFY_CLOSE_REGION
    int len = (args == null)? 0: args.length;
    int xtraArgsCount = 1;
    Object [] newargs = new Object[len + xtraArgsCount];
    newargs[0] = regionname;
    if(args != null) {
      System.arraycopy(args, 0, newargs, xtraArgsCount, len);
    }
    modifyTable(HConstants.META_TABLE_NAME, HConstants.Modify.CLOSE_REGION,
      newargs);
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   */
  public void flush(final String tableNameOrRegionName) throws IOException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Flush a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   */
  public void flush(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_FLUSH);
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(final String tableNameOrRegionName) throws IOException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   */
  public void compact(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_COMPACT);
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(final String tableNameOrRegionName)
  throws IOException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Major compact a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   */
  public void majorCompact(final byte [] tableNameOrRegionName)
  throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_MAJOR_COMPACT);
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   */
  public void split(final String tableNameOrRegionName) throws IOException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Split a table or an individual region.
   * Asynchronous operation.
   *
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   */
  public void split(final byte [] tableNameOrRegionName) throws IOException {
    modifyTable(tableNameOrRegionName, HConstants.Modify.TABLE_SPLIT);
  }

  /*
   * Call modifyTable using passed tableName or region name String.  If no
   * such table, presume we have been passed a region name.
   * @param tableNameOrRegionName
   * @param op
   * @throws IOException
   */
  private void modifyTable(final byte [] tableNameOrRegionName,
      final HConstants.Modify op)
  throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    byte [] tableName = tableExists(tableNameOrRegionName)?
      tableNameOrRegionName: null;
    byte [] regionName = tableName == null? tableNameOrRegionName: null;
    Object [] args = regionName == null? null: new byte [][] {regionName};
    modifyTable(tableName == null? null: tableName, op, args);
  }

  /**
   * Modify an existing table, more IRB friendly version.
   * Asynchronous operation.
   *
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HTableDescriptor htd)
  throws IOException {
    modifyTable(tableName, HConstants.Modify.TABLE_SET_HTD, htd);
  }

  /**
   * Modify an existing table.
   * Asynchronous operation.
   *
   * @param tableName name of table.  May be null if we are operating on a
   * region.
   * @param op table modification operation
   * @param args operation specific arguments
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(final byte [] tableName, HConstants.Modify op,
      Object... args)
      throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    // Let pass if its a catalog table.  Used by admins.
    if (tableName != null && !MetaUtils.isMetaTableName(tableName)) {
      // This will throw exception
      HTableDescriptor.isLegalTableName(tableName);
    }
    Writable[] arr = null;
    try {
      switch (op) {
      case TABLE_SET_HTD:
        if (args == null || args.length < 1 ||
            !(args[0] instanceof HTableDescriptor)) {
          throw new IllegalArgumentException("SET_HTD requires a HTableDescriptor");
        }
        arr = new Writable[1];
        arr[0] = (HTableDescriptor)args[0];
        this.master.modifyTable(tableName, op, arr);
        break;

      case TABLE_COMPACT:
      case TABLE_SPLIT:
      case TABLE_MAJOR_COMPACT:
      case TABLE_FLUSH:
        if (args != null && args.length > 0) {
          arr = new Writable[1];
          if (args[0] instanceof byte[]) {
            arr[0] = new ImmutableBytesWritable((byte[])args[0]);
          } else if (args[0] instanceof ImmutableBytesWritable) {
            arr[0] = (ImmutableBytesWritable)args[0];
          } else if (args[0] instanceof String) {
            arr[0] = new ImmutableBytesWritable(Bytes.toBytes((String)args[0]));
          } else {
            throw new IllegalArgumentException("Requires byte[], String, or" +
              "ImmutableBytesWritable");
          }
        }
        this.master.modifyTable(tableName, op, arr);
        break;

      case CLOSE_REGION:
        if (args == null || args.length < 1) {
          throw new IllegalArgumentException("Requires at least a region name");
        }
        arr = new Writable[args.length];
        for (int i = 0; i < args.length; i++) {
          if (args[i] instanceof byte[]) {
            arr[i] = new ImmutableBytesWritable((byte[])args[i]);
          } else if (args[i] instanceof ImmutableBytesWritable) {
            arr[i] = (ImmutableBytesWritable)args[i];
          } else if (args[i] instanceof String) {
            arr[i] = new ImmutableBytesWritable(Bytes.toBytes((String)args[i]));
          } else if (args[i] instanceof Boolean) {
            arr[i] = new BooleanWritable((Boolean) args[i]);
          } else {
            throw new IllegalArgumentException("Requires byte [] or " +
              "ImmutableBytesWritable, not " + args[i]);
          }
        }
        this.master.modifyTable(tableName, op, arr);
        break;

      default:
        throw new IOException("unknown modifyTable op " + op);
      }
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /**
   * Shuts down the HBase instance
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    try {
      this.master.shutdown();
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    } finally {
      this.master = null;
    }
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    return this.master.getClusterStatus();
  }

  private HRegionLocation getFirstMetaServerForTable(final byte [] tableName)
  throws IOException {
    return connection.locateRegion(HConstants.META_TABLE_NAME,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   *
   * @param conf system configuration
   * @throws MasterNotRunningException if a remote or network exception occurs
   */
  public static void checkHBaseAvailable(Configuration conf)
  throws MasterNotRunningException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);
    copyOfConf.setInt("hbase.client.retries.number", 1);
    new HBaseAdmin(copyOfConf);
  }
}
