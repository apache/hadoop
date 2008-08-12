/**
 * Copyright 2007 The Apache Software Foundation
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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Provides administrative functions for HBase
 */
public class HBaseAdmin {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private final HConnection connection;
  private final long pause;
  private final int numRetries;
  private volatile HMasterInterface master;

  /**
   * Constructor
   * 
   * @param conf Configuration object
   * @throws MasterNotRunningException
   */
  public HBaseAdmin(HBaseConfiguration conf) throws MasterNotRunningException {
    this.connection = HConnectionManager.getConnection(conf);
    this.pause = conf.getLong("hbase.client.pause", 30 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    this.master = connection.getMaster();
  }

  /**
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException
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
   * @throws MasterNotRunningException
   */
  public boolean tableExists(final Text tableName)
  throws MasterNotRunningException {
    return tableExists(tableName.getBytes());
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException
   */
  public boolean tableExists(final String tableName)
  throws MasterNotRunningException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws MasterNotRunningException
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
   * @throws IOException
   */
  public HTableDescriptor[] listTables() throws IOException {
    return this.connection.listTables();
  }

  private long getPauseTime(int tries) {
    if (tries >= HConstants.RETRY_BACKOFF.length)
      tries = HConstants.RETRY_BACKOFF.length - 1;
    return this.pause * HConstants.RETRY_BACKOFF[tries];
  }

  /**
   * Creates a new table
   * 
   * @param desc table descriptor for table
   * 
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    HTableDescriptor.isLegalTableName(desc.getName());
    createTableAsync(desc);
    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // Wait for new table to come on-line
        connection.locateRegion(desc.getName(), HConstants.EMPTY_START_ROW);
        break;
        
      } catch (TableNotFoundException e) {
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
  public void createTableAsync(HTableDescriptor desc)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(desc.getName());
    try {
      this.master.createTable(desc);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }
  
  /**
   * Deletes a table
   * 
   * @param tableName name of table to delete
   * @throws IOException
   */
  public void deleteTable(final Text tableName) throws IOException {
    deleteTable(tableName.getBytes());
  }

  /**
   * Deletes a table
   * 
   * @param tableName name of table to delete
   * @throws IOException
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table
   * 
   * @param tableName name of table to delete
   * @throws IOException
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

    // Wait until first region is deleted
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());
    HRegionInfo info = new HRegionInfo();
    for (int tries = 0; tries < numRetries; tries++) {
      long scannerId = -1L;
      try {
        scannerId =
          server.openScanner(firstMetaServer.getRegionInfo().getRegionName(),
            HConstants.COL_REGIONINFO_ARRAY, tableName,
            HConstants.LATEST_TIMESTAMP, null);
        RowResult values = server.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        boolean found = false;
        for (Map.Entry<byte [], Cell> e: values.entrySet()) {
          if (Bytes.equals(e.getKey(), HConstants.COL_REGIONINFO)) {
            info = (HRegionInfo) Writables.getWritable(
              e.getValue().getValue(), info);
            
            if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
              found = true;
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
    LOG.info("Deleted " + Bytes.toString(tableName));
  }

  /**
   * Brings a table on-line (enables it)
   * 
   * @param tableName name of the table
   * @throws IOException
   */
  public void enableTable(final Text tableName) throws IOException {
    enableTable(tableName.getBytes());
  }

  /**
   * Brings a table on-line (enables it)
   * 
   * @param tableName name of the table
   * @throws IOException
   */
  public void enableTable(final String tableName) throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it)
   * 
   * @param tableName name of the table
   * @throws IOException
   */
  public void enableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    try {
      this.master.enableTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until all regions are enabled
    
    for (int tries = 0;
        (tries < numRetries) && (!isTableEnabled(tableName));
        tries++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for all regions to be enabled from " +
          Bytes.toString(tableName));
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for all regions to be enabled from " +
          Bytes.toString(tableName));
      }
    }
    if (!isTableEnabled(tableName))
      throw new IOException("unable to enable table " +
        Bytes.toString(tableName));
    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * 
   * @param tableName name of table
   * @throws IOException
   */
  public void disableTable(final Text tableName) throws IOException {
    disableTable(tableName.getBytes());
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * 
   * @param tableName name of table
   * @throws IOException
   */
  public void disableTable(final String tableName) throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * 
   * @param tableName name of table
   * @throws IOException
   */
  public void disableTable(final byte [] tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    try {
      this.master.disableTable(tableName);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until all regions are disabled
    for (int tries = 0;
        (tries < numRetries) && (isTableEnabled(tableName));
        tries++) {
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
    if (isTableEnabled(tableName))
      throw new IOException("unable to disable table " +
        Bytes.toString(tableName));
    LOG.info("Disabled " + Bytes.toString(tableName));
  }
  
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public boolean isTableEnabled(Text tableName) throws IOException {
    return isTableEnabled(tableName.getBytes());
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }
  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return connection.isTableEnabled(tableName);
  }
  
  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
   */
  public void addColumn(final Text tableName, HColumnDescriptor column)
  throws IOException {
    addColumn(tableName.getBytes(), column);
  }

  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
   */
  public void addColumn(final String tableName, HColumnDescriptor column)
  throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
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
   * Delete a column from a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException
   */
  public void deleteColumn(final Text tableName, final Text columnName)
  throws IOException {
    deleteColumn(tableName.getBytes(), columnName.getBytes());
  }

  /**
   * Delete a column from a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException
   */
  public void deleteColumn(final String tableName, final String columnName)
  throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException
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
   * Modify an existing column family on a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException
   */
  public void modifyColumn(final Text tableName, final Text columnName, 
      HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(tableName.getBytes(), columnName.getBytes(), descriptor);
  }

  /**
   * Modify an existing column family on a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException
   */
  public void modifyColumn(final String tableName, final String columnName, 
      HColumnDescriptor descriptor)
  throws IOException {
    modifyColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName),
      descriptor);
  }

  /**
   * Modify an existing column family on a table
   * 
   * @param tableName name of table
   * @param columnName name of column to be modified
   * @param descriptor new column descriptor to use
   * @throws IOException
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
   * Modify a table's HTableDescriptor
   * 
   * @param tableName name of table
   * @param desc the updated descriptor
   * @throws IOException
   */
  public void modifyTableMeta(final byte [] tableName, HTableDescriptor desc)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    HTableDescriptor.isLegalTableName(tableName);
    try {
      this.master.modifyTableMeta(tableName, desc);
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /** 
   * Shuts down the HBase instance 
   * @throws IOException
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

  private HRegionLocation getFirstMetaServerForTable(final byte [] tableName)
  throws IOException {
    return connection.locateRegion(HConstants.META_TABLE_NAME,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES));
  }

  /**
   * Check to see if HBase is running. Throw an exception if not.
   *
   * @param conf
   * @throws MasterNotRunningException
   */
  public static void checkHBaseAvailable(HBaseConfiguration conf)
  throws MasterNotRunningException {
    HBaseConfiguration copyOfConf = new HBaseConfiguration(conf);
    copyOfConf.setInt("hbase.client.retries.number", 1);
    new HBaseAdmin(copyOfConf);
  }
}