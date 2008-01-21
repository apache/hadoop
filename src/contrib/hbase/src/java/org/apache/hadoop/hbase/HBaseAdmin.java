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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Provides administrative functions for HBase
 */
public class HBaseAdmin implements HConstants {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  protected final HConnection connection;
  protected final long pause;
  protected final int numRetries;
  protected volatile HMasterInterface master;
  
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
  public boolean tableExists(final Text tableName) throws MasterNotRunningException {
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

  /**
   * Creates a new table
   * 
   * @param desc table descriptor for table
   * 
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws NoServerForRegionException if root region is not being served
   * @throws TableExistsException if table already exists (If concurrent
   * threads, the table may have been created between test-for-existence
   * and attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc)
  throws IOException {
    createTableAsync(desc);

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // Wait for new table to come on-line
        connection.locateRegion(desc.getName(), EMPTY_START_ROW);
        break;
        
      } catch (TableNotFoundException e) {
        if (tries == numRetries - 1) {
          // Ran out of tries
          throw e;
        }
      }
      try {
        Thread.sleep(pause);
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
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws NoServerForRegionException if root region is not being served
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
    checkReservedTableName(desc.getName());
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
  public void deleteTable(Text tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
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
            COL_REGIONINFO_ARRAY, tableName, System.currentTimeMillis(), null);
        HbaseMapWritable values = server.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        boolean found = false;
        for (Map.Entry<Writable, Writable> e: values.entrySet()) {
          HStoreKey key = (HStoreKey) e.getKey();
          if (key.getColumn().equals(COL_REGIONINFO)) {
            info = (HRegionInfo) Writables.getWritable(
                  ((ImmutableBytesWritable) e.getValue()).get(), info);
            
            if (info.getTableDesc().getName().equals(tableName)) {
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
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    LOG.info("table " + tableName + " deleted");
  }

  /**
   * Brings a table on-line (enables it)
   * 
   * @param tableName name of the table
   * @throws IOException
   */
  public void enableTable(Text tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    
    try {
      this.master.enableTable(tableName);
      
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until first region is enabled
    
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());

    HRegionInfo info = new HRegionInfo();
    for (int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId =
          server.openScanner(firstMetaServer.getRegionInfo().getRegionName(),
            COL_REGIONINFO_ARRAY, tableName, System.currentTimeMillis(), null);
        boolean isenabled = false;
        
        while (true) {
          HbaseMapWritable values = server.next(scannerId);
          if (values == null || values.size() == 0) {
            if (valuesfound == 0) {
              throw new NoSuchElementException(
                  "table " + tableName + " not found");
            }
            break;
          }
          valuesfound += 1;
          for (Map.Entry<Writable, Writable> e: values.entrySet()) {
            HStoreKey key = (HStoreKey) e.getKey();
            if (key.getColumn().equals(COL_REGIONINFO)) {
              info = (HRegionInfo) Writables.getWritable(
                    ((ImmutableBytesWritable) e.getValue()).get(), info);
            
              isenabled = !info.isOffline();
              break;
            }
          }
          if (isenabled) {
            break;
          }
        }
        if (isenabled) {
          break;
        }
        
      } catch (IOException e) {
        if (tries == numRetries - 1) {                  // no more retries
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        
      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch (Exception e) {
            LOG.warn(e);
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for first region to be enabled from " +
            tableName);
      }
      try {
        Thread.sleep(pause);
        
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be enabled from " +
            tableName);
      }
    }
    LOG.info("Enabled table " + tableName);
  }

  /**
   * Disables a table (takes it off-line) If it is being served, the master
   * will tell the servers to stop serving it.
   * 
   * @param tableName name of table
   * @throws IOException
   */
  public void disableTable(Text tableName) throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
    HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);

    try {
      this.master.disableTable(tableName);
      
    } catch (RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until first region is disabled
    
    HRegionInterface server =
      connection.getHRegionConnection(firstMetaServer.getServerAddress());

    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId =
          server.openScanner(firstMetaServer.getRegionInfo().getRegionName(),
            COL_REGIONINFO_ARRAY, tableName, System.currentTimeMillis(), null);
        
        boolean disabled = false;
        while (true) {
          HbaseMapWritable values = server.next(scannerId);
          if (values == null || values.size() == 0) {
            if (valuesfound == 0) {
              throw new NoSuchElementException("table " + tableName + " not found");
            }
            break;
          }
          valuesfound += 1;
          for (Map.Entry<Writable, Writable> e: values.entrySet()) {
            HStoreKey key = (HStoreKey) e.getKey();
            if (key.getColumn().equals(COL_REGIONINFO)) {
              info = (HRegionInfo) Writables.getWritable(
                    ((ImmutableBytesWritable) e.getValue()).get(), info);
            
              disabled = info.isOffline();
              break;
            }
          }
          if (disabled) {
            break;
          }
        }
        if (disabled) {
          break;
        }
        
      } catch (IOException e) {
        if (tries == numRetries - 1) {                  // no more retries
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        
      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch (Exception e) {
            LOG.warn(e);
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for first region to be disabled from " +
            tableName);
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be disabled from " +
            tableName);
      }
    }
    LOG.info("Disabled table " + tableName);
  }
  
  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
   */
  public void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
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
  public void deleteColumn(Text tableName, Text columnName)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
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
  public void modifyColumn(Text tableName, Text columnName, 
    HColumnDescriptor descriptor)
  throws IOException {
    if (this.master == null) {
      throw new MasterNotRunningException("master has been shut down");
    }
    
    checkReservedTableName(tableName);
    try {
      this.master.modifyColumn(tableName, columnName, descriptor);
      
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

  /*
   * Verifies that the specified table name is not a reserved name
   * @param tableName - the table name to be checked
   * @throws IllegalArgumentException - if the table name is reserved
   */
  protected void checkReservedTableName(Text tableName) {
    if (tableName == null || tableName.getLength() <= 0) {
      throw new IllegalArgumentException("Null or empty table name");
    }
    if(tableName.charAt(0) == '-' ||
        tableName.charAt(0) == '.' ||
        tableName.find(",") != -1) {
      throw new IllegalArgumentException(tableName + " is a reserved table name");
    }
  }
  
  private HRegionLocation getFirstMetaServerForTable(Text tableName)
  throws IOException {
    Text tableKey = new Text(tableName.toString() + ",,99999999999999");
    return connection.locateRegion(META_TABLE_NAME, tableKey);
  }
}