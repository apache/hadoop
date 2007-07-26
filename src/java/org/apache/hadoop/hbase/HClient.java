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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.KeyedData;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;

/**
 * HClient manages a connection to a single HRegionServer.
 */
public class HClient implements HConstants {
  final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  static final Text[] META_COLUMNS = {
    COLUMN_FAMILY
  };
  
  private static final Text[] REGIONINFO = {
    COL_REGIONINFO
  };
  
  static final Text EMPTY_START_ROW = new Text();
  
  long pause;
  int numRetries;
  HMasterInterface master;
  private final Configuration conf;
  private volatile long currentLockId;
  private Class<? extends HRegionInterface> serverInterfaceClass;
  
  protected class BatchHandler {
    private HashMap<RegionLocation, BatchUpdate> regionToBatch;
    private HashMap<Long, BatchUpdate> lockToBatch;
    
    /** constructor */
    public BatchHandler() {
      this.regionToBatch = new HashMap<RegionLocation, BatchUpdate>();
      this.lockToBatch = new HashMap<Long, BatchUpdate>();
    }
    
    /** 
     * Start a batch row insertion/update.
     * 
     * Manages multiple batch updates that are targeted for multiple servers,
     * should the rows span several region servers.
     * 
     * No changes are committed until the client commits the batch operation via
     * HClient.batchCommit().
     * 
     * The entire batch update can be abandoned by calling HClient.batchAbort();
     *
     * Callers to this method are given a handle that corresponds to the row being
     * changed. The handle must be supplied on subsequent put or delete calls so
     * that the row can be identified.
     * 
     * @param row Name of row to start update against.
     * @return Row lockid.
     */
    public synchronized long startUpdate(Text row) {
      RegionLocation info = getRegionLocation(row);
      BatchUpdate batch = regionToBatch.get(info);
      if(batch == null) {
        batch = new BatchUpdate();
        regionToBatch.put(info, batch);
      }
      long lockid = batch.startUpdate(row);
      lockToBatch.put(lockid, batch);
      return lockid;
    }
    
    /**
     * Change the value for the specified column
     * 
     * @param lockid lock id returned from startUpdate
     * @param column column whose value is being set
     * @param value new value for column
     */
    public synchronized void put(long lockid, Text column, byte[] value) {
      BatchUpdate batch = lockToBatch.get(lockid);
      if (batch == null) {
        throw new IllegalArgumentException("invalid lock id " + lockid);
      }
      batch.put(lockid, column, value);
    }
    
    /** 
     * Delete the value for a column
     *
     * @param lockid              - lock id returned from startUpdate
     * @param column              - name of column whose value is to be deleted
     */
    public synchronized void delete(long lockid, Text column) {
      BatchUpdate batch = lockToBatch.get(lockid);
      if (batch == null) {
        throw new IllegalArgumentException("invalid lock id " + lockid);
      }
      batch.delete(lockid, column);
    }
    
    /** 
     * Finalize a batch mutation
     *
     * @param timestamp time to associate with all the changes
     * @throws IOException
     */
    public synchronized void commit(long timestamp) throws IOException {
      try {
        for(Map.Entry<RegionLocation, BatchUpdate> e: regionToBatch.entrySet()) {
          RegionLocation r = e.getKey();
          HRegionInterface server = getHRegionConnection(r.serverAddress);
          server.batchUpdate(r.regionInfo.getRegionName(), timestamp,
              e.getValue());
        }
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }
    }
  }

  private BatchHandler batch;
  
  /*
   * Data structure that holds current location for a region and its info.
   */
  @SuppressWarnings("unchecked")
  protected static class RegionLocation implements Comparable {
    HRegionInfo regionInfo;
    HServerAddress serverAddress;

    RegionLocation(HRegionInfo regionInfo, HServerAddress serverAddress) {
      this.regionInfo = regionInfo;
      this.serverAddress = serverAddress;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return "address: " + this.serverAddress.toString() + ", regioninfo: " +
        this.regionInfo;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
      return this.compareTo(o) == 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      int result = this.regionInfo.hashCode();
      result ^= this.serverAddress.hashCode();
      return result;
    }
    
    /** @return HRegionInfo */
    public HRegionInfo getRegionInfo(){
      return regionInfo;
    }

    /** @return HServerAddress */
    public HServerAddress getServerAddress(){
      return serverAddress;
    }

    //
    // Comparable
    //
    
    /**
     * {@inheritDoc}
     */
    public int compareTo(Object o) {
      RegionLocation other = (RegionLocation) o;
      int result = this.regionInfo.compareTo(other.regionInfo);
      if(result == 0) {
        result = this.serverAddress.compareTo(other.serverAddress);
      }
      return result;
    }
  }

  /** encapsulates finding the servers for a table */
  protected class TableServers {
    // Map tableName -> (Map startRow -> (HRegionInfo, HServerAddress)
    private TreeMap<Text, SortedMap<Text, RegionLocation>> tablesToServers;

    /** constructor */
    public TableServers() {
      this.tablesToServers =
        new TreeMap<Text, SortedMap<Text, RegionLocation>>();
    }
    
    /**
     * Gets the servers of the given table out of cache, or calls
     * findServersForTable if there is nothing in the cache.
     * 
     * @param tableName - the table to be located
     * @return map of startRow -> RegionLocation
     * @throws IOException - if the table can not be located after retrying
     */
    public synchronized SortedMap<Text, RegionLocation>
    getTableServers(Text tableName) throws IOException {
      if(tableName == null || tableName.getLength() == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
      SortedMap<Text, RegionLocation> serverResult  =
        tablesToServers.get(tableName);
      
      if (serverResult == null ) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No servers for " + tableName + ". Doing a find...");
        }
        // We don't know where the table is.
        // Load the information from meta.
        serverResult = findServersForTable(tableName);
      }
      return serverResult;
    }

    /*
     * Clears the cache of all known information about the specified table and
     * locates a table by searching the META or ROOT region (as appropriate) or
     * by querying the master for the location of the root region if that is the
     * table requested.
     * 
     * @param tableName - name of table to find servers for
     * @return - map of first row to table info for all regions in the table
     * @throws IOException
     */
    private SortedMap<Text, RegionLocation> findServersForTable(Text tableName)
        throws IOException {
      
      // Wipe out everything we know about this table

      if (this.tablesToServers.containsKey(tableName)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wiping out all we know of " + tableName);
        }
        this.tablesToServers.remove(tableName);
      }
      
      SortedMap<Text, RegionLocation> servers = null;
      
      if (tableName.equals(ROOT_TABLE_NAME)) {
        servers = locateRootRegion();
        
      } else if (tableName.equals(META_TABLE_NAME)) {
        if (tablesToServers.get(ROOT_TABLE_NAME) == null) {
          findServersForTable(ROOT_TABLE_NAME);
        }
        for (int tries = 0; tries < numRetries; tries++) {
          try {
            servers = loadMetaFromRoot();
            break;
          
          } catch (IOException e) {
            if (tries < numRetries - 1) {
              findServersForTable(ROOT_TABLE_NAME);
              continue;
            }
            throw e;
          }
        }
      } else {
        for (int tries = 0; tries < numRetries; tries++) {
          boolean success = true;                         // assume this works

          SortedMap<Text, RegionLocation> metaServers =
            this.tablesToServers.get(META_TABLE_NAME);
          if (metaServers == null) {
            metaServers = findServersForTable(META_TABLE_NAME);
          }
          Text firstMetaRegion = metaServers.headMap(tableName).lastKey();
          metaServers = metaServers.tailMap(firstMetaRegion);

          servers = new TreeMap<Text,RegionLocation>();
          for (RegionLocation t: metaServers.values()) {
            try {
              servers.putAll(scanOneMetaRegion(t, tableName));

            } catch (IOException e) {
              e.printStackTrace();
              if(tries < numRetries - 1) {
                findServersForTable(META_TABLE_NAME);
                success = false;
                break;
              }
              throw e;
            }
          }
          if (success) {
            break;
          }
        }
      }
      this.tablesToServers.put(tableName, servers);
      if (LOG.isDebugEnabled()) {
        if(servers != null) {
          for (Map.Entry<Text, RegionLocation> e: servers.entrySet()) {
            LOG.debug("Server " + e.getKey() + " is serving: " + e.getValue() +
                " for table " + tableName);
          }
        }
      }
      return servers;
    }

    /*
     * Load the meta table from the root table.
     * 
     * @return map of first row to TableInfo for all meta regions
     * @throws IOException
     */
    private TreeMap<Text, RegionLocation> loadMetaFromRoot() throws IOException {
      SortedMap<Text, RegionLocation> rootRegion =
        this.tablesToServers.get(ROOT_TABLE_NAME);
      return scanOneMetaRegion(rootRegion.get(rootRegion.firstKey()), META_TABLE_NAME);
    }
    
    /*
     * Repeatedly try to find the root region by asking the master for where it is
     * @return TreeMap<Text, TableInfo> for root regin if found
     * @throws NoServerForRegionException - if the root region can not be located
     * after retrying
     * @throws IOException 
     */
    private TreeMap<Text, RegionLocation> locateRootRegion() throws IOException {
      checkMaster();
      
      HServerAddress rootRegionLocation = null;
      for(int tries = 0; tries < numRetries; tries++) {
        int localTimeouts = 0;
        while(rootRegionLocation == null && localTimeouts < numRetries) {
          rootRegionLocation = master.findRootRegion();
          if(rootRegionLocation == null) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Sleeping. Waiting for root region.");
              }
              Thread.sleep(pause);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wake. Retry finding root region.");
              }
            } catch(InterruptedException iex) {
              // continue
            }
            localTimeouts++;
          }
        }
        
        if(rootRegionLocation == null) {
          throw new NoServerForRegionException(
              "Timed out trying to locate root region");
        }
        
        HRegionInterface rootRegion = getHRegionConnection(rootRegionLocation);

        try {
          rootRegion.getRegionInfo(HGlobals.rootRegionInfo.regionName);
          break;
        } catch(IOException e) {
          if(tries == numRetries - 1) {
            // Don't bother sleeping. We've run out of retries.
            if(e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
          
          // Sleep and retry finding root region.
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Root region location changed. Sleeping.");
            }
            Thread.sleep(pause);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Wake. Retry finding root region.");
            }
          } catch(InterruptedException iex) {
            // continue
          }
        }
        rootRegionLocation = null;
      }
      
      if (rootRegionLocation == null) {
        throw new NoServerForRegionException(
          "unable to locate root region server");
      }
      
      TreeMap<Text, RegionLocation> rootServer = new TreeMap<Text, RegionLocation>();
      rootServer.put(EMPTY_START_ROW,
          new RegionLocation(HGlobals.rootRegionInfo, rootRegionLocation));
      
      return rootServer;
    }

    /*
     * Scans a single meta region
     * @param t the meta region we're going to scan
     * @param tableName the name of the table we're looking for
     * @return returns a map of startingRow to TableInfo
     * @throws RegionNotFoundException - if table does not exist
     * @throws IllegalStateException - if table is offline
     * @throws NoServerForRegionException - if table can not be found after retrying
     * @throws IOException 
     */
    private TreeMap<Text, RegionLocation> scanOneMetaRegion(final RegionLocation t,
      final Text tableName) throws IOException { 
      HRegionInterface server = getHRegionConnection(t.serverAddress);
      TreeMap<Text, RegionLocation> servers = new TreeMap<Text, RegionLocation>();
      for(int tries = 0; servers.size() == 0 && tries < numRetries; tries++) {

        long scannerId = -1L;
        try {
          scannerId =
            server.openScanner(t.regionInfo.regionName, META_COLUMNS, tableName,
                System.currentTimeMillis(), null);

          DataInputBuffer inbuf = new DataInputBuffer();
          while(true) {
            HRegionInfo regionInfo = null;
            String serverAddress = null;
            KeyedData[] values = server.next(scannerId);
            if(values.length == 0) {
              if(servers.size() == 0) {
                // If we didn't find any servers then the table does not exist
                throw new TableNotFoundException("table '" + tableName +
                    "' does not exist in " + t);
              }

              // We found at least one server for the table and now we're done.
              if (LOG.isDebugEnabled()) {
                LOG.debug("Found " + servers.size() + " server(s) for " +
                    "location: " + t + " for tablename " + tableName);
              }
              break;
            }

            byte[] bytes = null;
            TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
            for(int i = 0; i < values.length; i++) {
              results.put(values[i].getKey().getColumn(), values[i].getData());
            }
            regionInfo = new HRegionInfo();
            bytes = results.get(COL_REGIONINFO);
            inbuf.reset(bytes, bytes.length);
            regionInfo.readFields(inbuf);

            if(!regionInfo.tableDesc.getName().equals(tableName)) {
              // We're done
              if (LOG.isDebugEnabled()) {
                LOG.debug("Found " + servers.size() + " servers for table " +
                  tableName);
              }
              break;
            }

            if(regionInfo.offLine) {
              throw new IllegalStateException("table offline: " + tableName);
            }

            bytes = results.get(COL_SERVER);
            if(bytes == null || bytes.length == 0) {
              // We need to rescan because the table we want is unassigned.
              if(LOG.isDebugEnabled()) {
                LOG.debug("no server address for " + regionInfo.toString());
              }
              servers.clear();
              break;
            }
            serverAddress = new String(bytes, UTF8_ENCODING);
            servers.put(regionInfo.startKey, 
                new RegionLocation(regionInfo, new HServerAddress(serverAddress)));
          }
        } catch (IOException e) {
          if(tries == numRetries - 1) {                 // no retries left
            if(e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
          }
          
        } finally {
          if(scannerId != -1L) {
            try {
              server.close(scannerId);
            } catch(Exception ex) {
              LOG.warn(ex);
            }
          }
        }
        
        if(servers.size() == 0 && tries == numRetries - 1) {
          throw new NoServerForRegionException("failed to find server for "
                + tableName + " after " + numRetries + " retries");
        }

        if (servers.size() <= 0) {
          // The table is not yet being served. Sleep and retry.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sleeping. Table " + tableName +
              " not currently being served.");
          }
          try {
            Thread.sleep(pause);
          } catch (InterruptedException ie) {
            // continue
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake. Retry finding table " + tableName);
          }
        }
      }
      return servers;
    }

    /**
     * Reloads servers for the specified table.
     * @param tableName name of table whose servers are to be reloaded
     * @return map of start key -> RegionLocation
     * @throws IOException
     */
    public synchronized SortedMap<Text, RegionLocation>
    reloadTableServers(final Text tableName)
    throws IOException {
      // Reload information for the whole table
      SortedMap<Text, RegionLocation> servers = findServersForTable(tableName);
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Result of findTable: " + servers.toString());
      }
      
      if (tablesToServers.get(tableName) == null) {
        throw new TableNotFoundException(tableName.toString());
      }
      
      return servers;
    }
    
  }
  
  protected TableServers tableServers;
  
  // For the "current" table: Map startRow -> RegionLocation
  SortedMap<Text, RegionLocation> currentTableServers;
  
  // Known region HServerAddress.toString() -> HRegionInterface 
  private TreeMap<String, HRegionInterface> servers;
  
  // For row mutation operations

  Text currentRegion;
  HRegionInterface currentServer;
  Random rand;
  long clientid;


  /** 
   * Creates a new HClient
   * @param conf - Configuration object
   */
  public HClient(Configuration conf) {
    this.conf = conf;
    this.batch = null;
    this.currentLockId = -1;

    this.pause = conf.getLong("hbase.client.pause", 30 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    
    this.master = null;
    this.tableServers = new TableServers();
    this.currentTableServers = null;
    this.servers = new TreeMap<String, HRegionInterface>();
    
    // For row mutation operations

    this.currentRegion = null;
    this.currentServer = null;
    this.rand = new Random();
  }

  /* Find the address of the master and connect to it */
  protected void checkMaster() throws MasterNotRunningException {
    if (this.master != null) {
      return;
    }
    
    for(int tries = 0; this.master == null && tries < numRetries; tries++) {
      HServerAddress masterLocation =
        new HServerAddress(this.conf.get(MASTER_ADDRESS,
          DEFAULT_MASTER_ADDRESS));
      
      try {
        HMasterInterface tryMaster =
          (HMasterInterface)RPC.getProxy(HMasterInterface.class, 
            HMasterInterface.versionID, masterLocation.getInetSocketAddress(),
            this.conf);
        if(tryMaster.isMasterRunning()) {
          this.master = tryMaster;
          break;
        }
      } catch(IOException e) {
        if(tries == numRetries - 1) {
          // This was our last chance - don't bother sleeping
          break;
        }
        LOG.info("Attempt " + tries + " of " + this.numRetries +
          " failed with <" + e + ">. Retrying after sleep of " + this.pause);
      }
      
      // We either cannot connect to master or it is not running. Sleep & retry
      try {
        Thread.sleep(this.pause);
      } catch(InterruptedException e) {
        // continue
      }
    }
    
    if(this.master == null) {
      throw new MasterNotRunningException();
    }
  }

  /**
   * @return - true if the master server is running
   */
  public boolean isMasterRunning() {
    if(this.master == null) {
      try {
        checkMaster();
        
      } catch(MasterNotRunningException e) {
        return false;
      }
    }
    return true;
  }

  /**
   * Reloads the cached server information for the current table
   * 
   * @param info RegionInfo for a region that is a part of the table
   * @throws IOException
   */
  protected synchronized void reloadCurrentTable(RegionLocation info)
  throws IOException {
    this.currentTableServers = tableServers.reloadTableServers(
        info.getRegionInfo().getTableDesc().getName());
  }
  
  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   */
  protected synchronized RegionLocation getRegionLocation(Text row) {
    if(this.currentTableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    
    // Only one server will have the row we are looking for
    Text serverKey = (this.currentTableServers.containsKey(row))? row:
      this.currentTableServers.headMap(row).lastKey();
    return this.currentTableServers.get(serverKey);
  }

  /** 
   * Establishes a connection to the region server at the specified address.
   * @param regionServer - the server to connect to
   * @throws IOException
   */
  protected synchronized HRegionInterface getHRegionConnection (
      HServerAddress regionServer) throws IOException {

    getRegionServerInterface();

    // See if we already have a connection
    HRegionInterface server = this.servers.get(regionServer.toString());

    if (server == null) { // Get a connection
      long versionId = 0;
      try {
        versionId =
          serverInterfaceClass.getDeclaredField("versionID").getLong(server);
        
      } catch (IllegalAccessException e) {
        // Should never happen unless visibility of versionID changes
        throw new UnsupportedOperationException(
            "Unable to open a connection to a " + serverInterfaceClass.getName()
            + " server.", e);
        
      } catch (NoSuchFieldException e) {
        // Should never happen unless versionID field name changes in HRegionInterface
        throw new UnsupportedOperationException(
            "Unable to open a connection to a " + serverInterfaceClass.getName()
            + " server.", e);
      }

      try {
        server = (HRegionInterface) RPC.waitForProxy(serverInterfaceClass,
            versionId, regionServer.getInetSocketAddress(), this.conf);
        
      } catch (RemoteException e) {
        throw RemoteExceptionHandler.decodeRemoteException(e);
      }

      this.servers.put(regionServer.toString(), server);
    }
    return server;
  }

  //
  // Administrative methods
  //

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
  public synchronized void createTable(HTableDescriptor desc)
  throws IOException {
    createTableAsync(desc);

    // Wait for new table to come on-line
    tableServers.getTableServers(desc.getName());
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
  public synchronized void createTableAsync(HTableDescriptor desc)
      throws IOException {
    checkReservedTableName(desc.getName());
    checkMaster();
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
  public synchronized void deleteTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    RegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);

    try {
      this.master.deleteTable(tableName);
    } catch(RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until first region is deleted
    HRegionInterface server =
      getHRegionConnection(firstMetaServer.serverAddress);
    DataInputBuffer inbuf = new DataInputBuffer();
    HRegionInfo info = new HRegionInfo();
    for (int tries = 0; tries < numRetries; tries++) {
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName, System.currentTimeMillis(), null);
        KeyedData[] values = server.next(scannerId);
        if(values == null || values.length == 0) {
          break;
        }
        boolean found = false;
        for(int j = 0; j < values.length; j++) {
          if (values[j].getKey().getColumn().equals(COL_REGIONINFO)) {
            inbuf.reset(values[j].getData(), values[j].getData().length);
            info.readFields(inbuf);
            if(info.tableDesc.getName().equals(tableName)) {
              found = true;
            }
          }
        }
        if(!found) {
          break;
        }

      } catch (IOException ex) {
        if(tries == numRetries - 1) {           // no more tries left
          if(ex instanceof RemoteException) {
            ex = RemoteExceptionHandler.decodeRemoteException((RemoteException) ex);
          }
          throw ex;
        }

      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);
          } catch(Exception ex) {
            LOG.warn(ex);
          }
        }
      }

      try {
        Thread.sleep(pause);
      } catch(InterruptedException e) {
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
  public synchronized void enableTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    RegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
    
    try {
      this.master.enableTable(tableName);
      
    } catch(RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until first region is enabled
    
    HRegionInterface server = getHRegionConnection(firstMetaServer.serverAddress);

    DataInputBuffer inbuf = new DataInputBuffer();
    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName, System.currentTimeMillis(), null);
        boolean isenabled = false;
        while(true) {
          KeyedData[] values = server.next(scannerId);
          if(values == null || values.length == 0) {
            if(valuesfound == 0) {
              throw new NoSuchElementException("table " + tableName + " not found");
            }
            break;
          }
          valuesfound += 1;
          for(int j = 0; j < values.length; j++) {
            if(values[j].getKey().getColumn().equals(COL_REGIONINFO)) {
              inbuf.reset(values[j].getData(), values[j].getData().length);
              info.readFields(inbuf);
              isenabled = !info.offLine;
              break;
            }
          }
          if(isenabled) {
            break;
          }
        }
        if(isenabled) {
          break;
        }
        
      } catch (IOException e) {
        if(tries == numRetries - 1) {                   // no more retries
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        
      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch(Exception e) {
            LOG.warn(e);
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for first region to be enabled from " + tableName);
      }
      try {
        Thread.sleep(pause);
        
      } catch(InterruptedException e) {
        // continue
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be enabled from " + tableName);
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
  public synchronized void disableTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    RegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);

    try {
      this.master.disableTable(tableName);
      
    } catch(RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }

    // Wait until first region is disabled
    
    HRegionInterface server = getHRegionConnection(firstMetaServer.serverAddress);

    DataInputBuffer inbuf = new DataInputBuffer();
    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName, System.currentTimeMillis(), null);
        boolean disabled = false;
        while(true) {
          KeyedData[] values = server.next(scannerId);
          if(values == null || values.length == 0) {
            if(valuesfound == 0) {
              throw new NoSuchElementException("table " + tableName + " not found");
            }
            break;
          }
          valuesfound += 1;
          for(int j = 0; j < values.length; j++) {
            if(values[j].getKey().getColumn().equals(COL_REGIONINFO)) {
              inbuf.reset(values[j].getData(), values[j].getData().length);
              info.readFields(inbuf);
              disabled = info.offLine;
              break;
            }
          }
          if(disabled) {
            break;
          }
        }
        if(disabled) {
          break;
        }
        
      } catch(IOException e) {
        if(tries == numRetries - 1) {                   // no more retries
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        
      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch(Exception e) {
            LOG.warn(e);
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Sleep. Waiting for first region to be disabled from " + tableName);
      }
      try {
        Thread.sleep(pause);
      } catch(InterruptedException e) {
        // continue
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be disabled from " + tableName);
      }
    }
    LOG.info("Disabled table " + tableName);
  }
  
  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(final Text tableName) throws IOException {
    HTableDescriptor [] tables = listTables();
    boolean result = false;
    for (int i = 0; i < tables.length; i++) {
      if (tables[i].getName().equals(tableName)) {
        result = true;
        break;
      }
    }
    return result;
  }

  /**
   * Add a column to an existing table
   * 
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException
   */
  public synchronized void addColumn(Text tableName, HColumnDescriptor column)
  throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
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
  public synchronized void deleteColumn(Text tableName, Text columnName)
  throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    try {
      this.master.deleteColumn(tableName, columnName);
      
    } catch(RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }
  
  /** 
   * Shuts down the HBase instance 
   * @throws IOException
   */
  public synchronized void shutdown() throws IOException {
    checkMaster();
    try {
      this.master.shutdown();
    } catch(RemoteException e) {
      throw RemoteExceptionHandler.decodeRemoteException(e);
    }
  }

  /*
   * Verifies that the specified table name is not a reserved name
   * @param tableName - the table name to be checked
   * @throws IllegalArgumentException - if the table name is reserved
   */
  protected void checkReservedTableName(Text tableName) {
    if(tableName.equals(ROOT_TABLE_NAME)
        || tableName.equals(META_TABLE_NAME)) {
      
      throw new IllegalArgumentException(tableName + " is a reserved table name");
    }
  }
  
  private RegionLocation getFirstMetaServerForTable(Text tableName)
  throws IOException {
    SortedMap<Text, RegionLocation> metaservers =
      tableServers.getTableServers(META_TABLE_NAME);
    
    return metaservers.get((metaservers.containsKey(tableName)) ?
        tableName : metaservers.headMap(tableName).lastKey());
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Client API
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Loads information so that a table can be manipulated.
   * 
   * @param tableName the table to be located
   * @throws IOException if the table can not be located after retrying
   */
  public synchronized void openTable(Text tableName) throws IOException {
    if(tableName == null || tableName.getLength() == 0) {
      throw new IllegalArgumentException("table name cannot be null or zero length");
    }
    if(this.currentLockId != -1 || batch != null) {
      throw new IllegalStateException("update in progress");
    }
    this.currentTableServers = tableServers.getTableServers(tableName);
  }
  
  /**
   * Gets the starting row key for every region in the currently open table
   * @return Array of region starting row keys
   */
  public synchronized Text[] getStartKeys() {
    if(this.currentTableServers == null) {
      throw new IllegalStateException("Must open table first");
    }

    Text[] keys = new Text[currentTableServers.size()];
    int i = 0;
    for(Text key: currentTableServers.keySet()){
      keys[i++] = key;
    }
    return keys;
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
  public synchronized HTableDescriptor[] listTables()
      throws IOException {
    TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    
    SortedMap<Text, RegionLocation> metaTables =
      tableServers.getTableServers(META_TABLE_NAME);

    for (RegionLocation t: metaTables.values()) {
      HRegionInterface server = getHRegionConnection(t.serverAddress);
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(t.regionInfo.regionName,
            META_COLUMNS, EMPTY_START_ROW, System.currentTimeMillis(), null);
        
        DataInputBuffer inbuf = new DataInputBuffer();
        while(true) {
          KeyedData[] values = server.next(scannerId);
          if(values.length == 0) {
            break;
          }
          for(int i = 0; i < values.length; i++) {
            if(values[i].getKey().getColumn().equals(COL_REGIONINFO)) {
              inbuf.reset(values[i].getData(), values[i].getData().length);
              HRegionInfo info = new HRegionInfo();
              info.readFields(inbuf);

              // Only examine the rows where the startKey is zero length   
              if(info.startKey.getLength() == 0) {
                uniqueTables.add(info.tableDesc);
              }
            }
          }
        }
      } catch (RemoteException ex) {
        throw RemoteExceptionHandler.decodeRemoteException(ex);
        
      } finally {
        if(scannerId != -1L) {
          server.close(scannerId);
        }
      }
    }
    return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public byte[] get(Text row, Text column) throws IOException {
    byte [] value = null;
    for(int tries = 0; tries < numRetries; tries++) {
      RegionLocation info = getRegionLocation(row);
      HRegionInterface server = getHRegionConnection(info.serverAddress);
      try {
        value = server.get(info.regionInfo.regionName, row, column);
        break;
        
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        reloadCurrentTable(info);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }
    return value;
  }
 
  /** 
   * Get the specified number of versions of the specified row and column
   * 
   * @param row         - row key
   * @param column      - column name
   * @param numVersions - number of versions to retrieve
   * @return            - array byte values
   * @throws IOException
   */
  public byte[][] get(Text row, Text column, int numVersions) throws IOException {
    byte [][] values = null;
    for(int tries = 0; tries < numRetries; tries++) {
      RegionLocation info = getRegionLocation(row);
      HRegionInterface server = getHRegionConnection(info.serverAddress);
      
      try {
        values = server.get(info.regionInfo.regionName, row, column, numVersions);
        break;
        
      } catch(IOException e) {
        if(tries == numRetries - 1) {
          // No more tries
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        reloadCurrentTable(info);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }

    if(values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for(int i = 0 ; i < values.length; i++) {
        bytes.add(values[i]);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
  }
  
  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param row         - row key
   * @param column      - column name
   * @param timestamp   - timestamp
   * @param numVersions - number of versions to retrieve
   * @return            - array of values that match the above criteria
   * @throws IOException
   */
  public byte[][] get(Text row, Text column, long timestamp, int numVersions)
  throws IOException {
    byte [][] values = null;
    for(int tries = 0; tries < numRetries; tries++) {
      RegionLocation info = getRegionLocation(row);
      HRegionInterface server = getHRegionConnection(info.serverAddress);
      try {
        values = server.get(info.regionInfo.regionName, row, column, timestamp, numVersions);
        break;
    
      } catch(IOException e) {
        if(tries == numRetries - 1) {
          // No more tries
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        reloadCurrentTable(info);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }

    if(values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for(int i = 0 ; i < values.length; i++) {
        bytes.add(values[i]);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
  }
    
  /** 
   * Get all the data for the specified row
   * 
   * @param row         - row key
   * @return            - map of colums to values
   * @throws IOException
   */
  public SortedMap<Text, byte[]> getRow(Text row) throws IOException {
    KeyedData[] value = null;
    for(int tries = 0; tries < numRetries; tries++) {
      RegionLocation info = getRegionLocation(row);
      HRegionInterface server = getHRegionConnection(info.serverAddress);
      
      try {
        value = server.getRow(info.regionInfo.regionName, row);
        break;
        
      } catch(IOException e) {
        if(tries == numRetries - 1) {
          // No more tries
          if(e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
          }
          throw e;
        }
        reloadCurrentTable(info);
      }
      try {
        Thread.sleep(this.pause);
        
      } catch (InterruptedException x) {
        // continue
      }
    }
    TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    if(value != null && value.length != 0) {
      for(int i = 0; i < value.length; i++) {
        results.put(value[i].getKey().getColumn(), value[i].getData());
      }
    }
    return results;
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns,
      Text startRow) throws IOException {
    return obtainScanner(columns, startRow, System.currentTimeMillis(), null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns,
      Text startRow, long timestamp) throws IOException {
    return obtainScanner(columns, startRow, timestamp, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns,
      Text startRow, RowFilterInterface filter) throws IOException { 
    return obtainScanner(columns, startRow, System.currentTimeMillis(), filter);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns array of columns to return
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns,
      Text startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    if(this.currentTableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    return new ClientScanner(columns, startRow, timestamp, filter);
  }

  /** 
   * Start a batch of row insertions/updates.
   * 
   * No changes are committed until the call to commitBatchUpdate returns.
   * A call to abortBatchUpdate will abandon the entire batch.
   * 
   * Note that in batch mode, calls to commit or abort are ignored.
   */
  public synchronized void startBatchUpdate() {
    if(this.currentTableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    
    if(batch == null) {
      batch = new BatchHandler();
    }
  }
  
  /** 
   * Abort a batch mutation
   */
  public synchronized void abortBatch() {
    batch = null;
  }
  
  /** 
   * Finalize a batch mutation
   *
   * @throws IOException
   */
  public synchronized void commitBatch() throws IOException {
    commitBatch(System.currentTimeMillis());
  }

  /** 
   * Finalize a batch mutation
   *
   * @param timestamp time to associate with all the changes
   * @throws IOException
   */
  public synchronized void commitBatch(long timestamp) throws IOException {
    if(batch == null) {
      throw new IllegalStateException("no batch update in progress");
    }
    
    try {
      batch.commit(timestamp);
      
    } finally {
      batch = null;
    }
  }
  
  /** 
   * Start an atomic row insertion/update.  No changes are committed until the 
   * call to commit() returns. A call to abort() will abandon any updates in progress.
   *
   * Callers to this method are given a lease for each unique lockid; before the
   * lease expires, either abort() or commit() must be called. If it is not 
   * called, the system will automatically call abort() on the client's behalf.
   *
   * The client can gain extra time with a call to renewLease().
   * Start an atomic row insertion or update
   * 
   * @param row Name of row to start update against.
   * @return Row lockid.
   * @throws IOException
   */
  public synchronized long startUpdate(final Text row) throws IOException {
    if(this.currentLockId != -1) {
      throw new IllegalStateException("update in progress");
    }
    if(batch != null) {
      return batch.startUpdate(row);
    }
    for(int tries = 0; tries < numRetries; tries++) {
      IOException e = null;
      RegionLocation info = getRegionLocation(row);
      try {
        currentServer = getHRegionConnection(info.serverAddress);
        currentRegion = info.regionInfo.regionName;
        clientid = rand.nextLong();
        this.currentLockId = currentServer.startUpdate(currentRegion, clientid, row);
        break;
        
      } catch (IOException ex) {
        e = ex;
      }
      if(tries < numRetries - 1) {
        try {
          Thread.sleep(this.pause);
          
        } catch (InterruptedException ex) {
        }
        try {
          reloadCurrentTable(info);
          
        } catch (IOException ex) {
          e = ex;
        }
      } else {
        if(e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        throw e;
      }
    }
    return this.currentLockId;
  }
  
  /** 
   * Change a value for the specified column.
   * Runs {@link #abort(long)} if exception thrown.
   *
   * @param lockid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column
   * @throws IOException
   */
  public void put(long lockid, Text column, byte val[]) throws IOException {
    if(val == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    if(batch != null) {
      batch.put(lockid, column, val);
      return;
    }
    
    if(lockid != this.currentLockId) {
      throw new IllegalArgumentException("invalid lockid");
    }
    try {
      this.currentServer.put(this.currentRegion, this.clientid, lockid, column,
        val);
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
      } catch(IOException e2) {
        LOG.warn(e2);
      }
      this.currentServer = null;
      this.currentRegion = null;
      if(e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
      }
      throw e;
    }
  }
  
  /** 
   * Delete the value for a column
   *
   * @param lockid              - lock id returned from startUpdate
   * @param column              - name of column whose value is to be deleted
   * @throws IOException
   */
  public void delete(long lockid, Text column) throws IOException {
    if(batch != null) {
      batch.delete(lockid, column);
      return;
    }
    
    if(lockid != this.currentLockId) {
      throw new IllegalArgumentException("invalid lockid");
    }
    try {
      this.currentServer.delete(this.currentRegion, this.clientid, lockid,
        column);
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
      } catch(IOException e2) {
        LOG.warn(e2);
      }
      this.currentServer = null;
      this.currentRegion = null;
      if(e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
      }
      throw e;
    }
  }
  
  /** 
   * Abort a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   * @throws IOException
   */
  public void abort(long lockid) throws IOException {
    if(batch != null) {
      return;
    }
    
    if(lockid != this.currentLockId) {
      throw new IllegalArgumentException("invalid lockid");
    }
    try {
      this.currentServer.abort(this.currentRegion, this.clientid, lockid);
    } catch(IOException e) {
      this.currentServer = null;
      this.currentRegion = null;
      if(e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
      }
      throw e;
    } finally {
      this.currentLockId = -1;
    }
  }
  
  /** 
   * Finalize a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   * @throws IOException
   */
  public void commit(long lockid) throws IOException {
    commit(lockid, System.currentTimeMillis());
  }

  /** 
   * Finalize a row mutation
   *
   * @param lockid              - lock id returned from startUpdate
   * @param timestamp           - time to associate with the change
   * @throws IOException
   */
  public void commit(long lockid, long timestamp) throws IOException {
    if(batch != null) {
      return;
    }
    
    if(lockid != this.currentLockId) {
      throw new IllegalArgumentException("invalid lockid");
    }
    try {
      this.currentServer.commit(this.currentRegion, this.clientid, lockid,
          timestamp);
      
    } catch (IOException e) {
      this.currentServer = null;
      this.currentRegion = null;
      if(e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
      }
      throw e;
    } finally {
      this.currentLockId = -1;
    }
  }
  
  /**
   * Renew lease on update
   * 
   * @param lockid              - lock id returned from startUpdate
   * @throws IOException
   */
  public void renewLease(long lockid) throws IOException {
    if(batch != null) {
      return;
    }
    
    if(lockid != this.currentLockId) {
      throw new IllegalArgumentException("invalid lockid");
    }
    try {
      this.currentServer.renewLease(lockid, this.clientid);
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
      } catch(IOException e2) {
        LOG.warn(e2);
      }
      this.currentServer = null;
      this.currentRegion = null;
      if(e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
      }
      throw e;
    }
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  private class ClientScanner implements HScannerInterface {
    private final Text EMPTY_COLUMN = new Text();
    private Text[] columns;
    private Text startRow;
    private long scanTime;
    private boolean closed;
    private volatile RegionLocation[] regions;
    @SuppressWarnings("hiding")
    private int currentRegion;
    private HRegionInterface server;
    private long scannerId;
    private RowFilterInterface filter;
    
    private void loadRegions() {
      Text firstServer = null;
      if(this.startRow == null || this.startRow.getLength() == 0) {
        firstServer = currentTableServers.firstKey();

      } else if(currentTableServers.containsKey(startRow)) {
        firstServer = startRow;

      } else {
        firstServer = currentTableServers.headMap(startRow).lastKey();
      }
      Collection<RegionLocation> info =
        currentTableServers.tailMap(firstServer).values();
      
      this.regions = info.toArray(new RegionLocation[info.size()]);
    }
    
    ClientScanner(Text[] columns, Text startRow, long timestamp,
        RowFilterInterface filter) throws IOException {
      this.columns = columns;
      this.startRow = startRow;
      this.scanTime = timestamp;
      this.closed = false;
      this.filter = filter;
      if (filter != null) {
        filter.validate(columns);
      }
      loadRegions();
      this.currentRegion = -1;
      this.server = null;
      this.scannerId = -1L;
      nextScanner();
    }
    
    /*
     * Gets a scanner for the next region.
     * Returns false if there are no more scanners.
     */
    private boolean nextScanner() throws IOException {
      if(this.scannerId != -1L) {
        this.server.close(this.scannerId);
        this.scannerId = -1L;
      }
      this.currentRegion += 1;
      if(this.currentRegion == this.regions.length) {
        close();
        return false;
      }
      try {
        for(int tries = 0; tries < numRetries; tries++) {
          RegionLocation info = this.regions[currentRegion];
          this.server = getHRegionConnection(this.regions[currentRegion].serverAddress);
          
          try {
            if (this.filter == null) {
              this.scannerId = this.server.openScanner(info.regionInfo.regionName,
                      this.columns, currentRegion == 0 ? this.startRow
                          : EMPTY_START_ROW, scanTime, null);
            } else {
              this.scannerId =
                this.server.openScanner(info.regionInfo.regionName,
                    this.columns, currentRegion == 0 ? this.startRow
                        : EMPTY_START_ROW, scanTime, filter);
            }

            break;
        
          } catch(IOException e) {
            if(tries == numRetries - 1) {
              // No more tries
              if(e instanceof RemoteException) {
                e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              }
              throw e;
            }
            reloadCurrentTable(info);
            loadRegions();
          }
        }

      } catch(IOException e) {
        close();
        if(e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        throw e;
      }
      return true;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean next(HStoreKey key, TreeMap<Text, byte[]> results) throws IOException {
      if(this.closed) {
        return false;
      }
      KeyedData[] values = null;
      do {
        values = this.server.next(this.scannerId);
      } while(values != null && values.length == 0 && nextScanner());

      if(values != null && values.length != 0) {
        for(int i = 0; i < values.length; i++) {
          key.setRow(values[i].getKey().getRow());
          key.setVersion(values[i].getKey().getTimestamp());
          key.setColumn(EMPTY_COLUMN);
          results.put(values[i].getKey().getColumn(), values[i].getData());
        }
      }
      return values == null ? false : values.length != 0;
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
      if(this.scannerId != -1L) {
        this.server.close(this.scannerId);
        this.scannerId = -1L;
      }
      this.server = null;
      this.closed = true;
    }
  }
  
  private void printUsage() {
    printUsage(null);
  }
  
  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
        " [--master=host:port] <command> <args>");
    System.err.println("Options:");
    System.err.println(" master       Specify host and port of HBase " +
        "cluster master. If not present,");
    System.err.println("              address is read from configuration.");
    System.err.println("Commands:");
    System.err.println(" shutdown     Shutdown the HBase cluster.");
    System.err.println(" createTable  Create named table.");
    System.err.println(" deleteTable  Delete named table.");
    System.err.println(" listTables   List all tables.");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() + " shutdown");
    System.err.println(" % java " + this.getClass().getName() +
        " createTable webcrawl contents: anchors: 10");
  }
  
  private void printCreateTableUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
      " [options] createTable <name> <colfamily1> ... <max_versions>");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() +
      " createTable testtable column_x column_y column_z 3");
  }
  
  private void printDeleteTableUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
      " [options] deleteTable <name>");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() +
      " deleteTable testtable");
  }
  
  /**
   * Process command-line args.
   * @param args - command arguments
   * @return 0 if successful -1 otherwise
   */
  public int doCommandLine(final String args[]) {
    // TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).    
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage();
          errCode = 0;
          break;
        }
        
        final String masterArgKey = "--master=";
        if (cmd.startsWith(masterArgKey)) {
          this.conf.set(MASTER_ADDRESS, cmd.substring(masterArgKey.length()));
          continue;
        }
       
        if (cmd.equals("shutdown")) {
          shutdown();
          errCode = 0;
          break;
        }
        
        if (cmd.equals("listTables")) {
          HTableDescriptor [] tables = listTables();
          for (int ii = 0; ii < tables.length; ii++) {
            System.out.println(tables[ii].getName());
          }
          errCode = 0;
          break;
        }
        
        if (cmd.equals("createTable")) {
          if (i + 2 > args.length) {
            printCreateTableUsage("Error: Supply a table name," +
              " at least one column family, and maximum versions");
            errCode = 1;
            break;
          }
          HTableDescriptor desc = new HTableDescriptor(args[i + 1]);
          boolean addedFamily = false;
          for (int ii = i + 2; ii < (args.length - 1); ii++) {
            desc.addFamily(new HColumnDescriptor(args[ii]));
            addedFamily = true;
          }
          if (!addedFamily) {
            throw new IllegalArgumentException("Must supply at least one " +
              "column family");
          }
          createTable(desc);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("deleteTable")) {
          if (i + 1 > args.length) {
            printDeleteTableUsage("Error: Must supply a table name");
            errCode = 1;
            break;
          }
          deleteTable(new Text(args[i + 1]));
          errCode = 0;
          break;
        }
        
        printUsage();
        break;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
    
    return errCode;
  }    

  /**
   * Determine the region server interface to use from configuration properties.
   *
   */
  @SuppressWarnings("unchecked")
  private void getRegionServerInterface() {
    if (this.serverInterfaceClass != null) {
      return;
    }

    String serverClassName = this.conf.get(REGION_SERVER_CLASS,
                                           DEFAULT_REGION_SERVER_CLASS);

    try {
      this.serverInterfaceClass =
        (Class<? extends HRegionInterface>) Class.forName(serverClassName);
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
            "Unable to find region server interface " + serverClassName, e);
    }
  }

  /**
   * @return the configuration for this client
   */
  protected Configuration getConf(){
    return conf;
  }
  
  /**
   * Main program
   * @param args
   */
  public static void main(final String args[]) {
    Configuration c = new HBaseConfiguration();
    int errCode = (new HClient(c)).doCommandLine(args);
    System.exit(errCode);
  }

}
