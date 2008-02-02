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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HbaseRPC;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;

/**
 * A non-instantiable class that manages connections to multiple tables in
 * multiple HBase instances
 */
public class HConnectionManager implements HConstants {
  /*
   * Private. Not instantiable.
   */
  private HConnectionManager() {
    super();
  }
  
  // A Map of master HServerAddress -> connection information for that instance
  // Note that although the Map is synchronized, the objects it contains
  // are mutable and hence require synchronized access to them
  
  private static final Map<String, TableServers> HBASE_INSTANCES =
    Collections.synchronizedMap(new HashMap<String, TableServers>());

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   * @param conf
   * @return HConnection object for the instance specified by the configuration
   */
  public static HConnection getConnection(HBaseConfiguration conf) {
    TableServers connection;
    synchronized (HBASE_INSTANCES) {
      String instanceName = conf.get(HBASE_DIR, DEFAULT_HBASE_DIR);

      connection = HBASE_INSTANCES.get(instanceName);

      if (connection == null) {
        connection = new TableServers(conf);
        HBASE_INSTANCES.put(instanceName, connection);
      }
    }
    return connection;
  }
  
  /**
   * Delete connection information for the instance specified by the configuration
   * @param conf
   */
  public static void deleteConnection(HBaseConfiguration conf) {
    synchronized (HBASE_INSTANCES) {
      TableServers instance =
        HBASE_INSTANCES.remove(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR));
      if (instance != null) {
        instance.closeAll();
      }
    }    
  }
  
  /* Encapsulates finding the servers for an HBase instance */
  private static class TableServers implements HConnection, HConstants {
    private static final Log LOG = LogFactory.getLog(TableServers.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long pause;
    private final int numRetries;

    private final Integer masterLock = new Integer(0);
    private volatile boolean closed;
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;
    
    private final Integer rootRegionLock = new Integer(0);
    private final Integer metaRegionLock = new Integer(0);
    private final Integer userRegionLock = new Integer(0);
        
    private volatile HBaseConfiguration conf;

    // Set of closed tables
    private Set<Text> closedTables;
    
    // Known region HServerAddress.toString() -> HRegionInterface 
    private Map<String, HRegionInterface> servers;

    private HRegionLocation rootRegionLocation; 
    
    private Map<Text, SortedMap<Text, HRegionLocation>> cachedRegionLocations;
    
    /** 
     * constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public TableServers(HBaseConfiguration conf) {
      this.conf = LocalHBaseCluster.doLocal(new HBaseConfiguration(conf));
      
      String serverClassName =
        conf.get(REGION_SERVER_CLASS, DEFAULT_REGION_SERVER_CLASS);

      this.closed = false;
      
      try {
        this.serverInterfaceClass =
          (Class<? extends HRegionInterface>) Class.forName(serverClassName);
        
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(
            "Unable to find region server interface " + serverClassName, e);
      }

      this.pause = conf.getLong("hbase.client.pause", 30 * 1000);
      this.numRetries = conf.getInt("hbase.client.retries.number", 5);
      
      this.master = null;
      this.masterChecked = false;

      this.cachedRegionLocations = 
        new ConcurrentHashMap<Text, SortedMap<Text, HRegionLocation>>();
      this.closedTables = Collections.synchronizedSet(new HashSet<Text>());
      this.servers = new ConcurrentHashMap<String, HRegionInterface>();
    }
    
    /** {@inheritDoc} */
    public HMasterInterface getMaster() throws MasterNotRunningException {
      synchronized (this.masterLock) {
        for (int tries = 0;
          !this.closed &&
          !this.masterChecked && this.master == null &&
          tries < numRetries;
        tries++) {
          
          HServerAddress masterLocation = new HServerAddress(this.conf.get(
              MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS));

          try {
            HMasterInterface tryMaster = (HMasterInterface)HbaseRPC.getProxy(
                HMasterInterface.class, HMasterInterface.versionID, 
                masterLocation.getInetSocketAddress(), this.conf);
            
            if (tryMaster.isMasterRunning()) {
              this.master = tryMaster;
              break;
            }
            
          } catch (IOException e) {
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
          } catch (InterruptedException e) {
            // continue
          }
        }
        this.masterChecked = true;
      }
      if (this.master == null) {
        throw new MasterNotRunningException();
      }
      return this.master;
    }

    /** {@inheritDoc} */
    public boolean isMasterRunning() {
      if (this.master == null) {
        try {
          getMaster();
          
        } catch (MasterNotRunningException e) {
          return false;
        }
      }
      return true;
    }

    /** {@inheritDoc} */
    public boolean tableExists(final Text tableName) {
      if (tableName == null) {
        throw new IllegalArgumentException("Table name cannot be null");
      }
      boolean exists = false;
      try {
        HTableDescriptor[] tables = listTables();
        for (int i = 0; i < tables.length; i++) {
          if (tables[i].getName().equals(tableName)) {
            exists = true;
          }
        }
      } catch (IOException e) {
        LOG.warn("Testing for table existence threw exception", e);
      }
      return exists;
    }

    /** {@inheritDoc} */
    public HTableDescriptor[] listTables() throws IOException {
      HashSet<HTableDescriptor> uniqueTables = new HashSet<HTableDescriptor>();
      long scannerId = -1L;
      HRegionInterface server = null;
      
      Text startRow = EMPTY_START_ROW;
      HRegionLocation metaLocation = null;

      // scan over the each meta region
      do {
        try{
          // turn the start row into a location
          metaLocation = locateRegion(META_TABLE_NAME, startRow);

          // connect to the server hosting the .META. region
          server = getHRegionConnection(metaLocation.getServerAddress());

          // open a scanner over the meta region
          scannerId = server.openScanner(
            metaLocation.getRegionInfo().getRegionName(),
            COLUMN_FAMILY_ARRAY, startRow, LATEST_TIMESTAMP,
            null);
          
          // iterate through the scanner, accumulating unique table names
          while (true) {
            HbaseMapWritable values = server.next(scannerId);
            if (values == null || values.size() == 0) {
              break;
            }
            for (Map.Entry<Writable, Writable> e: values.entrySet()) {
              HStoreKey key = (HStoreKey) e.getKey();
              if (key.getColumn().equals(COL_REGIONINFO)) {
                HRegionInfo info = new HRegionInfo();
                info = (HRegionInfo) Writables.getWritable(
                    ((ImmutableBytesWritable) e.getValue()).get(), info);

                // Only examine the rows where the startKey is zero length   
                if (info.getStartKey().getLength() == 0) {
                  uniqueTables.add(info.getTableDesc());
                }
              }
            }
          }
          
          server.close(scannerId);
          scannerId = -1L;
          
          // advance the startRow to the end key of the current region
          startRow = metaLocation.getRegionInfo().getEndKey();          
        } catch (IOException e) {
          // Retry once.
          metaLocation = relocateRegion(META_TABLE_NAME, startRow);
          continue;
        }
        finally {
          if (scannerId != -1L) {
            server.close(scannerId);
          }
        }
      } while (startRow.compareTo(LAST_ROW) != 0);
      
      return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
    }

    public HRegionLocation locateRegion(Text tableName, Text row)
    throws IOException{
      return locateRegion(tableName, row, true);
    }

    public HRegionLocation relocateRegion(Text tableName, Text row)
    throws IOException{
      return locateRegion(tableName, row, false);
    }

    private HRegionLocation locateRegion(Text tableName, Text row, 
      boolean useCache)
    throws IOException{
      if (tableName == null || tableName.getLength() == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
            
      if (tableName.equals(ROOT_TABLE_NAME)) {
        synchronized (rootRegionLock) {
          // This block guards against two threads trying to find the root
          // region at the same time. One will go do the find while the 
          // second waits. The second thread will not do find.
          
          if (!useCache || rootRegionLocation == null) {
            return locateRootRegion();
          }
          return rootRegionLocation;
        }        
      } else if (tableName.equals(META_TABLE_NAME)) {
        synchronized (metaRegionLock) {
          // This block guards against two threads trying to load the meta 
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.

          return locateRegionInMeta(ROOT_TABLE_NAME, tableName, row, useCache);
        }
      } else {
        synchronized(userRegionLock){
          return locateRegionInMeta(META_TABLE_NAME, tableName, row, useCache);
        }
      }
    }

    /**
      * Convenience method for turning a MapWritable into the underlying
      * SortedMap we all know and love.
      */
    private SortedMap<Text, byte[]> sortedMapFromMapWritable(
      HbaseMapWritable writable) {
      SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      for (Map.Entry<Writable, Writable> e: writable.entrySet()) {
        HStoreKey key = (HStoreKey) e.getKey();
        results.put(key.getColumn(), 
          ((ImmutableBytesWritable) e.getValue()).get());
      }
      
      return results;
    }

    /**
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    private HRegionLocation locateRegionInMeta(Text parentTable,
      Text tableName, Text row, boolean useCache)
    throws IOException{
      HRegionLocation location = null;
      
      // if we're supposed to be using the cache, then check it for a possible
      // hit. otherwise, delete any existing cached location so it won't 
      // interfere.
      if (useCache) {
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      } else{
        deleteCachedLocation(tableName, row);
      }

      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      Text metaKey = new Text(tableName.toString() + "," 
        + row.toString() + ",999999999999999");

      int tries = 0;
      while (true) {
        tries++;
        
        if (tries >= numRetries) {
          throw new NoServerForRegionException("Unable to find region for " 
            + row + " after " + numRetries + " tries.");
        }

        try{
          // locate the root region
          HRegionLocation metaLocation = locateRegion(parentTable, metaKey);
          HRegionInterface server = 
            getHRegionConnection(metaLocation.getServerAddress());

          // query the root region for the location of the meta region
          HbaseMapWritable regionInfoRow = server.getClosestRowBefore(
            metaLocation.getRegionInfo().getRegionName(), 
            metaKey, HConstants.LATEST_TIMESTAMP);

          if (regionInfoRow == null) {
            throw new TableNotFoundException("Table '" + tableName + 
              "' does not exist.");
          }

          // convert the MapWritable into a Map we can use
          SortedMap<Text, byte[]> results = 
            sortedMapFromMapWritable(regionInfoRow);

          byte[] bytes = results.get(COL_REGIONINFO);

          if (bytes == null || bytes.length == 0) {
            throw new IOException("HRegionInfo was null or empty in " + 
              parentTable);
          }

          // convert the row result into the HRegionLocation we need!
          HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
              results.get(COL_REGIONINFO), new HRegionInfo());

          if (regionInfo.isOffline()) {
            throw new IllegalStateException("region offline: " + 
              regionInfo.getRegionName());
          }

          // possible we got a region of a different table...
          if (!regionInfo.getTableDesc().getName().equals(tableName)) {
            throw new TableNotFoundException(
              "Table '" + tableName + "' was not found.");
          }

          String serverAddress = 
            Writables.bytesToString(results.get(COL_SERVER));
        
          if (serverAddress.equals("")) { 
            throw new NoServerForRegionException(
              "No server address listed in " + parentTable + " for region "
              + regionInfo.getRegionName());
          }
        
          // instantiate the location
          location = new HRegionLocation(regionInfo, 
            new HServerAddress(serverAddress));
      
          cacheLocation(tableName, location);

          return location;
        } catch (IllegalStateException e) {
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("reloading table servers because: " + e.getMessage());
            }
            relocateRegion(parentTable, metaKey);
          } else {
            throw e;
          }
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("reloading table servers because: " + e.getMessage());
            }
            relocateRegion(parentTable, metaKey);
          } else {
            throw e;
          }
        }
      
        try{
          Thread.sleep(pause);              
        } catch (InterruptedException e){
          // continue
        }
      }
    }

    /** 
      * Search the cache for a location that fits our table and row key.
      * Return null if no suitable region is located. TODO: synchronization note
      */
    private HRegionLocation getCachedLocation(Text tableName, Text row) {
      // find the map of cached locations for this table
      SortedMap<Text, HRegionLocation> tableLocations = 
        cachedRegionLocations.get(tableName);

      // if tableLocations for this table isn't built yet, make one
      if (tableLocations == null) {
        tableLocations = new TreeMap<Text, HRegionLocation>();
        cachedRegionLocations.put(tableName, tableLocations);
      }

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (!tableLocations.isEmpty()) {
        if (tableLocations.containsKey(row)) {
          return tableLocations.get(row);
        }
        
        // cut the cache so that we only get the part that could contain
        // regions that match our key
        SortedMap<Text, HRegionLocation> matchingRegions =
          tableLocations.headMap(row);

        // if that portion of the map is empty, then we're done. otherwise,
        // we need to examine the cached location to verify that it is 
        // a match by end key as well.
        if (!matchingRegions.isEmpty()) {
          HRegionLocation possibleRegion = 
            matchingRegions.get(matchingRegions.lastKey());
          
          Text endKey = possibleRegion.getRegionInfo().getEndKey();
          
          // make sure that the end key is greater than the row we're looking 
          // for, otherwise the row actually belongs in the next region, not 
          // this one. the exception case is when the endkey is EMPTY_START_ROW,
          // signifying that the region we're checking is actually the last 
          // region in the table.
          if (endKey.equals(EMPTY_TEXT) || endKey.compareTo(row) > 0) {
            return possibleRegion;
          }
        }
      }
      
      // passed all the way through, so we got nothin - complete cache miss
      return null;
    }


    /**
      * Delete a cached location, if it satisfies the table name and row
      * requirements.
      */
    private void deleteCachedLocation(Text tableName, Text row){
      // find the map of cached locations for this table
      SortedMap<Text, HRegionLocation> tableLocations = 
        cachedRegionLocations.get(tableName);

      // if tableLocations for this table isn't built yet, make one
      if (tableLocations == null) {
        tableLocations = new TreeMap<Text, HRegionLocation>();
        cachedRegionLocations.put(tableName, tableLocations);
      }

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (!tableLocations.isEmpty()) {
        // cut the cache so that we only get the part that could contain
        // regions that match our key
        SortedMap<Text, HRegionLocation> matchingRegions =
          tableLocations.headMap(row);

        // if that portion of the map is empty, then we're done. otherwise,
        // we need to examine the cached location to verify that it is 
        // a match by end key as well.
        if (!matchingRegions.isEmpty()) {
          HRegionLocation possibleRegion = 
            matchingRegions.get(matchingRegions.lastKey());
          
          Text endKey = possibleRegion.getRegionInfo().getEndKey();
          
          // by nature of the map, we know that the start key has to be < 
          // otherwise it wouldn't be in the headMap. 
          if (endKey.compareTo(row) <= 0) {
            // delete any matching entry
            tableLocations.remove(matchingRegions.lastKey());
          }
        }
      }      
    }


    /**
      * Put a newly discovered HRegionLocation into the cache.
      */
    private void cacheLocation(Text tableName, HRegionLocation location){
      Text startKey = location.getRegionInfo().getStartKey();
      
      // find the map of cached locations for this table
      SortedMap<Text, HRegionLocation> tableLocations = 
        cachedRegionLocations.get(tableName);

      // if tableLocations for this table isn't built yet, make one
      if (tableLocations == null) {
        tableLocations = new TreeMap<Text, HRegionLocation>();
        cachedRegionLocations.put(tableName, tableLocations);
      }
      
      // save the HRegionLocation under the startKey
      tableLocations.put(startKey, location);
    }
    
    /** {@inheritDoc} */
    public HRegionInterface getHRegionConnection(
      HServerAddress regionServer) 
    throws IOException {

      HRegionInterface server;
      synchronized (this.servers) {
        // See if we already have a connection
        server = this.servers.get(regionServer.toString());

        if (server == null) { // Get a connection
          long versionId = 0;
          try {
            versionId =
              serverInterfaceClass.getDeclaredField("versionID").getLong(server);
          } catch (IllegalAccessException e) {
            // Should never happen unless visibility of versionID changes
            throw new UnsupportedOperationException(
                "Unable to open a connection to a " +
                serverInterfaceClass.getName() + " server.", e);
          } catch (NoSuchFieldException e) {
            // Should never happen unless versionID field name changes in HRegionInterface
            throw new UnsupportedOperationException(
                "Unable to open a connection to a " +
                serverInterfaceClass.getName() + " server.", e);
          }

          try {
            server = (HRegionInterface)HbaseRPC.waitForProxy(serverInterfaceClass,
                versionId, regionServer.getInetSocketAddress(), this.conf);
          } catch (RemoteException e) {
            throw RemoteExceptionHandler.decodeRemoteException(e);
          }
          this.servers.put(regionServer.toString(), server);
        }
      }
      return server;
    }

    /** {@inheritDoc} */
    public void close(Text tableName) {
      if (tableName == null || tableName.getLength() == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
            
      if (closedTables.contains(tableName)) {
        // Table already closed. Ignore it.
        return;
      }

      closedTables.add(tableName);

      if (cachedRegionLocations.containsKey(tableName)) {
        SortedMap<Text, HRegionLocation> tableServers = 
          cachedRegionLocations.remove(tableName);

        // Shut down connections to the HRegionServers
        synchronized (this.servers) {
          for (HRegionLocation r: tableServers.values()) {
            this.servers.remove(r.getServerAddress().toString());
          }
        }
      }
    }
    
    /** Convenience method for closing all open tables.*/
    void closeAll() {
      this.closed = true;
      ArrayList<Text> tables = 
        new ArrayList<Text>(cachedRegionLocations.keySet());
      for (Text tableName: tables) {
        close(tableName);
      }
    }
    
    /*
     * Repeatedly try to find the root region by asking the master for where it is
     * @return HRegionLocation for root region if found
     * @throws NoServerForRegionException - if the root region can not be located
     * after retrying
     * @throws IOException 
     */
    private HRegionLocation locateRootRegion()
    throws IOException {
    
      getMaster();
      
      HServerAddress rootRegionAddress = null;
      
      for (int tries = 0; tries < numRetries; tries++) {
        int localTimeouts = 0;
        
        // ask the master which server has the root region
        while (rootRegionAddress == null && localTimeouts < numRetries) {
          rootRegionAddress = master.findRootRegion();
          if (rootRegionAddress == null) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Sleeping. Waiting for root region.");
              }
              Thread.sleep(pause);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wake. Retry finding root region.");
              }
            } catch (InterruptedException iex) {
              // continue
            }
            localTimeouts++;
          }
        }
        
        if (rootRegionAddress == null) {
          throw new NoServerForRegionException(
              "Timed out trying to locate root region");
        }
        
        // get a connection to the region server
        HRegionInterface server = getHRegionConnection(rootRegionAddress);

        try {
          // if this works, then we're good, and we have an acceptable address,
          // so we can stop doing retries and return the result.
          server.getRegionInfo(HRegionInfo.rootRegionInfo.getRegionName());
          break;
        } catch (IOException e) {
          if (tries == numRetries - 1) {
            // Don't bother sleeping. We've run out of retries.
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
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
          } catch (InterruptedException iex) {
            // continue
          }
        }
        
        rootRegionAddress = null;
      }
      
      // if the adress is null by this point, then the retries have failed,
      // and we're sort of sunk
      if (rootRegionAddress == null) {
        throw new NoServerForRegionException(
          "unable to locate root region server");
      }
      
      // return the region location
      return new HRegionLocation(
        HRegionInfo.rootRegionInfo, rootRegionAddress);
    }
  }
}
