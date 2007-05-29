/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.lang.Class;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;

/**
 * HClient manages a connection to a single HRegionServer.
 */
public class HClient implements HConstants {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  private static final Text[] META_COLUMNS = {
    COLUMN_FAMILY
  };
  
  private static final Text[] REGIONINFO = {
    COL_REGIONINFO
  };
  
  private static final Text EMPTY_START_ROW = new Text();
  
  private long clientTimeout;
  private int numTimeouts;
  private int numRetries;
  private HMasterInterface master;
  private final Configuration conf;
  
  private static class TableInfo {
    public HRegionInfo regionInfo;
    public HServerAddress serverAddress;

    TableInfo(HRegionInfo regionInfo, HServerAddress serverAddress) {
      this.regionInfo = regionInfo;
      this.serverAddress = serverAddress;
    }
  }
  
  // Map tableName -> (Map startRow -> (HRegionInfo, HServerAddress)
  
  private TreeMap<Text, SortedMap<Text, TableInfo>> tablesToServers;
  
  // For the "current" table: Map startRow -> (HRegionInfo, HServerAddress)
  
  private SortedMap<Text, TableInfo> tableServers;
  
  // Known region HServerAddress.toString() -> HRegionInterface
  
  private TreeMap<String, HRegionInterface> servers;
  
  // For row mutation operations

  private Text currentRegion;
  private HRegionInterface currentServer;
  private Random rand;
  private long clientid;

  /** Creates a new HClient */
  public HClient(Configuration conf) {
    this.conf = conf;

    this.clientTimeout = conf.getLong("hbase.client.timeout.length", 30 * 1000);
    this.numTimeouts = conf.getInt("hbase.client.timeout.number", 5);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    
    this.master = null;
    this.tablesToServers = new TreeMap<Text, SortedMap<Text, TableInfo>>();
    this.tableServers = null;
    this.servers = new TreeMap<String, HRegionInterface>();
    
    // For row mutation operations

    this.currentRegion = null;
    this.currentServer = null;
    this.rand = new Random();
  }
  
  private void handleRemoteException(RemoteException e) throws IOException {
    String msg = e.getMessage();
    if(e.getClassName().equals("org.apache.hadoop.hbase.InvalidColumnNameException")) {
      throw new InvalidColumnNameException(msg);
      
    } else if(e.getClassName().equals("org.apache.hadoop.hbase.LockException")) {
      throw new LockException(msg);
      
    } else if(e.getClassName().equals("org.apache.hadoop.hbase.MasterNotRunningException")) {
      throw new MasterNotRunningException(msg);
      
    } else if(e.getClassName().equals("org.apache.hadoop.hbase.NoServerForRegionException")) {
      throw new NoServerForRegionException(msg);
      
    } else if(e.getClassName().equals("org.apache.hadoop.hbase.NotServingRegionException")) {
      throw new NotServingRegionException(msg);
      
    } else if(e.getClassName().equals("org.apache.hadoop.hbase.TableNotDisabledException")) {
      throw new TableNotDisabledException(msg);
      
    } else {
      throw e;
    }
  }
  
  /* Find the address of the master and connect to it */
  private void checkMaster() throws IOException {
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
      }
      
      // We either cannot connect to the master or it is not running.
      // Sleep and retry
      
      try {
        Thread.sleep(this.clientTimeout);
      } catch(InterruptedException e) {
      }
    }
    if(this.master == null) {
      throw new MasterNotRunningException();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Administrative methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new table
   * 
   * @param desc - table descriptor for table
   * 
   * @throws IllegalArgumentException - if the table name is reserved
   * @throws MasterNotRunningException - if master is not running
   * @throws NoServerForRegionException - if root region is not being served
   * @throws IOException
   */
  public synchronized void createTable(HTableDescriptor desc) throws IOException {
    checkReservedTableName(desc.getName());
    checkMaster();
    try {
      this.master.createTable(desc);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }

    // Save the current table
    
    SortedMap<Text, TableInfo> oldServers = this.tableServers;

    try {
      // Wait for new table to come on-line

      findServersForTable(desc.getName());
      
    } finally {
      if(oldServers != null && oldServers.size() != 0) {
        // Restore old current table if there was one
      
        this.tableServers = oldServers;
      }
    }
  }

  public synchronized void deleteTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    TableInfo firstMetaServer = getFirstMetaServerForTable(tableName);

    try {
      this.master.deleteTable(tableName);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }

    // Wait until first region is deleted
    
    HRegionInterface server = getHRegionConnection(firstMetaServer.serverAddress);

    DataInputBuffer inbuf = new DataInputBuffer();
    HStoreKey key = new HStoreKey();
    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName);
        LabelledData[] values = server.next(scannerId, key);
        if(values == null || values.length == 0) {
          break;
        }
        boolean found = false;
        for(int j = 0; j < values.length; j++) {
          if(values[j].getLabel().equals(COL_REGIONINFO)) {
            byte[] bytes = new byte[values[j].getData().getSize()];
            System.arraycopy(values[j].getData().get(), 0, bytes, 0, bytes.length);
            inbuf.reset(bytes, bytes.length);
            info.readFields(inbuf);
            if(info.tableDesc.getName().equals(tableName)) {
              found = true;
            }
          }
        }
        if(!found) {
          break;
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
        LOG.debug("Sleep. Waiting for first region to be deleted from " + tableName);
      }
      try {
        Thread.sleep(clientTimeout);
        
      } catch(InterruptedException e) {
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be deleted from " + tableName);
      }
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("table deleted " + tableName);
    }
  }

  public synchronized void addColumn(Text tableName, HColumnDescriptor column) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    try {
      this.master.addColumn(tableName, column);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }
  }

  public synchronized void deleteColumn(Text tableName, Text columnName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    try {
      this.master.deleteColumn(tableName, columnName);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }
  }
  
  public synchronized void mergeRegions(Text regionName1, Text regionName2) throws IOException {
    
  }
  
  public synchronized void enableTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    TableInfo firstMetaServer = getFirstMetaServerForTable(tableName);
    
    try {
      this.master.enableTable(tableName);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }

    // Wait until first region is enabled
    
    HRegionInterface server = getHRegionConnection(firstMetaServer.serverAddress);

    DataInputBuffer inbuf = new DataInputBuffer();
    HStoreKey key = new HStoreKey();
    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName);
        LabelledData[] values = server.next(scannerId, key);
        if(values == null || values.length == 0) {
          if(valuesfound == 0) {
            throw new NoSuchElementException("table " + tableName + " not found");
          }
        }
        valuesfound += 1;
        boolean isenabled = false;
        for(int j = 0; j < values.length; j++) {
          if(values[j].getLabel().equals(COL_REGIONINFO)) {
            byte[] bytes = new byte[values[j].getData().getSize()];
            System.arraycopy(values[j].getData().get(), 0, bytes, 0, bytes.length);
            inbuf.reset(bytes, bytes.length);
            info.readFields(inbuf);
            isenabled = !info.offLine;
          }
        }
        if(isenabled) {
          break;
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
        Thread.sleep(clientTimeout);
        
      } catch(InterruptedException e) {
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be enabled from " + tableName);
      }
    }
  }

  public synchronized void disableTable(Text tableName) throws IOException {
    checkReservedTableName(tableName);
    checkMaster();
    TableInfo firstMetaServer = getFirstMetaServerForTable(tableName);

    try {
      this.master.disableTable(tableName);
      
    } catch(RemoteException e) {
      handleRemoteException(e);
    }

    // Wait until first region is disabled
    
    HRegionInterface server = getHRegionConnection(firstMetaServer.serverAddress);

    DataInputBuffer inbuf = new DataInputBuffer();
    HStoreKey key = new HStoreKey();
    HRegionInfo info = new HRegionInfo();
    for(int tries = 0; tries < numRetries; tries++) {
      int valuesfound = 0;
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(firstMetaServer.regionInfo.regionName,
            REGIONINFO, tableName);
        LabelledData[] values = server.next(scannerId, key);
        if(values == null || values.length == 0) {
          if(valuesfound == 0) {
            throw new NoSuchElementException("table " + tableName + " not found");
          }
        }
        valuesfound += 1;
        boolean disabled = false;
        for(int j = 0; j < values.length; j++) {
          if(values[j].getLabel().equals(COL_REGIONINFO)) {
            byte[] bytes = new byte[values[j].getData().getSize()];
            System.arraycopy(values[j].getData().get(), 0, bytes, 0, bytes.length);
            inbuf.reset(bytes, bytes.length);
            info.readFields(inbuf);
            disabled = info.offLine;
          }
        }
        if(disabled) {
          break;
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
        Thread.sleep(clientTimeout);
        
      } catch(InterruptedException e) {
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Waiting for first region to be disabled from " + tableName);
      }
    }
  }
  
  public synchronized void shutdown() throws IOException {
    checkMaster();
    this.master.shutdown();
  }

  /*
   * Verifies that the specified table name is not a reserved name
   * @param tableName - the table name to be checked
   * @throws IllegalArgumentException - if the table name is reserved
   */
  private void checkReservedTableName(Text tableName) {
    if(tableName.equals(ROOT_TABLE_NAME)
        || tableName.equals(META_TABLE_NAME)) {
      
      throw new IllegalArgumentException(tableName + " is a reserved table name");
    }
  }
  
  private TableInfo getFirstMetaServerForTable(Text tableName) throws IOException {
    SortedMap<Text, TableInfo> metaservers = findMetaServersForTable(tableName);
    return metaservers.get(metaservers.firstKey());
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Client API
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Loads information so that a table can be manipulated.
   * 
   * @param tableName - the table to be located
   * @throws IOException - if the table can not be located after retrying
   */
  public synchronized void openTable(Text tableName) throws IOException {
    if(tableName == null || tableName.getLength() == 0) {
      throw new IllegalArgumentException("table name cannot be null or zero length");
    }
    this.tableServers = tablesToServers.get(tableName);
    if(this.tableServers == null ) {
      // We don't know where the table is.
      // Load the information from meta.
      this.tableServers = findServersForTable(tableName);
    }
  }

  /*
   * Locates a table by searching the META region
   * 
   * @param tableName - name of table to find servers for
   * @return - map of first row to table info for all regions in the table
   * @throws IOException
   */
  private SortedMap<Text, TableInfo> findServersForTable(Text tableName)
      throws IOException {

    SortedMap<Text, TableInfo> servers = null;
    if(tableName.equals(ROOT_TABLE_NAME)) {
      servers = locateRootRegion();

    } else if(tableName.equals(META_TABLE_NAME)) {
      servers = loadMetaFromRoot();
      
    } else {
      servers = new TreeMap<Text, TableInfo>();
      for(TableInfo t: findMetaServersForTable(tableName).values()) {
        servers.putAll(scanOneMetaRegion(t, tableName));
      }
      this.tablesToServers.put(tableName, servers);
    }
    return servers;
  }

  /*
   * Finds the meta servers that contain information about the specified table
   * @param tableName - the name of the table to get information about
   * @return - returns a SortedMap of the meta servers
   * @throws IOException
   */
  private SortedMap<Text, TableInfo> findMetaServersForTable(Text tableName)
      throws IOException {
    
    SortedMap<Text, TableInfo> metaServers = 
      this.tablesToServers.get(META_TABLE_NAME);
    
    if(metaServers == null) {                 // Don't know where the meta is
      metaServers = loadMetaFromRoot();
    }
    Text firstMetaRegion = (metaServers.containsKey(tableName)) ?
        tableName : metaServers.headMap(tableName).lastKey();

    return metaServers.tailMap(firstMetaRegion);
  }

  /*
   * Load the meta table from the root table.
   * 
   * @return map of first row to TableInfo for all meta regions
   * @throws IOException
   */
  private TreeMap<Text, TableInfo> loadMetaFromRoot() throws IOException {
    SortedMap<Text, TableInfo> rootRegion =
      this.tablesToServers.get(ROOT_TABLE_NAME);
    
    if(rootRegion == null) {
      rootRegion = locateRootRegion();
    }
    return scanRoot(rootRegion.get(rootRegion.firstKey()));
  }
  
  /*
   * Repeatedly try to find the root region by asking the master for where it is
   * 
   * @return TreeMap<Text, TableInfo> for root regin if found
   * @throws NoServerForRegionException - if the root region can not be located after retrying
   * @throws IOException 
   */
  private TreeMap<Text, TableInfo> locateRootRegion() throws IOException {
    checkMaster();
    
    HServerAddress rootRegionLocation = null;
    for(int tries = 0; rootRegionLocation == null && tries < numRetries; tries++){
      int localTimeouts = 0;
      while(rootRegionLocation == null && localTimeouts < numTimeouts) {
        rootRegionLocation = master.findRootRegion();

        if(rootRegionLocation == null) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleeping. Waiting for root region.");
            }
            Thread.sleep(this.clientTimeout);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Wake. Retry finding root region.");
            }
          } catch(InterruptedException iex) {
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
        
      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // Don't bother sleeping. We've run out of retries.
          break;
        }
        
        // Sleep and retry finding root region.

        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Root region location changed. Sleeping.");
          }
          Thread.sleep(this.clientTimeout);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Wake. Retry finding root region.");
          }
        } catch(InterruptedException iex) {
        }
      }
      rootRegionLocation = null;
    }
    
    if (rootRegionLocation == null) {
      throw new NoServerForRegionException(
          "unable to locate root region server");
    }
    
    TreeMap<Text, TableInfo> rootServer = new TreeMap<Text, TableInfo>();
    rootServer.put(EMPTY_START_ROW,
        new TableInfo(HGlobals.rootRegionInfo, rootRegionLocation));
    
    this.tablesToServers.put(ROOT_TABLE_NAME, rootServer);
    return rootServer;
  }

  /* 
   * Scans the root region to find all the meta regions
   * @return - TreeMap of meta region servers
   * @throws IOException
   */
  private TreeMap<Text, TableInfo> scanRoot(TableInfo rootRegion)
      throws IOException {
    
    TreeMap<Text, TableInfo> metaservers =
      scanOneMetaRegion(rootRegion, META_TABLE_NAME);
    this.tablesToServers.put(META_TABLE_NAME, metaservers);
    return metaservers;
  }

  /*
   * Scans a single meta region
   * @param t the meta region we're going to scan
   * @param tableName the name of the table we're looking for
   * @return returns a map of startingRow to TableInfo
   * @throws NoSuchElementException - if table does not exist
   * @throws IllegalStateException - if table is offline
   * @throws NoServerForRegionException - if table can not be found after retrying
   * @throws IOException 
   */
  private TreeMap<Text, TableInfo> scanOneMetaRegion(TableInfo t, Text tableName)
      throws IOException {
    
    HRegionInterface server = getHRegionConnection(t.serverAddress);
    TreeMap<Text, TableInfo> servers = new TreeMap<Text, TableInfo>();
    for(int tries = 0; servers.size() == 0 && tries < this.numRetries;
        tries++) {
  
      long scannerId = -1L;
      try {
        scannerId =
          server.openScanner(t.regionInfo.regionName, META_COLUMNS, tableName);

        DataInputBuffer inbuf = new DataInputBuffer();
        while(true) {
          HRegionInfo regionInfo = null;
          String serverAddress = null;
          HStoreKey key = new HStoreKey();
          LabelledData[] values = server.next(scannerId, key);
          if(values.length == 0) {
            if(servers.size() == 0) {
              // If we didn't find any servers then the table does not exist

              throw new NoSuchElementException("table '" + tableName
                  + "' does not exist");
            }

            // We found at least one server for the table and now we're done.

            break;
          }

          byte[] bytes = null;
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          for(int i = 0; i < values.length; i++) {
            bytes = new byte[values[i].getData().getSize()];
            System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
            results.put(values[i].getLabel(), bytes);
          }
          regionInfo = new HRegionInfo();
          bytes = results.get(COL_REGIONINFO);
          inbuf.reset(bytes, bytes.length);
          regionInfo.readFields(inbuf);

          if(!regionInfo.tableDesc.getName().equals(tableName)) {
            // We're done
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
              new TableInfo(regionInfo, new HServerAddress(serverAddress)));
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
        
      if(servers.size() == 0 && tries == this.numRetries - 1) {
        throw new NoServerForRegionException("failed to find server for "
            + tableName + " after " + this.numRetries + " retries");
      }

      // The table is not yet being served. Sleep and retry.

      if(LOG.isDebugEnabled()) {
        LOG.debug("Sleeping. Table " + tableName
            + " not currently being served.");
      }
      try {
        Thread.sleep(this.clientTimeout);

      } catch(InterruptedException e) {
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wake. Retry finding table " + tableName);
      }
    }
    return servers;
  }

  /* 
   * Establishes a connection to the region server at the specified address
   * @param regionServer - the server to connect to
   * @throws IOException
   */
  synchronized HRegionInterface getHRegionConnection(HServerAddress regionServer)
      throws IOException {

      // See if we already have a connection

    HRegionInterface server = this.servers.get(regionServer.toString());
    
    if(server == null) {                                // Get a connection
      
      server = (HRegionInterface)RPC.waitForProxy(HRegionInterface.class, 
          HRegionInterface.versionID, regionServer.getInetSocketAddress(),
          this.conf);
      
      this.servers.put(regionServer.toString(), server);
    }
    return server;
  }

  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   */
  public synchronized HTableDescriptor[] listTables()
      throws IOException {
    TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    
    SortedMap<Text, TableInfo> metaTables =
      this.tablesToServers.get(META_TABLE_NAME);
    
    if(metaTables == null) {
      // Meta is not loaded yet so go do that
      metaTables = loadMetaFromRoot();
    }

    for (TableInfo t: metaTables.values()) {
      HRegionInterface server = getHRegionConnection(t.serverAddress);
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(t.regionInfo.regionName,
            META_COLUMNS, EMPTY_START_ROW);
        
        HStoreKey key = new HStoreKey();
        DataInputBuffer inbuf = new DataInputBuffer();
        while(true) {
          LabelledData[] values = server.next(scannerId, key);
          if(values.length == 0) {
            break;
          }
          for(int i = 0; i < values.length; i++) {
            if(values[i].getLabel().equals(COL_REGIONINFO)) {
              byte[] bytes = values[i].getData().get();
              inbuf.reset(bytes, bytes.length);
              HRegionInfo info = new HRegionInfo();
              info.readFields(inbuf);

              // Only examine the rows where the startKey is zero length   
              if(info.startKey.getLength() == 0) {
                uniqueTables.add(info.tableDesc);
              }
            }
          }
        }
      } finally {
        if(scannerId != -1L) {
          server.close(scannerId);
        }
      }
    }
    return (HTableDescriptor[])uniqueTables.
      toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  private synchronized TableInfo getTableInfo(Text row) {
    if(row == null || row.getLength() == 0) {
      throw new IllegalArgumentException("row key cannot be null or zero length");
    }
    if(this.tableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    
    // Only one server will have the row we are looking for
    
    Text serverKey = null;
    if(this.tableServers.containsKey(row)) {
      serverKey = row;
      
    } else {
      serverKey = this.tableServers.headMap(row).lastKey();
    }
    return this.tableServers.get(serverKey);
  }
  
  private synchronized void findRegion(TableInfo info) throws IOException {
    
    // Wipe out everything we know about this table
    
    this.tablesToServers.remove(info.regionInfo.tableDesc.getName());
    this.tableServers.clear();
    
    // Reload information for the whole table
    
    this.tableServers = findServersForTable(info.regionInfo.tableDesc.getName());
    
    if(this.tableServers.get(info.regionInfo.startKey) == null ) {
      throw new IOException("region " + info.regionInfo.regionName
          + " does not exist");
    }
  }
  
  /** Get a single value for the specified row and column */
  public byte[] get(Text row, Text column) throws IOException {
    TableInfo info = null;
    BytesWritable value = null;

    for(int tries = 0; tries < numRetries && info == null; tries++) {
      info = getTableInfo(row);
      
      try {
        value = getHRegionConnection(info.serverAddress).get(
            info.regionInfo.regionName, row, column);
        
      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        findRegion(info);
        info = null;
      }
    }

    if(value != null) {
      byte[] bytes = new byte[value.getSize()];
      System.arraycopy(value.get(), 0, bytes, 0, bytes.length);
      return bytes;
    }
    return null;
  }
 
  /** Get the specified number of versions of the specified row and column */
  public byte[][] get(Text row, Text column, int numVersions) throws IOException {
    TableInfo info = null;
    BytesWritable[] values = null;

    for(int tries = 0; tries < numRetries && info == null; tries++) {
      info = getTableInfo(row);
      
      try {
        values = getHRegionConnection(info.serverAddress).get(
            info.regionInfo.regionName, row, column, numVersions);
        
      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        findRegion(info);
        info = null;
      }
    }

    if(values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for(int i = 0 ; i < values.length; i++) {
        byte[] value = new byte[values[i].getSize()];
        System.arraycopy(values[i].get(), 0, value, 0, value.length);
        bytes.add(value);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
  }
  
  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   */
  public byte[][] get(Text row, Text column, long timestamp, int numVersions) throws IOException {
    TableInfo info = null;
    BytesWritable[] values = null;

    for(int tries = 0; tries < numRetries && info == null; tries++) {
      info = getTableInfo(row);
      
      try {
        values = getHRegionConnection(info.serverAddress).get(
            info.regionInfo.regionName, row, column, timestamp, numVersions);
    
      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        findRegion(info);
        info = null;
      }
    }

    if(values != null) {
      ArrayList<byte[]> bytes = new ArrayList<byte[]>();
      for(int i = 0 ; i < values.length; i++) {
        byte[] value = new byte[values[i].getSize()];
        System.arraycopy(values[i].get(), 0, value, 0, value.length);
        bytes.add(value);
      }
      return bytes.toArray(new byte[values.length][]);
    }
    return null;
  }
    
  /** Get all the data for the specified row */
  public LabelledData[] getRow(Text row) throws IOException {
    TableInfo info = null;
    LabelledData[] value = null;
    
    for(int tries = 0; tries < numRetries && info == null; tries++) {
      info = getTableInfo(row);
      
      try {
        value = getHRegionConnection(info.serverAddress).getRow(
            info.regionInfo.regionName, row);
        
      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        findRegion(info);
        info = null;
      }
    }
    
    return value;
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   */
  public synchronized HScannerInterface obtainScanner(Text[] columns, Text startRow) throws IOException {
    if(this.tableServers == null) {
      throw new IllegalStateException("Must open table first");
    }
    return new ClientScanner(columns, startRow);
  }

  /** Start an atomic row insertion or update */
  public long startUpdate(Text row) throws IOException {
    TableInfo info = null;
    long lockid = -1L;
    
    for(int tries = 0; tries < numRetries && info == null; tries++) {
      info = getTableInfo(row);
      
      try {
        this.currentServer = getHRegionConnection(info.serverAddress);
        this.currentRegion = info.regionInfo.regionName;
        this.clientid = rand.nextLong();
        lockid = currentServer.startUpdate(this.currentRegion, this.clientid, row);

      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          // No more tries
          throw e;
        }
        findRegion(info);
        info = null;

      } catch(IOException e) {
        this.currentServer = null;
        this.currentRegion = null;
        throw e;
      }
      
    }
    
    return lockid;
  }
  
  /** Change a value for the specified column */
  public void put(long lockid, Text column, byte val[]) throws IOException {
    try {
      this.currentServer.put(this.currentRegion, this.clientid, lockid, column,
          new BytesWritable(val));
      
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
        
      } catch(IOException e2) {
      }
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Delete the value for a column */
  public void delete(long lockid, Text column) throws IOException {
    try {
      this.currentServer.delete(this.currentRegion, this.clientid, lockid, column);
      
    } catch(IOException e) {
      try {
        this.currentServer.abort(this.currentRegion, this.clientid, lockid);
        
      } catch(IOException e2) {
      }
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Abort a row mutation */
  public void abort(long lockid) throws IOException {
    try {
      this.currentServer.abort(this.currentRegion, this.clientid, lockid);
    } catch(IOException e) {
      this.currentServer = null;
      this.currentRegion = null;
      throw e;
    }
  }
  
  /** Finalize a row mutation */
  public void commit(long lockid) throws IOException {
    try {
      this.currentServer.commit(this.currentRegion, this.clientid, lockid);
      
    } finally {
      this.currentServer = null;
      this.currentRegion = null;
    }
  }
  
  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  private class ClientScanner implements HScannerInterface {
    private Text[] columns;
    private Text startRow;
    private boolean closed;
    private TableInfo[] regions;
    private int currentRegion;
    private HRegionInterface server;
    private long scannerId;
    
    private void loadRegions() {
      Text firstServer = null;
      if(this.startRow == null || this.startRow.getLength() == 0) {
        firstServer = tableServers.firstKey();

      } else if(tableServers.containsKey(startRow)) {
        firstServer = startRow;

      } else {
        firstServer = tableServers.headMap(startRow).lastKey();
      }
      Collection<TableInfo> info = tableServers.tailMap(firstServer).values();
      this.regions = info.toArray(new TableInfo[info.size()]);
    }
    
    public ClientScanner(Text[] columns, Text startRow) throws IOException {
      this.columns = columns;
      this.startRow = startRow;
      this.closed = false;
      
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
        this.server = getHRegionConnection(this.regions[currentRegion].serverAddress);
        
        for(int tries = 0; tries < numRetries; tries++) {
          TableInfo info = this.regions[currentRegion];
          
          try {
            this.scannerId = this.server.openScanner(info.regionInfo.regionName,
                this.columns, currentRegion == 0 ? this.startRow : EMPTY_START_ROW);
            
            break;
        
          } catch(NotServingRegionException e) {
            if(tries == numRetries - 1) {
              // No more tries
              throw e;
            }
            findRegion(info);
            loadRegions();
          }
        }

      } catch(IOException e) {
        close();
        throw e;
      }
      return true;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HScannerInterface#next(org.apache.hadoop.hbase.HStoreKey, java.util.TreeMap)
     */
    public boolean next(HStoreKey key, TreeMap<Text, byte[]> results) throws IOException {
      if(this.closed) {
        return false;
      }
      LabelledData[] values = null;
      do {
        values = this.server.next(this.scannerId, key);
      } while(values.length == 0 && nextScanner());

      for(int i = 0; i < values.length; i++) {
        byte[] bytes = new byte[values[i].getData().getSize()];
        System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
        results.put(values[i].getLabel(), bytes);
      }
      return values.length != 0;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.HScannerInterface#close()
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
    System.err.println(" createTable  Takes table name, column families,... ");
    System.err.println(" deleteTable  Takes a table name.");
    System.err.println(" iistTables   List all tables.");
    System.err.println("Example Usage:");
    System.err.println(" % java " + this.getClass().getName() + " shutdown");
    System.err.println(" % java " + this.getClass().getName() +
        " createTable webcrawl contents: anchors: 10");
  }
  
  public int doCommandLine(final String args[]) {
    // Process command-line args. TODO: Better cmd-line processing
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
            throw new IllegalArgumentException("Must supply a table name " +
              "and at least one column family");
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
            throw new IllegalArgumentException("Must supply a table name");
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
  
  public static void main(final String args[]) {
    Configuration c = new HBaseConfiguration();
    int errCode = (new HClient(c)).doCommandLine(args);
    System.exit(errCode);
  }
}
