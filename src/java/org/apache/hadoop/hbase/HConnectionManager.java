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
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
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
  
  /* encapsulates finding the servers for an HBase instance */
  private static class TableServers implements HConnection, HConstants {
    private static final Log LOG = LogFactory.getLog(TableServers.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long threadWakeFrequency;
    private final long pause;
    private final int numRetries;

    private final Integer masterLock = new Integer(0);
    private volatile boolean closed;
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;
    
    private final Integer rootRegionLock = new Integer(0);
    private final Integer metaRegionLock = new Integer(0);
    
    private volatile HBaseConfiguration conf;

    // Map tableName -> (Map startRow -> (HRegionInfo, HServerAddress)
    private Map<Text, SortedMap<Text, HRegionLocation>> tablesToServers;
    
    // Set of closed tables
    private Set<Text> closedTables;
    
    // Set of tables currently being located
    private Set<Text> tablesBeingLocated;

    // Known region HServerAddress.toString() -> HRegionInterface 
    private Map<String, HRegionInterface> servers;

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

      this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
      this.pause = conf.getLong("hbase.client.pause", 30 * 1000);
      this.numRetries = conf.getInt("hbase.client.retries.number", 5);
      
      this.master = null;
      this.masterChecked = false;

      this.tablesToServers = 
        new ConcurrentHashMap<Text, SortedMap<Text, HRegionLocation>>();
      
      this.closedTables = Collections.synchronizedSet(new HashSet<Text>());
      this.tablesBeingLocated = Collections.synchronizedSet(
          new HashSet<Text>());
      
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
            HMasterInterface tryMaster = (HMasterInterface)RPC.getProxy(
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

      SortedMap<Text, HRegionLocation> metaTables =
        getTableServers(META_TABLE_NAME);

      for (HRegionLocation t: metaTables.values()) {
        HRegionInterface server = getHRegionConnection(t.getServerAddress());
        long scannerId = -1L;
        try {
          scannerId = server.openScanner(t.getRegionInfo().getRegionName(),
              COLUMN_FAMILY_ARRAY, EMPTY_START_ROW, System.currentTimeMillis(),
              null);

          while (true) {
            MapWritable values = server.next(scannerId);
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
        } catch (RemoteException ex) {
          throw RemoteExceptionHandler.decodeRemoteException(ex);

        } finally {
          if (scannerId != -1L) {
            server.close(scannerId);
          }
        }
      }
      return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
    }

    /** {@inheritDoc} */
    public SortedMap<Text, HRegionLocation> getTableServers(Text tableName)
    throws IOException {
      
      if (tableName == null || tableName.getLength() == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }

      closedTables.remove(tableName);
      
      SortedMap<Text, HRegionLocation> tableServers  =
        tablesToServers.get(tableName);
      
      if (tableServers == null ) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No servers for " + tableName + ". Doing a find...");
        }
        // We don't know where the table is.
        // Load the information from meta.
        tableServers = findServersForTable(tableName);
      }
      SortedMap<Text, HRegionLocation> servers =
        new TreeMap<Text, HRegionLocation>();
      servers.putAll(tableServers);
      return servers;
    }

    /** {@inheritDoc} */
    public SortedMap<Text, HRegionLocation>
    reloadTableServers(final Text tableName) throws IOException {
      closedTables.remove(tableName);
      SortedMap<Text, HRegionLocation> tableServers =
        new TreeMap<Text, HRegionLocation>();
      // Reload information for the whole table
      tableServers.putAll(findServersForTable(tableName));
      if (LOG.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (HRegionLocation location: tableServers.values()) {
          if (sb.length() > 0) {
            sb.append(" ");
          }
          sb.append(count++);
          sb.append(". ");
          sb.append("address=");
          sb.append(location.getServerAddress());
          sb.append(", ");
          sb.append(location.getRegionInfo().getRegionName());
        }
        LOG.debug("Result of findTable on " + tableName.toString() +
          ": " + sb.toString());
      }
      
      return tableServers;
    }

    /** {@inheritDoc} */
    public HRegionInterface getHRegionConnection(
        HServerAddress regionServer) throws IOException {

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
            server = (HRegionInterface) RPC.waitForProxy(serverInterfaceClass,
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

      SortedMap<Text, HRegionLocation> tableServers =
        tablesToServers.remove(tableName);

      if (tableServers == null) {
        // Table not open. Ignore it.
        return;
      }
      
      closedTables.add(tableName);
      
      // Shut down connections to the HRegionServers

      synchronized (this.servers) {
        for (HRegionLocation r: tableServers.values()) {
          this.servers.remove(r.getServerAddress().toString());
        }
      }
    }
    
    void closeAll() {
      this.closed = true;
      ArrayList<Text> tables = new ArrayList<Text>(tablesToServers.keySet());
      for (Text tableName: tables) {
        close(tableName);
      }
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
    private SortedMap<Text, HRegionLocation> findServersForTable(Text tableName)
    throws IOException {
      
      // Wipe out everything we know about this table
      if (this.tablesToServers.remove(tableName) != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wiping out all we know of " + tableName);
        }
      }
      
      SortedMap<Text, HRegionLocation> srvrs =
        new TreeMap<Text, HRegionLocation>();
      
      if (tableName.equals(ROOT_TABLE_NAME)) {
        synchronized (rootRegionLock) {
          // This block guards against two threads trying to find the root
          // region at the same time. One will go do the find while the 
          // second waits. The second thread will not do find.
          
          srvrs = this.tablesToServers.get(ROOT_TABLE_NAME);
          
          if (srvrs == null) {
            srvrs = locateRootRegion();
          }
          this.tablesToServers.put(tableName, srvrs);
        }
        
      } else if (tableName.equals(META_TABLE_NAME)) {
        synchronized (metaRegionLock) {
          // This block guards against two threads trying to load the meta 
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          
          SortedMap<Text, HRegionLocation> rootServers =
            tablesToServers.get(ROOT_TABLE_NAME);
          
          for (boolean refindRoot = true; refindRoot; ) {
            if (rootServers == null || rootServers.size() == 0) {
              // (re)find the root region
              rootServers = findServersForTable(ROOT_TABLE_NAME);
              // but don't try again
              refindRoot = false;
            }
            try {
              srvrs = getTableServers(rootServers, META_TABLE_NAME);
              break;
              
            } catch (NotServingRegionException e) {
              if (!refindRoot) {
                // Already found root once. Give up.
                throw e;
              }
              // The root region must have moved - refind it
              rootServers.clear();
            }
          }
          this.tablesToServers.put(tableName, srvrs);
        }
      } else {
        boolean waited = false;
        synchronized (this.tablesBeingLocated) {
          // This block ensures that only one thread will actually try to
          // find a table. If a second thread comes along it will wait
          // until the first thread finishes finding the table.
          
          while (this.tablesBeingLocated.contains(tableName)) {
            waited = true;
            try {
              this.tablesBeingLocated.wait(threadWakeFrequency);
            } catch (InterruptedException e) {
              // continue
            }
          }
          if (!waited) {
            this.tablesBeingLocated.add(tableName);
            
          } else {
            SortedMap<Text, HRegionLocation> tableServers =
              this.tablesToServers.get(tableName);
            
            if (tableServers == null) {
              throw new TableNotFoundException("table not found: " + tableName);
            }
            srvrs.putAll(tableServers);
          }
        }
        if (!waited) {
          try {
            SortedMap<Text, HRegionLocation> metaServers =
              this.tablesToServers.get(META_TABLE_NAME);

            for (boolean refindMeta = true; refindMeta; ) {
              if (metaServers == null || metaServers.size() == 0) {
                // (re)find the meta table
                metaServers = findServersForTable(META_TABLE_NAME);
                // but don't try again
                refindMeta = false;
              }
              try {
                srvrs = getTableServers(metaServers, tableName);
                break;

              } catch (NotServingRegionException e) {
                if (!refindMeta) {
                  // Already refound meta once. Give up.
                  throw e;
                }
                // The meta table must have moved - refind it
                metaServers.clear();
              }
            }
            this.tablesToServers.put(tableName, srvrs);
          } finally {
            synchronized (this.tablesBeingLocated) {
              // Wake up the threads waiting for us to find the table
              this.tablesBeingLocated.remove(tableName);
              this.tablesBeingLocated.notifyAll();
            }
          }
        }
      }
      this.tablesToServers.put(tableName, srvrs);
      return srvrs;
    }

    /*
     * Repeatedly try to find the root region by asking the master for where it is
     * @return TreeMap<Text, TableInfo> for root regin if found
     * @throws NoServerForRegionException - if the root region can not be located
     * after retrying
     * @throws IOException 
     */
    private TreeMap<Text, HRegionLocation> locateRootRegion()
    throws IOException {
    
      getMaster();
      
      HServerAddress rootRegionLocation = null;
      for (int tries = 0; tries < numRetries; tries++) {
        int localTimeouts = 0;
        while (rootRegionLocation == null && localTimeouts < numRetries) {
          rootRegionLocation = master.findRootRegion();
          if (rootRegionLocation == null) {
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
        
        if (rootRegionLocation == null) {
          throw new NoServerForRegionException(
              "Timed out trying to locate root region");
        }
        
        HRegionInterface rootRegion = getHRegionConnection(rootRegionLocation);

        try {
          rootRegion.getRegionInfo(HRegionInfo.rootRegionInfo.getRegionName());
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
        rootRegionLocation = null;
      }
      
      if (rootRegionLocation == null) {
        throw new NoServerForRegionException(
          "unable to locate root region server");
      }
      
      TreeMap<Text, HRegionLocation> rootServer =
        new TreeMap<Text, HRegionLocation>();
      
      rootServer.put(EMPTY_START_ROW,
          new HRegionLocation(HRegionInfo.rootRegionInfo, rootRegionLocation));
      
      return rootServer;
    }
    
    /*
     * @param metaServers the meta servers that would know where the table is
     * @param tableName name of the table
     * @return map of region start key -> server location
     * @throws IOException
     */
    private SortedMap<Text, HRegionLocation> getTableServers(
        final SortedMap<Text, HRegionLocation> metaServers,
        final Text tableName) throws IOException {

      // If there is more than one meta server, find the first one that should
      // know about the table we are looking for, and reduce the number of
      // servers we need to query.
      
      SortedMap<Text, HRegionLocation> metaServersForTable = metaServers;
      if (metaServersForTable.size() > 1) {
        Text firstMetaRegion = metaServersForTable.headMap(tableName).lastKey();
        metaServersForTable = metaServersForTable.tailMap(firstMetaRegion);
      }
      
      SortedMap<Text, HRegionLocation> tableServers =
        new TreeMap<Text, HRegionLocation>();
      
      int tries = 0;
      do {
        if (tries >= numRetries - 1) {
          throw new NoServerForRegionException(
              "failed to find server for " + tableName + " after "
              + numRetries + " retries");

        } else if (tries > 0) {
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
        for (HRegionLocation t: metaServersForTable.values()) {
          tableServers.putAll(scanOneMetaRegion(t, tableName));
        }
        tries += 1;
      } while (tableServers.size() == 0);
      return tableServers;
    }

    /*
     * Scans a single meta region
     * @param t the meta region we're going to scan
     * @param tableName the name of the table we're looking for
     * @return returns a map of startingRow to TableInfo
     * @throws TableNotFoundException - if table does not exist
     * @throws IllegalStateException - if table is offline
     * @throws IOException 
     */
    private SortedMap<Text, HRegionLocation> scanOneMetaRegion(
        final HRegionLocation t, final Text tableName) throws IOException {
      
      HRegionInterface server = getHRegionConnection(t.getServerAddress());
      TreeMap<Text, HRegionLocation> servers =
        new TreeMap<Text, HRegionLocation>();
      
      long scannerId = -1L;
      try {
        scannerId = server.openScanner(t.getRegionInfo().getRegionName(),
            COLUMN_FAMILY_ARRAY, tableName, System.currentTimeMillis(), null);

        while (true) {
          MapWritable values = server.next(scannerId);
          if (values == null || values.size() == 0) {
            if (servers.size() == 0) {
              // If we didn't find any servers then the table does not exist
              throw new TableNotFoundException("table '" + tableName +
                  "' does not exist in " + t);
            }

            // We found at least one server for the table and now we're done.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found " + servers.size() + " region(s) for " +
                  tableName + " at " + t);
            }
            break;
          }

          SortedMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          for (Map.Entry<Writable, Writable> e: values.entrySet()) {
            HStoreKey key = (HStoreKey) e.getKey();
            results.put(key.getColumn(),
                ((ImmutableBytesWritable) e.getValue()).get());
          }

          byte[] bytes = results.get(COL_REGIONINFO);
          if (bytes == null || bytes.length == 0) {
            // This can be null.  Looks like an info:splitA or info:splitB
            // is only item in the row.
            if (LOG.isDebugEnabled()) {
              LOG.debug(COL_REGIONINFO.toString() + " came back empty: " +
                  results.toString());
            }
            servers.clear();
            break;
          }

          HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
              results.get(COL_REGIONINFO), new HRegionInfo());

          if (!regionInfo.getTableDesc().getName().equals(tableName)) {
            // We're done
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found " + servers.size() + " servers for table " +
                  tableName);
            }
            break;
          }

          if (regionInfo.isSplit()) {
            // Region is a split parent. Skip it.
            continue;
          }

          if (regionInfo.isOffline()) {
            throw new IllegalStateException("table offline: " + tableName);
          }

          bytes = results.get(COL_SERVER);
          if (bytes == null || bytes.length == 0) {
            // We need to rescan because the table we want is unassigned.
            if (LOG.isDebugEnabled()) {
              LOG.debug("no server address for " + regionInfo.toString());
            }
            servers.clear();
            break;
          }

          String serverAddress = Writables.bytesToString(bytes);
          servers.put(regionInfo.getStartKey(), new HRegionLocation(
              regionInfo, new HServerAddress(serverAddress)));
        }
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        throw e;

      } finally {
        if (scannerId != -1L) {
          try {
            server.close(scannerId);
          } catch (Exception ex) {
            LOG.warn(ex);
          }
        }
      }
      return servers;
    }
  }
}
