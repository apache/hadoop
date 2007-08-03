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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.io.KeyedData;

/**
 * A non-instantiable class that manages connections to multiple tables in
 * multiple HBase instances
 */
public class HConnectionManager implements HConstants {
  private HConnectionManager(){}                        // Not instantiable
  
  // A Map of master HServerAddress -> connection information for that instance
  // Note that although the Map is synchronized, the objects it contains
  // are mutable and hence require synchronized access to them
  
  private static final Map<String, HConnection> HBASE_INSTANCES =
    Collections.synchronizedMap(new HashMap<String, HConnection>());

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   * @param conf
   * @return HConnection object for the instance specified by the configuration
   */
  public static HConnection getConnection(Configuration conf) {
    HConnection connection;
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
  public static void deleteConnection(Configuration conf) {
    synchronized (HBASE_INSTANCES) {
      HBASE_INSTANCES.remove(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR));
    }    
  }
  
  /* encapsulates finding the servers for an HBase instance */
  private static class TableServers implements HConnection, HConstants {
    private final Log LOG = LogFactory.getLog(this.getClass().getName());
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long threadWakeFrequency;
    private final long pause;
    private final int numRetries;

    private final Integer masterLock = new Integer(0);
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;
    
    private final Integer rootRegionLock = new Integer(0);
    private final Integer metaRegionLock = new Integer(0);
    
    private volatile Configuration conf;

    // Map tableName -> (Map startRow -> (HRegionInfo, HServerAddress)
    private Map<Text, SortedMap<Text, HRegionLocation>> tablesToServers;
    
    // Set of closed tables
    private Set<Text> closedTables;
    
    // Set of tables currently being located
    private HashSet<Text> tablesBeingLocated;

    // Known region HServerAddress.toString() -> HRegionInterface 
    private HashMap<String, HRegionInterface> servers;

    /** constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public TableServers(Configuration conf) {
      this.conf = conf;
      
      String serverClassName =
        conf.get(REGION_SERVER_CLASS, DEFAULT_REGION_SERVER_CLASS);

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

      this.tablesToServers = Collections.synchronizedMap(
        new HashMap<Text, SortedMap<Text, HRegionLocation>>());
      
      this.closedTables = Collections.synchronizedSet(new HashSet<Text>());
      this.tablesBeingLocated = new HashSet<Text>();
      
      this.servers = new HashMap<String, HRegionInterface>();
    }
    
    /** {@inheritDoc} */
    public HMasterInterface getMaster() throws MasterNotRunningException {
      synchronized (this.masterLock) {
        for (int tries = 0;
        !this.masterChecked && this.master == null && tries < numRetries;
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
      boolean exists = true;
      try {
        SortedMap<Text, HRegionLocation> servers = getTableServers(tableName);
        if (servers == null || servers.size() == 0) {
          exists = false;
        }

      } catch (IOException e) {
        exists = false;
      }
      return exists;
    }

    /** {@inheritDoc} */
    public HTableDescriptor[] listTables() throws IOException {
      TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();

      SortedMap<Text, HRegionLocation> metaTables =
        getTableServers(META_TABLE_NAME);

      for (HRegionLocation t: metaTables.values()) {
        HRegionInterface server = getHRegionConnection(t.getServerAddress());
        long scannerId = -1L;
        try {
          scannerId = server.openScanner(t.getRegionInfo().getRegionName(),
              COLUMN_FAMILY_ARRAY, EMPTY_START_ROW, System.currentTimeMillis(),
              null);

          DataInputBuffer inbuf = new DataInputBuffer();
          while (true) {
            KeyedData[] values = server.next(scannerId);
            if (values.length == 0) {
              break;
            }
            for (int i = 0; i < values.length; i++) {
              if (values[i].getKey().getColumn().equals(COL_REGIONINFO)) {
                inbuf.reset(values[i].getData(), values[i].getData().length);
                HRegionInfo info = new HRegionInfo();
                info.readFields(inbuf);

                // Only examine the rows where the startKey is zero length   
                if (info.startKey.getLength() == 0) {
                  uniqueTables.add(info.tableDesc);
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
    public SortedMap<Text, HRegionLocation>
    getTableServers(Text tableName) throws IOException {
      
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

      SortedMap<Text, HRegionLocation> servers =
        new TreeMap<Text, HRegionLocation>();
      
      // Reload information for the whole table

      servers.putAll(findServersForTable(tableName));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Result of findTable: " + servers.toString());
      }
      
      return servers;
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
        throw new IllegalStateException("table already closed: " + tableName);
      }

      SortedMap<Text, HRegionLocation> tableServers =
        tablesToServers.remove(tableName);

      if (tableServers == null) {
        throw new IllegalArgumentException("table not open: " + tableName);
      }
      
      closedTables.add(tableName);
      
      // Shut down connections to the HRegionServers

      synchronized (this.servers) {
        for (HRegionLocation r: tableServers.values()) {
          this.servers.remove(r.getServerAddress().toString());
        }
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
      
      SortedMap<Text, HRegionLocation> servers =
        new TreeMap<Text, HRegionLocation>();
      
      if (tableName.equals(ROOT_TABLE_NAME)) {
        synchronized (rootRegionLock) {
          // This block guards against two threads trying to find the root
          // region at the same time. One will go do the find while the 
          // second waits. The second thread will not do find.
          
          SortedMap<Text, HRegionLocation> tableServers =
            this.tablesToServers.get(ROOT_TABLE_NAME);
          
          if (tableServers == null) {
            tableServers = locateRootRegion();
          }
          servers.putAll(tableServers);
        }
        
      } else if (tableName.equals(META_TABLE_NAME)) {
        synchronized (metaRegionLock) {
          // This block guards against two threads trying to load the meta 
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          
          if (tablesToServers.get(ROOT_TABLE_NAME) == null) {
            findServersForTable(ROOT_TABLE_NAME);
          }
          
          SortedMap<Text, HRegionLocation> tableServers =
            this.tablesToServers.get(META_TABLE_NAME);
          
          if (tableServers == null) {
            for (int tries = 0; tries < numRetries; tries++) {
              try {
                tableServers = loadMetaFromRoot();
                break;

              } catch (IOException e) {
                if (tries < numRetries - 1) {
                  findServersForTable(ROOT_TABLE_NAME);
                  continue;
                }
                throw e;
              }
            }
          }
          servers.putAll(tableServers);
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
            servers.putAll(tableServers);
          }
        }
        if (!waited) {
          try {
            for (int tries = 0; tries < numRetries; tries++) {
              boolean success = true;                         // assume this works

              SortedMap<Text, HRegionLocation> metaServers =
                this.tablesToServers.get(META_TABLE_NAME);
              if (metaServers == null) {
                metaServers = findServersForTable(META_TABLE_NAME);
              }
              Text firstMetaRegion = metaServers.headMap(tableName).lastKey();
              metaServers = metaServers.tailMap(firstMetaRegion);

              for (HRegionLocation t: metaServers.values()) {
                try {
                  servers.putAll(scanOneMetaRegion(t, tableName));

                } catch (IOException e) {
                  if (tries < numRetries - 1) {
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
          } finally {
            synchronized (this.tablesBeingLocated) {
              // Wake up the threads waiting for us to find the table
              this.tablesBeingLocated.remove(tableName);
              this.tablesBeingLocated.notifyAll();
            }
          }
        }
      }
      this.tablesToServers.put(tableName, servers);
      if (LOG.isDebugEnabled()) {
        for (Map.Entry<Text, HRegionLocation> e: servers.entrySet()) {
          LOG.debug("Server " + e.getKey() + " is serving: " + e.getValue() +
              " for table " + tableName);
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
    private TreeMap<Text, HRegionLocation> loadMetaFromRoot()
    throws IOException {
      
      SortedMap<Text, HRegionLocation> rootRegion =
        this.tablesToServers.get(ROOT_TABLE_NAME);
      
      return scanOneMetaRegion(
          rootRegion.get(rootRegion.firstKey()), META_TABLE_NAME);
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
          rootRegion.getRegionInfo(HGlobals.rootRegionInfo.regionName);
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
          new HRegionLocation(HGlobals.rootRegionInfo, rootRegionLocation));
      
      return rootServer;
    }

    /*
     * Scans a single meta region
     * @param t the meta region we're going to scan
     * @param tableName the name of the table we're looking for
     * @return returns a map of startingRow to TableInfo
     * @throws TableNotFoundException - if table does not exist
     * @throws IllegalStateException - if table is offline
     * @throws NoServerForRegionException - if table can not be found after retrying
     * @throws IOException 
     */
    private TreeMap<Text, HRegionLocation> scanOneMetaRegion(
        final HRegionLocation t, final Text tableName) throws IOException {
      
      HRegionInterface server = getHRegionConnection(t.getServerAddress());
      TreeMap<Text, HRegionLocation> servers =
        new TreeMap<Text, HRegionLocation>();
      
      for (int tries = 0; servers.size() == 0 && tries < numRetries; tries++) {

        long scannerId = -1L;
        try {
          scannerId =
            server.openScanner(t.getRegionInfo().getRegionName(),
                COLUMN_FAMILY_ARRAY, tableName, System.currentTimeMillis(), null);

          DataInputBuffer inbuf = new DataInputBuffer();
          while (true) {
            HRegionInfo regionInfo = null;
            String serverAddress = null;
            KeyedData[] values = server.next(scannerId);
            if (values.length == 0) {
              if (servers.size() == 0) {
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
            for (int i = 0; i < values.length; i++) {
              results.put(values[i].getKey().getColumn(), values[i].getData());
            }
            regionInfo = new HRegionInfo();
            bytes = results.get(COL_REGIONINFO);
            inbuf.reset(bytes, bytes.length);
            regionInfo.readFields(inbuf);

            if (!regionInfo.tableDesc.getName().equals(tableName)) {
              // We're done
              if (LOG.isDebugEnabled()) {
                LOG.debug("Found " + servers.size() + " servers for table " +
                  tableName);
              }
              break;
            }

            if (regionInfo.offLine) {
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
            serverAddress = new String(bytes, UTF8_ENCODING);
            servers.put(regionInfo.startKey, new HRegionLocation(
                regionInfo, new HServerAddress(serverAddress)));
          }
        } catch (IOException e) {
          if (tries == numRetries - 1) {                 // no retries left
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
            }
            throw e;
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
        
        if (servers.size() == 0 && tries == numRetries - 1) {
          throw new NoServerForRegionException("failed to find server for " +
              tableName + " after " + numRetries + " retries");
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
  }
}
