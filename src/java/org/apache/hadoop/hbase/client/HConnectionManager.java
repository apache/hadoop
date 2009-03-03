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
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.ipc.RemoteException;

/**
 * A non-instantiable class that manages connections to multiple tables in
 * multiple HBase instances.
 * 
 * Used by {@link HTable} and {@link HBaseAdmin}
 */
public class HConnectionManager implements HConstants {

  /*
   * Not instantiable.
   */
  protected HConnectionManager() {
    super();
  }
  
  // A Map of master HServerAddress -> connection information for that instance
  // Note that although the Map is synchronized, the objects it contains
  // are mutable and hence require synchronized access to them
  private static final Map<String, TableServers> HBASE_INSTANCES =
    new ConcurrentHashMap<String, TableServers>();

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   * @param conf
   * @return HConnection object for the instance specified by the configuration
   */
  public static HConnection getConnection(HBaseConfiguration conf) {
    TableServers connection;
    synchronized (HBASE_INSTANCES) {
      String instanceName = conf.get(HBASE_DIR);
      connection = HBASE_INSTANCES.get(instanceName);
      if (connection == null) {
        connection = new TableServers(conf);
        HBASE_INSTANCES.put(instanceName, connection);
      }
    }
    return connection;
  }
  
  /**
   * Delete connection information for the instance specified by configuration
   * @param conf
   * @param stopProxy
   */
  public static void deleteConnectionInfo(HBaseConfiguration conf,
      boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      TableServers t = HBASE_INSTANCES.remove(conf.get(HBASE_DIR));
      if (t != null) {
        t.close(stopProxy);
      }
    }
  }

  /* Encapsulates finding the servers for an HBase instance */
  private static class TableServers implements ServerConnection, HConstants {
    private static final Log LOG = LogFactory.getLog(TableServers.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long pause;
    private final int numRetries;
    private final int maxRPCAttempts;

    private final Integer masterLock = new Integer(0);
    private volatile boolean closed;
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;
    
    private final Integer rootRegionLock = new Integer(0);
    private final Integer metaRegionLock = new Integer(0);
    private final Integer userRegionLock = new Integer(0);
        
    private volatile HBaseConfiguration conf;
    
    // Known region HServerAddress.toString() -> HRegionInterface 
    private final Map<String, HRegionInterface> servers =
      new ConcurrentHashMap<String, HRegionInterface>();

    // Used by master and region servers during safe mode only
    private volatile HRegionLocation rootRegionLocation; 
    
    private final Map<Integer, SoftValueSortedMap<byte [], HRegionLocation>> 
      cachedRegionLocations =
        new HashMap<Integer, SoftValueSortedMap<byte [], HRegionLocation>>();

    private ZooKeeperWrapper zooKeeperWrapper;

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

      this.pause = conf.getLong("hbase.client.pause", 2 * 1000);
      this.numRetries = conf.getInt("hbase.client.retries.number", 10);
      this.maxRPCAttempts = conf.getInt("hbase.client.rpc.maxattempts", 1);
      
      this.master = null;
      this.masterChecked = false;
    }

    private long getPauseTime(int tries) {
      int ntries = tries;
      if (ntries >= HConstants.RETRY_BACKOFF.length)
        ntries = HConstants.RETRY_BACKOFF.length - 1;
      return this.pause * HConstants.RETRY_BACKOFF[ntries];
    }

    // Used by master and region servers during safe mode only
    public void unsetRootRegionLocation() {
      this.rootRegionLocation = null;
    }
    
    // Used by master and region servers during safe mode only
    public void setRootRegionLocation(HRegionLocation rootRegion) {
      if (rootRegion == null) {
        throw new IllegalArgumentException(
            "Cannot set root region location to null.");
      }
      this.rootRegionLocation = rootRegion;
    }
    
    public HMasterInterface getMaster() throws MasterNotRunningException {
      HServerAddress masterLocation = null;
      synchronized (this.masterLock) {
        for (int tries = 0;
          !this.closed &&
          !this.masterChecked && this.master == null &&
          tries < numRetries;
        tries++) {
          
          masterLocation = new HServerAddress(this.conf.get(MASTER_ADDRESS,
            DEFAULT_MASTER_ADDRESS));
          try {
            HMasterInterface tryMaster = (HMasterInterface)HBaseRPC.getProxy(
                HMasterInterface.class, HBaseRPCProtocolVersion.versionID, 
                masterLocation.getInetSocketAddress(), this.conf);
            
            if (tryMaster.isMasterRunning()) {
              this.master = tryMaster;
              break;
            }
            
          } catch (IOException e) {
            if (tries == numRetries - 1) {
              // This was our last chance - don't bother sleeping
              break;
            }
            LOG.info("getMaster attempt " + tries + " of " + this.numRetries +
              " failed; retrying after sleep of " +
              getPauseTime(tries), e);
          }

          // Cannot connect to master or it is not running. Sleep & retry
          try {
            Thread.sleep(getPauseTime(tries));
          } catch (InterruptedException e) {
            // continue
          }
        }
        this.masterChecked = true;
      }
      if (this.master == null) {
        if (masterLocation == null) {
          throw new MasterNotRunningException();
        }
        throw new MasterNotRunningException(masterLocation.toString());
      }
      return this.master;
    }

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

    public boolean tableExists(final byte [] tableName)
    throws MasterNotRunningException {
      getMaster();
      if (tableName == null) {
        throw new IllegalArgumentException("Table name cannot be null");
      }
      if (isMetaTableName(tableName)) {
        return true;
      }
      boolean exists = false;
      try {
        HTableDescriptor[] tables = listTables();
        for (int i = 0; i < tables.length; i++) {
          if (Bytes.equals(tables[i].getName(), tableName)) {
            exists = true;
          }
        }
      } catch (IOException e) {
        LOG.warn("Testing for table existence threw exception", e);
      }
      return exists;
    }
    
    /*
     * @param n
     * @return Truen if passed tablename <code>n</code> is equal to the name
     * of a catalog table.
     */
    private static boolean isMetaTableName(final byte [] n) {
      return MetaUtils.isMetaTableName(n);
    }

    public HRegionLocation getRegionLocation(final byte [] name,
        final byte [] row, boolean reload)
    throws IOException {
      getMaster();
      return reload? relocateRegion(name, row): locateRegion(name, row);
    }

    public HTableDescriptor[] listTables() throws IOException {
      getMaster();
      final TreeSet<HTableDescriptor> uniqueTables =
        new TreeSet<HTableDescriptor>();

      MetaScannerVisitor visitor = new MetaScannerVisitor() {

        public boolean processRow(RowResult rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfo(
              rowResult.get(COL_REGIONINFO));

          // Only examine the rows where the startKey is zero length
          if (info.getStartKey().length == 0) {
            uniqueTables.add(info.getTableDesc());
          }
          return true;
        }

      };
      MetaScanner.metaScan(conf, visitor);

      return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
    }
    
    public boolean isTableEnabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, true);
    }
    
    public boolean isTableDisabled(byte[] tableName) throws IOException {
      return testTableOnlineState(tableName, false);
    }

    /*
     * If online == true
     *   Returns true if all regions are online
     *   Returns false in any other case
     * If online == false
     *   Returns true if all regions are offline
     *   Returns false in any other case
     */
    private boolean testTableOnlineState(byte[] tableName, 
        boolean online) throws IOException {
      if (!tableExists(tableName)) {
        throw new TableNotFoundException(Bytes.toString(tableName));
      }
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        // The root region is always enabled
        return true;
      }
      
      int rowsScanned = 0;
      int rowsOffline = 0;
      byte[] startKey =
        HRegionInfo.createRegionName(tableName, null, HConstants.ZEROES);
      byte[] endKey = null;
      HRegionInfo currentRegion = null;
      ScannerCallable s = new ScannerCallable(this, 
          (Bytes.equals(tableName, HConstants.META_TABLE_NAME) ?
              HConstants.ROOT_TABLE_NAME : HConstants.META_TABLE_NAME),
          HConstants.COL_REGIONINFO_ARRAY, startKey,
          HConstants.LATEST_TIMESTAMP, null
      );
      try {
        // Open scanner
        getRegionServerWithRetries(s);
        do {
          HRegionInfo oldRegion = currentRegion;
          if (oldRegion != null) {
            startKey = oldRegion.getEndKey();
          }
          currentRegion = s.getHRegionInfo();
          RowResult r = null;
          RowResult[] rrs = null;
          while ((rrs = getRegionServerWithRetries(s)) != null) {
            r = rrs[0];
            Cell c = r.get(HConstants.COL_REGIONINFO);
            if (c != null) {
              byte[] value = c.getValue();
              if (value != null) {
                HRegionInfo info = Writables.getHRegionInfoOrNull(value);
                if (info != null) {
                  if (Bytes.equals(info.getTableDesc().getName(), tableName)) {
                    rowsScanned += 1;
                    rowsOffline += info.isOffline() ? 1 : 0;
                  }
                }
              }
            }
          }
          endKey = currentRegion.getEndKey();
        } while (!(endKey == null || HStoreKey.equalsTwoRowKeys(endKey,
            HConstants.EMPTY_BYTE_ARRAY)));
      }
      finally {
        s.setClose();
      }
      boolean onlineOffline = 
        online ? rowsOffline == 0 : rowsOffline == rowsScanned;
      return rowsScanned > 0 && onlineOffline;
      
    }
    
    private class HTableDescriptorFinder 
    implements MetaScanner.MetaScannerVisitor {
        byte[] tableName;
        HTableDescriptor result;
        protected HTableDescriptorFinder(byte[] tableName) {
          this.tableName = tableName;
        }
        public boolean processRow(RowResult rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfo(
            rowResult.get(HConstants.COL_REGIONINFO));
          HTableDescriptor desc = info.getTableDesc();
          if (Bytes.compareTo(desc.getName(), tableName) == 0) {
            result = desc;
            return false;
          }
          return true;
        }
        HTableDescriptor getResult() {
          return result;
        }
    }

    public HTableDescriptor getHTableDescriptor(final byte[] tableName)
    throws IOException {
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.ROOT_TABLEDESC);
      }
      if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return new UnmodifyableHTableDescriptor(HTableDescriptor.META_TABLEDESC);
      }
      HTableDescriptorFinder finder = new HTableDescriptorFinder(tableName);
      MetaScanner.metaScan(conf, finder);
      HTableDescriptor result = finder.getResult();
      if (result == null) {
        throw new TableNotFoundException(Bytes.toString(tableName));
      }
      return result;
    }

    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      getMaster();
      return locateRegion(tableName, row, true);
    }

    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      getMaster();
      return locateRegion(tableName, row, false);
    }

    private HRegionLocation locateRegion(final byte [] tableName,
      final byte [] row, boolean useCache)
    throws IOException{
      if (tableName == null || tableName.length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
            
      if (Bytes.equals(tableName, ROOT_TABLE_NAME)) {
        synchronized (rootRegionLock) {
          // This block guards against two threads trying to find the root
          // region at the same time. One will go do the find while the 
          // second waits. The second thread will not do find.
          
          if (!useCache || rootRegionLocation == null) {
            return locateRootRegion();
          }
          return rootRegionLocation;
        }        
      } else if (Bytes.equals(tableName, META_TABLE_NAME)) {
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

    /*
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    private HRegionLocation locateRegionInMeta(final byte [] parentTable,
      final byte [] tableName, final byte [] row, boolean useCache)
    throws IOException{
      HRegionLocation location = null;
      // If supposed to be using the cache, then check it for a possible hit.
      // Otherwise, delete any existing cached location so it won't interfere.
      if (useCache) {
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      } else {
        deleteCachedLocation(tableName, row);
      }
      
      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      byte [] metaKey = HRegionInfo.createRegionName(tableName, row,
        HConstants.NINES);
      for (int tries = 0; true; tries++) {
        if (tries >= numRetries) {
          throw new NoServerForRegionException("Unable to find region for " 
            + Bytes.toString(row) + " after " + numRetries + " tries.");
        }

        try {
          // locate the root region
          HRegionLocation metaLocation = locateRegion(parentTable, metaKey);
          HRegionInterface server =
            getHRegionConnection(metaLocation.getServerAddress());

          // Query the root region for the location of the meta region
          RowResult regionInfoRow = server.getClosestRowBefore(
            metaLocation.getRegionInfo().getRegionName(), metaKey,
            HConstants.COLUMN_FAMILY);
          if (regionInfoRow == null) {
            throw new TableNotFoundException(Bytes.toString(tableName));
          }

          Cell value = regionInfoRow.get(COL_REGIONINFO);
          if (value == null || value.getValue().length == 0) {
            throw new IOException("HRegionInfo was null or empty in " + 
              Bytes.toString(parentTable));
          }
          // convert the row result into the HRegionLocation we need!
          HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
              value.getValue(), new HRegionInfo());
          // possible we got a region of a different table...
          if (!Bytes.equals(regionInfo.getTableDesc().getName(), tableName)) {
            throw new TableNotFoundException(
              "Table '" + Bytes.toString(tableName) + "' was not found.");
          }
          if (regionInfo.isOffline()) {
            throw new RegionOfflineException("region offline: " + 
              regionInfo.getRegionNameAsString());
          }
          
          String serverAddress = 
            Writables.cellToString(regionInfoRow.get(COL_SERVER));
          if (serverAddress.equals("")) { 
            throw new NoServerForRegionException("No server address listed " +
              "in " + Bytes.toString(parentTable) + " for region " +
              regionInfo.getRegionNameAsString());
          }
        
          // instantiate the location
          location = new HRegionLocation(regionInfo,
              new HServerAddress(serverAddress));
          cacheLocation(tableName, location);
          return location;
        } catch (TableNotFoundException e) {
          // if we got this error, probably means the table just plain doesn't
          // exist. rethrow the error immediately. this should always be coming
          // from the HTable constructor.
          throw e;
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = RemoteExceptionHandler.decodeRemoteException(
                (RemoteException) e);
          }
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("locateRegionInMeta attempt " + tries + " of " +
                this.numRetries + " failed; retrying after sleep of " +
                getPauseTime(tries), e);
            }
            relocateRegion(parentTable, metaKey);
          } else {
            throw e;
          }
        }
      
        try{
          Thread.sleep(getPauseTime(tries));              
        } catch (InterruptedException e){
          // continue
        }
      }
    }

    /*
     * Search the cache for a location that fits our table and row key.
     * Return null if no suitable region is located. TODO: synchronization note
     * 
     * <p>TODO: This method during writing consumes 15% of CPU doing lookup
     * into the Soft Reference SortedMap.  Improve.
     * 
     * @param tableName
     * @param row
     * @return Null or region location found in cache.
     */
    private HRegionLocation getCachedLocation(final byte [] tableName,
        final byte [] row) {
      SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (tableLocations.isEmpty()) {
        return null;
      }

      HRegionLocation rl = tableLocations.get(row);
      if (rl != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cache hit for row <" +
            Bytes.toString(row) +
            "> in tableName " + Bytes.toString(tableName) +
            ": location server " + rl.getServerAddress() +
            ", location region name " +
            rl.getRegionInfo().getRegionNameAsString());
        }
        return rl;
      }

      // Cut the cache so that we only get the part that could contain
      // regions that match our key
      SoftValueSortedMap<byte[], HRegionLocation> matchingRegions =
        tableLocations.headMap(row);

      // if that portion of the map is empty, then we're done. otherwise,
      // we need to examine the cached location to verify that it is
      // a match by end key as well.
      if (!matchingRegions.isEmpty()) {
        HRegionLocation possibleRegion =
          matchingRegions.get(matchingRegions.lastKey());

        // there is a possibility that the reference was garbage collected
        // in the instant since we checked isEmpty().
        if (possibleRegion != null) {
          byte[] endKey = possibleRegion.getRegionInfo().getEndKey();

          // make sure that the end key is greater than the row we're looking
          // for, otherwise the row actually belongs in the next region, not
          // this one. the exception case is when the endkey is EMPTY_START_ROW,
          // signifying that the region we're checking is actually the last
          // region in the table.
          if (HStoreKey.equalsTwoRowKeys(endKey, HConstants.EMPTY_END_ROW) ||
              HStoreKey.getComparator(tableName).compareRows(endKey, row) > 0) {
            return possibleRegion;
          }
        }
      }

      // Passed all the way through, so we got nothin - complete cache miss
      return null;
    }

    /*
     * Delete a cached location, if it satisfies the table name and row
     * requirements.
     */
    private void deleteCachedLocation(final byte [] tableName,
        final byte [] row) {
      SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (!tableLocations.isEmpty()) {
        // cut the cache so that we only get the part that could contain
        // regions that match our key
        SoftValueSortedMap<byte [], HRegionLocation> matchingRegions =
          tableLocations.headMap(row);

        // if that portion of the map is empty, then we're done. otherwise,
        // we need to examine the cached location to verify that it is 
        // a match by end key as well.
        if (!matchingRegions.isEmpty()) {
          HRegionLocation possibleRegion =
            matchingRegions.get(matchingRegions.lastKey());
          byte [] endKey = possibleRegion.getRegionInfo().getEndKey();

          // by nature of the map, we know that the start key has to be < 
          // otherwise it wouldn't be in the headMap. 
          if (HStoreKey.getComparator(tableName).compareRows(endKey, row) <= 0) {
            // delete any matching entry
            HRegionLocation rl =
              tableLocations.remove(matchingRegions.lastKey());
            if (rl != null && LOG.isDebugEnabled()) {
              LOG.debug("Removed " + rl.getRegionInfo().getRegionNameAsString() +
                " for tableName=" + Bytes.toString(tableName) + " from cache " +
                "because of " + Bytes.toString(row));
            }
          }
        }
      }
    }
    
    /*
     * @param tableName
     * @return Map of cached locations for passed <code>tableName</code>
     */
    private SoftValueSortedMap<byte [], HRegionLocation> getTableLocations(
        final byte [] tableName) {
      // find the map of cached locations for this table
      Integer key = Bytes.mapKey(tableName);
      SoftValueSortedMap<byte [], HRegionLocation> result = null;
      synchronized (this.cachedRegionLocations) {
        result = this.cachedRegionLocations.get(key);
        // if tableLocations for this table isn't built yet, make one
        if (result == null) {
          result = new SoftValueSortedMap<byte [], HRegionLocation>(
              Bytes.BYTES_COMPARATOR);
          this.cachedRegionLocations.put(key, result);
        }
      }
      return result;
    }

    /*
     * Put a newly discovered HRegionLocation into the cache.
     */
    private void cacheLocation(final byte [] tableName,
        final HRegionLocation location) {
      byte [] startKey = location.getRegionInfo().getStartKey();
      SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);
      tableLocations.put(startKey, location);
    }
    
    public HRegionInterface getHRegionConnection(HServerAddress regionServer) 
    throws IOException {
      getMaster();
      HRegionInterface server;
      synchronized (this.servers) {
        // See if we already have a connection
        server = this.servers.get(regionServer.toString());
        if (server == null) { // Get a connection
          try {
            server = (HRegionInterface)HBaseRPC.waitForProxy(
                serverInterfaceClass, HBaseRPCProtocolVersion.versionID,
                regionServer.getInetSocketAddress(), this.conf, 
                this.maxRPCAttempts);
          } catch (RemoteException e) {
            throw RemoteExceptionHandler.decodeRemoteException(e);
          }
          this.servers.put(regionServer.toString(), server);
        }
      }
      return server;
    }

    private synchronized ZooKeeperWrapper getZooKeeperWrapper() throws IOException {
      if (zooKeeperWrapper == null) {
        zooKeeperWrapper = new ZooKeeperWrapper(conf);
      }
      return zooKeeperWrapper;
    }

    /*
     * Repeatedly try to find the root region by asking the master for where it is
     * @return HRegionLocation for root region if found
     * @throws NoServerForRegionException - if the root region can not be
     * located after retrying
     * @throws IOException 
     */
    private HRegionLocation locateRootRegion()
    throws IOException {
      getMaster();

      // We lazily instantiate the ZooKeeper object because we don't want to
      // make the constructor have to throw IOException or handle it itself.
      ZooKeeperWrapper zooKeeperWrapper = getZooKeeperWrapper();

      HServerAddress rootRegionAddress = null;
      for (int tries = 0; tries < numRetries; tries++) {
        int localTimeouts = 0;
        // ask the master which server has the root region
        while (rootRegionAddress == null && localTimeouts < numRetries) {
          // Don't read root region until we're out of safe mode so we know
          // that the meta regions have been assigned.
          boolean outOfSafeMode = zooKeeperWrapper.checkOutOfSafeMode();
          if (outOfSafeMode) {
            rootRegionAddress = zooKeeperWrapper.readRootRegionLocation();
          }
          if (rootRegionAddress == null) {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Sleeping " + getPauseTime(tries) +
                  "ms, waiting for root region.");
              }
              Thread.sleep(getPauseTime(tries));
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
          server.getRegionInfo(HRegionInfo.ROOT_REGIONINFO.getRegionName());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found ROOT at " + rootRegionAddress);
          }
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
            Thread.sleep(getPauseTime(tries));
            if (LOG.isDebugEnabled()) {
              LOG.debug("Wake. Retry finding root region.");
            }
          } catch (InterruptedException iex) {
            // continue
          }
        }
        
        rootRegionAddress = null;
      }
      
      // if the address is null by this point, then the retries have failed,
      // and we're sort of sunk
      if (rootRegionAddress == null) {
        throw new NoServerForRegionException(
          "unable to locate root region server");
      }
      
      // return the region location
      return new HRegionLocation(
        HRegionInfo.ROOT_REGIONINFO, rootRegionAddress);
    }

    public <T> T getRegionServerWithRetries(ServerCallable<T> callable) 
    throws IOException, RuntimeException {
      getMaster();
      List<Throwable> exceptions = new ArrayList<Throwable>();
      for(int tries = 0; tries < numRetries; tries++) {
        try {
          callable.instantiateServer(tries != 0);
          return callable.call();
        } catch (Throwable t) {
          if (t instanceof UndeclaredThrowableException) {
            t = t.getCause();
          }
          if (t instanceof RemoteException) {
            t = RemoteExceptionHandler.decodeRemoteException((RemoteException)t);
          }
          if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
          }
          exceptions.add(t);
          if (tries == numRetries - 1) {
            throw new RetriesExhaustedException(callable.getServerName(),
                callable.getRegionName(), callable.getRow(), tries, exceptions);
          }
        }
        try {
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          // continue
        }
      }
      return null;    
    }
    
    public <T> T getRegionServerForWithoutRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
      getMaster();
      try {
        callable.instantiateServer(false);
        return callable.call();
      } catch (Throwable t) {
        if (t instanceof UndeclaredThrowableException) {
          t = t.getCause();
        }
        if (t instanceof RemoteException) {
          t = RemoteExceptionHandler.decodeRemoteException((RemoteException) t);
        }
        if (t instanceof DoNotRetryIOException) {
          throw (DoNotRetryIOException) t;
        }
      }
      return null;    
    }

    private HRegionLocation
    getRegionLocationForRowWithRetries(byte[] tableName, byte[] rowKey, 
      boolean reload)
    throws IOException {
      boolean reloadFlag = reload;
      getMaster();
      List<Throwable> exceptions = new ArrayList<Throwable>();
      HRegionLocation location = null;
      int tries = 0;
      while (tries < numRetries) {
        try {
          location = getRegionLocation(tableName, rowKey, reloadFlag);
        } catch (Throwable t) {
          exceptions.add(t);
        }
        if (location != null) {
          break;
        }
        reloadFlag = true;
        tries++;
        try {
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          // continue
        }
      }
      if (location == null) {
        throw new RetriesExhaustedException("Some server",
          HConstants.EMPTY_BYTE_ARRAY, rowKey, tries, exceptions);
      }
      return location;
    }

    public void processBatchOfRows(ArrayList<BatchUpdate> list, byte[] tableName)
        throws IOException {
      if (list.isEmpty()) {
        return;
      }
      boolean retryOnlyOne = false;
      int tries = 0;
      Collections.sort(list);
      List<BatchUpdate> tempUpdates = new ArrayList<BatchUpdate>();
      HRegionLocation location =
        getRegionLocationForRowWithRetries(tableName, list.get(0).getRow(),
            false);
      byte [] currentRegion = location.getRegionInfo().getRegionName();
      byte [] region = currentRegion;
      boolean isLastRow = false;
      for (int i = 0; i < list.size() && tries < numRetries; i++) {
        BatchUpdate batchUpdate = list.get(i);
        tempUpdates.add(batchUpdate);
        isLastRow = (i + 1) == list.size();
        if (!isLastRow) {
          location = getRegionLocationForRowWithRetries(tableName,
            list.get(i+1).getRow(), false);
          region = location.getRegionInfo().getRegionName();
        }
        if (!Bytes.equals(currentRegion, region) || isLastRow || retryOnlyOne) {
          final BatchUpdate[] updates = tempUpdates.toArray(new BatchUpdate[0]);
          int index = getRegionServerWithRetries(new ServerCallable<Integer>(
              this, tableName, batchUpdate.getRow()) {
            public Integer call() throws IOException {
              int i = server.batchUpdates(location.getRegionInfo()
                  .getRegionName(), updates);
              return i;
            }
          });
          if (index != -1) {
            if (tries == numRetries - 1) {
              throw new RetriesExhaustedException("Some server",
                  currentRegion, batchUpdate.getRow(), 
                  tries, new ArrayList<Throwable>());
            }
            long sleepTime = getPauseTime(tries);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Reloading region " + Bytes.toString(currentRegion) +
                " location because regionserver didn't accept updates; " +
                "tries=" + tries +
                " of max=" + this.numRetries + ", waiting=" + sleepTime + "ms");
            }
            try {
              Thread.sleep(sleepTime);
              tries++;
            } catch (InterruptedException e) {
              // continue
            }
            i = i - updates.length + index;
            retryOnlyOne = true;
            location = getRegionLocationForRowWithRetries(tableName, 
              list.get(i + 1).getRow(), true);
            region = location.getRegionInfo().getRegionName();
          }
          else {
            retryOnlyOne = false;
          }
          currentRegion = region;
          tempUpdates.clear();
        }
      }
    }

    void close(boolean stopProxy) {
      if (master != null) {
        if (stopProxy) {
          HBaseRPC.stopProxy(master);
        }
        master = null;
        masterChecked = false;
      }
      if (stopProxy) {
        synchronized (servers) {
          for (HRegionInterface i: servers.values()) {
            HBaseRPC.stopProxy(i);
          }
        }
      }
    }
  } 
}