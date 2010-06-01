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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A non-instantiable class that manages connections to multiple tables in
 * multiple HBase instances.
 *
 * Used by {@link HTable} and {@link HBaseAdmin}
 */
public class HConnectionManager implements HConstants {
  private static final Delete [] DELETE_ARRAY_TYPE = new Delete[]{};
  private static final Put [] PUT_ARRAY_TYPE = new Put[]{};

  // Register a shutdown hook, one that cleans up RPC and closes zk sessions.
  static {
    Runtime.getRuntime().addShutdownHook(new Thread("HCM.shutdownHook") {
      @Override
      public void run() {
        HConnectionManager.deleteAllConnections(true);
      }
    });
  }

  /*
   * Not instantiable.
   */
  protected HConnectionManager() {
    super();
  }

  private static final int MAX_CACHED_HBASE_INSTANCES=31;
  // A LRU Map of master HBaseConfiguration -> connection information for that
  // instance. The objects it contains are mutable and hence require
  // synchronized access to them.  We set instances to 31.  The zk default max
  // connections is 30 so should run into zk issues before hit this value of 31.
  private static
  final Map<Integer, TableServers> HBASE_INSTANCES =
    new LinkedHashMap<Integer, TableServers>
      ((int) (MAX_CACHED_HBASE_INSTANCES/0.75F)+1, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Integer, TableServers> eldest) {
        return size() > MAX_CACHED_HBASE_INSTANCES;
      }
  };

  private static final Map<String, ClientZKWatcher> ZK_WRAPPERS =
    new HashMap<String, ClientZKWatcher>();

  /**
   * Get the connection object for the instance specified by the configuration
   * If no current connection exists, create a new connection for that instance
   * @param conf configuration
   * @return HConnection object for the instance specified by the configuration
   */
  public static HConnection getConnection(Configuration conf) {
    TableServers connection;
    Integer key = HBaseConfiguration.hashCode(conf);
    synchronized (HBASE_INSTANCES) {
      connection = HBASE_INSTANCES.get(key);
      if (connection == null) {
        connection = new TableServers(conf);
        HBASE_INSTANCES.put(key, connection);
      }
    }
    return connection;
  }

  /**
   * Delete connection information for the instance specified by configuration
   * @param conf configuration
   * @param stopProxy stop the proxy as well
   */
  public static void deleteConnectionInfo(Configuration conf,
      boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      Integer key = HBaseConfiguration.hashCode(conf);
      TableServers t = HBASE_INSTANCES.remove(key);
      if (t != null) {
        t.close(stopProxy);
      }
    }
  }

  /**
   * Delete information for all connections.
   * @param stopProxy stop the proxy as well
   */
  public static void deleteAllConnections(boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      for (TableServers t : HBASE_INSTANCES.values()) {
        if (t != null) {
          t.close(stopProxy);
        }
      }
    }
    synchronized (ZK_WRAPPERS) {
      for (ClientZKWatcher watch : ZK_WRAPPERS.values()) {
        watch.resetZooKeeper();
      }
    }
  }

  /**
   * Get a watcher of a zookeeper connection for a given quorum address.
   * If the connection isn't established, a new one is created.
   * This acts like a multiton.
   * @param conf configuration
   * @return ZKW watcher
   * @throws IOException if a remote or network exception occurs
   */
  public static synchronized ClientZKWatcher getClientZooKeeperWatcher(
      Configuration conf) throws IOException {
    if (!ZK_WRAPPERS.containsKey(
        ZooKeeperWrapper.getZookeeperClusterKey(conf))) {
      ZK_WRAPPERS.put(ZooKeeperWrapper.getZookeeperClusterKey(conf),
          new ClientZKWatcher(conf));
    }
    return ZK_WRAPPERS.get(ZooKeeperWrapper.getZookeeperClusterKey(conf));
  }

  /**
   * This class is responsible to handle connection and reconnection
   * to a zookeeper quorum.
   *
   */
  public static class ClientZKWatcher implements Watcher {

    static final Log LOG = LogFactory.getLog(ClientZKWatcher.class);
    private ZooKeeperWrapper zooKeeperWrapper;
    private Configuration conf;

    /**
     * Takes a configuration to pass it to ZKW but won't instanciate it
     * @param conf configuration
     */
    public ClientZKWatcher(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Called by ZooKeeper when an event occurs on our connection. We use this to
     * detect our session expiring. When our session expires, we have lost our
     * connection to ZooKeeper. Our handle is dead, and we need to recreate it.
     *
     * See http://hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers.html#ch_zkSessions
     * for more information.
     *
     * @param event WatchedEvent witnessed by ZooKeeper.
     */
    public void process(WatchedEvent event) {
      KeeperState state = event.getState();
      if(!state.equals(KeeperState.SyncConnected)) {
        LOG.debug("Got ZooKeeper event, state: " + state + ", type: "
            + event.getType() + ", path: " + event.getPath());
      }
      if (state == KeeperState.Expired) {
        resetZooKeeper();
      }
    }

    /**
     * Get this watcher's ZKW, instanciate it if necessary.
     * @return ZKW
     * @throws java.io.IOException if a remote or network exception occurs
     */
    public synchronized ZooKeeperWrapper getZooKeeperWrapper() throws IOException {
      if(zooKeeperWrapper == null) {
        zooKeeperWrapper = new ZooKeeperWrapper(conf, this);
      }
      return zooKeeperWrapper;
    }

    /**
     * Clear this connection to zookeeper.
     */
    private synchronized void resetZooKeeper() {
      if (zooKeeperWrapper != null) {
        zooKeeperWrapper.close();
        zooKeeperWrapper = null;
      }
    }
  }

  /* Encapsulates finding the servers for an HBase instance */
  private static class TableServers implements ServerConnection, HConstants {
    static final Log LOG = LogFactory.getLog(TableServers.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long pause;
    private final int numRetries;
    private final int maxRPCAttempts;
    private final long rpcTimeout;

    private final Object masterLock = new Object();
    private volatile boolean closed;
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;

    private final Object rootRegionLock = new Object();
    private final Object metaRegionLock = new Object();
    private final Object userRegionLock = new Object();

    private volatile Configuration conf;

    // Known region HServerAddress.toString() -> HRegionInterface
    private final Map<String, HRegionInterface> servers =
      new ConcurrentHashMap<String, HRegionInterface>();

    // Used by master and region servers during safe mode only
    private volatile HRegionLocation rootRegionLocation;

    private final Map<Integer, SoftValueSortedMap<byte [], HRegionLocation>>
      cachedRegionLocations =
        new HashMap<Integer, SoftValueSortedMap<byte [], HRegionLocation>>();

    /**
     * constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public TableServers(Configuration conf) {
      this.conf = conf;

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

      this.pause = conf.getLong("hbase.client.pause", 1000);
      this.numRetries = conf.getInt("hbase.client.retries.number", 10);
      this.maxRPCAttempts = conf.getInt("hbase.client.rpc.maxattempts", 1);
      this.rpcTimeout = conf.getLong(HBASE_REGIONSERVER_LEASE_PERIOD_KEY, DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);

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
      ZooKeeperWrapper zk;
      try {
        zk = getZooKeeperWrapper();
      } catch (IOException e) {
        throw new MasterNotRunningException(e);
      }

      HServerAddress masterLocation = null;
      synchronized (this.masterLock) {
        for (int tries = 0;
          !this.closed &&
          !this.masterChecked && this.master == null &&
          tries < numRetries;
        tries++) {

          try {
            masterLocation = zk.readMasterAddressOrThrow();

            HMasterInterface tryMaster = (HMasterInterface)HBaseRPC.getProxy(
                HMasterInterface.class, HBaseRPCProtocolVersion.versionID,
                masterLocation.getInetSocketAddress(), this.conf);

            if (tryMaster.isMasterRunning()) {
              this.master = tryMaster;
              this.masterLock.notifyAll();
              break;
            }

          } catch (IOException e) {
            if (tries == numRetries - 1) {
              // This was our last chance - don't bother sleeping
              LOG.info("getMaster attempt " + tries + " of " + this.numRetries +
                " failed; no more retrying.", e);
              break;
            }
            LOG.info("getMaster attempt " + tries + " of " + this.numRetries +
              " failed; retrying after sleep of " +
              getPauseTime(tries), e);
          }

          // Cannot connect to master or it is not running. Sleep & retry
          try {
            this.masterLock.wait(getPauseTime(tries));
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
        for (HTableDescriptor table : tables) {
          if (Bytes.equals(table.getName(), tableName)) {
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
      return reload? relocateRegion(name, row): locateRegion(name, row);
    }

    public HTableDescriptor[] listTables() throws IOException {
      getMaster();
      final TreeSet<HTableDescriptor> uniqueTables =
        new TreeSet<HTableDescriptor>();
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        public boolean processRow(Result result) throws IOException {
          try {
            byte[] value = result.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
            HRegionInfo info = null;
            if (value != null) {
              info = Writables.getHRegionInfo(value);
            }
            // Only examine the rows where the startKey is zero length
            if (info != null && info.getStartKey().length == 0) {
              uniqueTables.add(info.getTableDesc());
            }
            return true;
          } catch (RuntimeException e) {
            LOG.error("Result=" + result);
            throw e;
          }
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

    public boolean isTableAvailable(final byte[] tableName) throws IOException {
      final AtomicBoolean available = new AtomicBoolean(true);
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result row) throws IOException {
          byte[] value = row.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
          HRegionInfo info = Writables.getHRegionInfoOrNull(value);
          if (info != null) {
            if (Bytes.equals(tableName, info.getTableDesc().getName())) {
              value = row.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
              if (value == null) {
                available.set(false);
                return false;
              }
            }
          }
          return true;
        }
      };
      MetaScanner.metaScan(conf, visitor);
      return available.get();
    }

    /*
     * If online == true
     *   Returns true if all regions are online
     *   Returns false in any other case
     * If online == false
     *   Returns true if all regions are offline
     *   Returns false in any other case
     */
    private boolean testTableOnlineState(byte[] tableName, boolean online)
    throws IOException {
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
        HRegionInfo.createRegionName(tableName, null, HConstants.ZEROES, false);
      byte[] endKey;
      HRegionInfo currentRegion;
      Scan scan = new Scan(startKey);
      scan.addColumn(CATALOG_FAMILY, REGIONINFO_QUALIFIER);
      int rows = this.conf.getInt("hbase.meta.scanner.caching", 100);
      scan.setCaching(rows);
      ScannerCallable s = new ScannerCallable(this,
          (Bytes.equals(tableName, HConstants.META_TABLE_NAME) ?
              HConstants.ROOT_TABLE_NAME : HConstants.META_TABLE_NAME), scan);
      try {
        // Open scanner
        getRegionServerWithRetries(s);
        do {
          currentRegion = s.getHRegionInfo();
          Result r;
          Result [] rrs;
          while ((rrs = getRegionServerWithRetries(s)) != null && rrs.length > 0) {
            r = rrs[0];
            byte [] value = r.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
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
          endKey = currentRegion.getEndKey();
        } while (!(endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)));
      } finally {
        s.setClose();
        // Doing below will call 'next' again and this will close the scanner
        // Without it we leave scanners open.
        getRegionServerWithRetries(s);
      }
      LOG.debug("Rowscanned=" + rowsScanned + ", rowsOffline=" + rowsOffline);
      boolean onOffLine = online? rowsOffline == 0: rowsOffline == rowsScanned;
      return rowsScanned > 0 && onOffLine;
    }

    private static class HTableDescriptorFinder
    implements MetaScanner.MetaScannerVisitor {
        byte[] tableName;
        HTableDescriptor result;
        protected HTableDescriptorFinder(byte[] tableName) {
          this.tableName = tableName;
        }
        public boolean processRow(Result rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfo(
              rowResult.getValue(CATALOG_FAMILY, REGIONINFO_QUALIFIER));
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
        return HTableDescriptor.META_TABLEDESC;
      }
      HTableDescriptorFinder finder = new HTableDescriptorFinder(tableName);
      MetaScanner.metaScan(conf, finder, tableName);
      HTableDescriptor result = finder.getResult();
      if (result == null) {
        throw new TableNotFoundException(Bytes.toString(tableName));
      }
      return result;
    }

    public HRegionLocation locateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
      return locateRegion(tableName, row, true);
    }

    public HRegionLocation relocateRegion(final byte [] tableName,
        final byte [] row)
    throws IOException{
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
            this.rootRegionLocation = locateRootRegion();
          }
          return this.rootRegionLocation;
        }
      } else if (Bytes.equals(tableName, META_TABLE_NAME)) {
        return locateRegionInMeta(ROOT_TABLE_NAME, tableName, row, useCache,
                                  metaRegionLock);
      } else {
        // Region not in the cache - have to go to the meta RS
        return locateRegionInMeta(META_TABLE_NAME, tableName, row, useCache,
                                  userRegionLock);
      }
    }

    /*
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
    @SuppressWarnings({"ConstantConditions"})
    private HRegionLocation locateRegionInMeta(final byte [] parentTable,
      final byte [] tableName, final byte [] row, boolean useCache,
      Object regionLockObject)
    throws IOException {
      HRegionLocation location;
      // If we are supposed to be using the cache, look in the cache to see if
      // we already have the region.
      if (useCache) {
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      }

      // build the key of the meta region we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise region names.
      byte [] metaKey = HRegionInfo.createRegionName(tableName, row,
        HConstants.NINES, false);
      for (int tries = 0; true; tries++) {
        if (tries >= numRetries) {
          throw new NoServerForRegionException("Unable to find region for "
            + Bytes.toStringBinary(row) + " after " + numRetries + " tries.");
        }

        try {
          // locate the root or meta region
          HRegionLocation metaLocation = locateRegion(parentTable, metaKey);
          HRegionInterface server =
            getHRegionConnection(metaLocation.getServerAddress());

          Result regionInfoRow = null;
          // This block guards against two threads trying to load the meta
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          synchronized (regionLockObject) {
            // Check the cache again for a hit in case some other thread made the
            // same query while we were waiting on the lock. If not supposed to
            // be using the cache, delete any existing cached location so it won't
            // interfere.
            if (useCache) {
              location = getCachedLocation(tableName, row);
              if (location != null) {
                return location;
              }
            } else {
              deleteCachedLocation(tableName, row);
            }

          // Query the root or meta region for the location of the meta region
            regionInfoRow = server.getClosestRowBefore(
            metaLocation.getRegionInfo().getRegionName(), metaKey,
            HConstants.CATALOG_FAMILY);
          }
          if (regionInfoRow == null) {
            throw new TableNotFoundException(Bytes.toString(tableName));
          }
          byte [] value = regionInfoRow.getValue(CATALOG_FAMILY,
              REGIONINFO_QUALIFIER);
          if (value == null || value.length == 0) {
            throw new IOException("HRegionInfo was null or empty in " +
              Bytes.toString(parentTable) + ", row=" + regionInfoRow);
          }
          // convert the row result into the HRegionLocation we need!
          HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
              value, new HRegionInfo());
          // possible we got a region of a different table...
          if (!Bytes.equals(regionInfo.getTableDesc().getName(), tableName)) {
            throw new TableNotFoundException(
              "Table '" + Bytes.toString(tableName) + "' was not found.");
          }
          if (regionInfo.isOffline()) {
            throw new RegionOfflineException("region offline: " +
              regionInfo.getRegionNameAsString());
          }

          value = regionInfoRow.getValue(CATALOG_FAMILY, SERVER_QUALIFIER);
          String serverAddress = "";
          if(value != null) {
            serverAddress = Bytes.toString(value);
          }
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
                getPauseTime(tries) + " because: " + e.getMessage());
            }
          } else {
            throw e;
          }
          // Only relocate the parent region if necessary
          if(!(e instanceof RegionOfflineException ||
              e instanceof NoServerForRegionException)) {
            relocateRegion(parentTable, metaKey);
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
            Bytes.toStringBinary(row) +
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
          if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
              KeyValue.getRowComparator(tableName).compareRows(endKey, 0, endKey.length,
                  row, 0, row.length) > 0) {
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
      synchronized (this.cachedRegionLocations) {
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
            if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
                KeyValue.getRowComparator(tableName).compareRows(endKey, 0, endKey.length,
                    row, 0, row.length) > 0) {
              // delete any matching entry
              HRegionLocation rl =
                  tableLocations.remove(matchingRegions.lastKey());
              if (rl != null && LOG.isDebugEnabled()) {
                LOG.debug("Removed " + rl.getRegionInfo().getRegionNameAsString() +
                    " for tableName=" + Bytes.toString(tableName) + " from cache " +
                    "because of " + Bytes.toStringBinary(row));
              }
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
      SoftValueSortedMap<byte [], HRegionLocation> result;
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

    /**
     * Allows flushing the region cache.
     */
    public void clearRegionCache() {
     cachedRegionLocations.clear();
    }

    /*
     * Put a newly discovered HRegionLocation into the cache.
     */
    private void cacheLocation(final byte [] tableName,
        final HRegionLocation location) {
      byte [] startKey = location.getRegionInfo().getStartKey();
      SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
        getTableLocations(tableName);
      if (tableLocations.put(startKey, location) == null) {
        LOG.debug("Cached location for " +
            location.getRegionInfo().getRegionNameAsString() +
            " is " + location.getServerAddress());
      }
    }

    public HRegionInterface getHRegionConnection(
        HServerAddress regionServer, boolean getMaster)
    throws IOException {
      if (getMaster) {
        getMaster();
      }
      HRegionInterface server;
      synchronized (this.servers) {
        // See if we already have a connection
        server = this.servers.get(regionServer.toString());
        if (server == null) { // Get a connection
          try {
            server = (HRegionInterface)HBaseRPC.waitForProxy(
                serverInterfaceClass, HBaseRPCProtocolVersion.versionID,
                regionServer.getInetSocketAddress(), this.conf,
                this.maxRPCAttempts, this.rpcTimeout);
          } catch (RemoteException e) {
            throw RemoteExceptionHandler.decodeRemoteException(e);
          }
          this.servers.put(regionServer.toString(), server);
        }
      }
      return server;
    }

    public HRegionInterface getHRegionConnection(
        HServerAddress regionServer)
    throws IOException {
      return getHRegionConnection(regionServer, false);
    }

    public synchronized ZooKeeperWrapper getZooKeeperWrapper()
        throws IOException {
      return HConnectionManager.getClientZooKeeperWatcher(conf)
          .getZooKeeperWrapper();
    }

    /*
     * Repeatedly try to find the root region in ZK
     * @return HRegionLocation for root region if found
     * @throws NoServerForRegionException - if the root region can not be
     * located after retrying
     * @throws IOException
     */
    private HRegionLocation locateRootRegion()
    throws IOException {

      // We lazily instantiate the ZooKeeper object because we don't want to
      // make the constructor have to throw IOException or handle it itself.
      ZooKeeperWrapper zk = getZooKeeperWrapper();

      HServerAddress rootRegionAddress = null;
      for (int tries = 0; tries < numRetries; tries++) {
        int localTimeouts = 0;
        // ask the master which server has the root region
        while (rootRegionAddress == null && localTimeouts < numRetries) {
          // Don't read root region until we're out of safe mode so we know
          // that the meta regions have been assigned.
          rootRegionAddress = zk.readRootRegionLocation();
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

        try {
          // Get a connection to the region server
          HRegionInterface server = getHRegionConnection(rootRegionAddress);
          // if this works, then we're good, and we have an acceptable address,
          // so we can stop doing retries and return the result.
          server.getRegionInfo(HRegionInfo.ROOT_REGIONINFO.getRegionName());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found ROOT at " + rootRegionAddress);
          }
          break;
        } catch (Throwable t) {
          t = translateException(t);

          if (tries == numRetries - 1) {
            throw new NoServerForRegionException("Timed out trying to locate "+
                "root region because: " + t.getMessage());
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

    @SuppressWarnings({"ConstantConditions"})
    public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
    throws IOException, RuntimeException {
      List<Throwable> exceptions = new ArrayList<Throwable>();
      for(int tries = 0; tries < numRetries; tries++) {
        try {
          callable.instantiateServer(tries != 0);
          return callable.call();
        } catch (Throwable t) {
          t = translateException(t);
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

    public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
      try {
        callable.instantiateServer(false);
        return callable.call();
      } catch (Throwable t) {
        Throwable t2 = translateException(t);
        if (t2 instanceof IOException) {
          throw (IOException)t2;
        } else {
          throw new RuntimeException(t2);
        }
      }
    }

    @SuppressWarnings({"ConstantConditions"})
    private HRegionLocation
      getRegionLocationForRowWithRetries(byte[] tableName, byte[] rowKey,
        boolean reload)
    throws IOException {
      boolean reloadFlag = reload;
      List<Throwable> exceptions = new ArrayList<Throwable>();
      HRegionLocation location = null;
      int tries = 0;
      for (; tries < numRetries;) {
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
        throw new RetriesExhaustedException(" -- nothing found, no 'location' returned," +
          " tableName=" + Bytes.toString(tableName) +
          ", reload=" + reload + " --",
          HConstants.EMPTY_BYTE_ARRAY, rowKey, tries, exceptions);
      }
      return location;
    }

    /*
     * Helper class for batch updates.
     * Holds code shared doing batch puts and batch deletes.
     */
    private abstract class Batch {
      final HConnection c;

      private Batch(final HConnection c) {
        this.c = c;
      }

      /**
       * This is the method subclasses must implement.
       * @param currentList current list of rows
       * @param tableName table we are processing
       * @param row row
       * @return Count of items processed or -1 if all.
       * @throws IOException if a remote or network exception occurs
       * @throws RuntimeException other undefined exception
       */
      abstract int doCall(final List<? extends Row> currentList,
        final byte [] row, final byte [] tableName)
      throws IOException, RuntimeException;

      /**
       * Process the passed <code>list</code>.
       * @param list list of rows to process
       * @param tableName table we are processing
       * @return Count of how many added or -1 if all added.
       * @throws IOException if a remote or network exception occurs
       */
      int process(final List<? extends Row> list, final byte[] tableName)
      throws IOException {
        byte [] region = getRegionName(tableName, list.get(0).getRow(), false);
        byte [] currentRegion = region;
        boolean isLastRow;
        boolean retryOnlyOne = false;
        List<Row> currentList = new ArrayList<Row>();
        int i, tries;
        for (i = 0, tries = 0; i < list.size() && tries < numRetries; i++) {
          Row row = list.get(i);
          currentList.add(row);
          // If the next record goes to a new region, then we are to clear
          // currentList now during this cycle.
          isLastRow = (i + 1) == list.size();
          if (!isLastRow) {
            region = getRegionName(tableName, list.get(i + 1).getRow(), false);
          }
          if (!Bytes.equals(currentRegion, region) || isLastRow || retryOnlyOne) {
            int index = doCall(currentList, row.getRow(), tableName);
            // index is == -1 if all processed successfully, else its index
            // of last record successfully processed.
            if (index != -1) {
              if (tries == numRetries - 1) {
                throw new RetriesExhaustedException("Some server, retryOnlyOne=" +
                  retryOnlyOne + ", index=" + index + ", islastrow=" + isLastRow +
                  ", tries=" + tries + ", numtries=" + numRetries + ", i=" + i +
                  ", listsize=" + list.size() + ", region=" +
                  Bytes.toStringBinary(region), currentRegion, row.getRow(),
                  tries, new ArrayList<Throwable>());
              }
              tries = doBatchPause(currentRegion, tries);
              i = i - currentList.size() + index;
              retryOnlyOne = true;
              // Reload location.
              region = getRegionName(tableName, list.get(i + 1).getRow(), true);
            } else {
              // Reset these flags/counters on successful batch Put
              retryOnlyOne = false;
              tries = 0;
            }
            currentRegion = region;
            currentList.clear();
          }
        }
        return i;
      }

      /*
       * @param t
       * @param r
       * @param re
       * @return Region name that holds passed row <code>r</code>
       * @throws IOException
       */
      private byte [] getRegionName(final byte [] t, final byte [] r,
        final boolean re)
      throws IOException {
        HRegionLocation location = getRegionLocationForRowWithRetries(t, r, re);
        return location.getRegionInfo().getRegionName();
      }

      /*
       * Do pause processing before retrying...
       * @param currentRegion
       * @param tries
       * @return New value for tries.
       */
      private int doBatchPause(final byte [] currentRegion, final int tries) {
        int localTries = tries;
        long sleepTime = getPauseTime(tries);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reloading region " + Bytes.toStringBinary(currentRegion) +
            " location because regionserver didn't accept updates; tries=" +
            tries + " of max=" + numRetries + ", waiting=" + sleepTime + "ms");
        }
        try {
          Thread.sleep(sleepTime);
          localTries++;
        } catch (InterruptedException e) {
          // continue
        }
        return localTries;
      }
    }

    public int processBatchOfRows(final ArrayList<Put> list,
      final byte[] tableName)
    throws IOException {
      if (list.isEmpty()) return 0;
      if (list.size() > 1) Collections.sort(list);
      Batch b = new Batch(this) {
        @Override
        int doCall(final List<? extends Row> currentList, final byte [] row,
          final byte [] tableName)
        throws IOException, RuntimeException {
          final Put [] puts = currentList.toArray(PUT_ARRAY_TYPE);
          return getRegionServerWithRetries(new ServerCallable<Integer>(this.c,
              tableName, row) {
            public Integer call() throws IOException {
              return server.put(location.getRegionInfo().getRegionName(), puts);
            }
          });
        }
      };
      return b.process(list, tableName);
    }

    public int processBatchOfDeletes(final List<Delete> list,
      final byte[] tableName)
    throws IOException {
      if (list.isEmpty()) return 0;
      if (list.size() > 1) Collections.sort(list);
      Batch b = new Batch(this) {
        @Override
        int doCall(final List<? extends Row> currentList, final byte [] row,
          final byte [] tableName)
        throws IOException, RuntimeException {
          final Delete [] deletes = currentList.toArray(DELETE_ARRAY_TYPE);
          return getRegionServerWithRetries(new ServerCallable<Integer>(this.c,
                tableName, row) {
              public Integer call() throws IOException {
                return server.delete(location.getRegionInfo().getRegionName(),
                  deletes);
              }
            });
          }
        };
        return b.process(list, tableName);
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
        for (HRegionInterface i: servers.values()) {
          HBaseRPC.stopProxy(i);
        }
      }
    }

    /**
     * Process a batch of Puts on the given executor service.
     *
     * @param list the puts to make - successful puts will be removed.
     * @param pool thread pool to execute requests on
     *
     * In the case of an exception, we take different actions depending on the
     * situation:
     *  - If the exception is a DoNotRetryException, we rethrow it and leave the
     *    'list' parameter in an indeterminate state.
     *  - If the 'list' parameter is a singleton, we directly throw the specific
     *    exception for that put.
     *  - Otherwise, we throw a generic exception indicating that an error occurred.
     *    The 'list' parameter is mutated to contain those puts that did not succeed.
     */
    public void processBatchOfPuts(List<Put> list,
                                   final byte[] tableName, ExecutorService pool) throws IOException {
      boolean singletonList = list.size() == 1;
      Throwable singleRowCause = null;
      for ( int tries = 0 ; tries < numRetries && !list.isEmpty(); ++tries) {
        Collections.sort(list);
        Map<HServerAddress, MultiPut> regionPuts =
            new HashMap<HServerAddress, MultiPut>();
        // step 1:
        //  break up into regionserver-sized chunks and build the data structs
        for ( Put put : list ) {
          byte [] row = put.getRow();

          HRegionLocation loc = locateRegion(tableName, row, true);
          HServerAddress address = loc.getServerAddress();
          byte [] regionName = loc.getRegionInfo().getRegionName();

          MultiPut mput = regionPuts.get(address);
          if (mput == null) {
            mput = new MultiPut(address);
            regionPuts.put(address, mput);
          }
          mput.add(regionName, put);
        }

        // step 2:
        //  make the requests
        // Discard the map, just use a list now, makes error recovery easier.
        List<MultiPut> multiPuts = new ArrayList<MultiPut>(regionPuts.values());

        List<Future<MultiPutResponse>> futures =
            new ArrayList<Future<MultiPutResponse>>(regionPuts.size());
        for ( MultiPut put : multiPuts ) {
          futures.add(pool.submit(createPutCallable(put.address,
              put,
              tableName)));
        }
        // RUN!
        List<Put> failed = new ArrayList<Put>();

        // step 3:
        //  collect the failures and tries from step 1.
        for (int i = 0; i < futures.size(); i++ ) {
          Future<MultiPutResponse> future = futures.get(i);
          MultiPut request = multiPuts.get(i);
          try {
            MultiPutResponse resp = future.get();

            // For each region
            for (Map.Entry<byte[], List<Put>> e : request.puts.entrySet()) {
              Integer result = resp.getAnswer(e.getKey());
              if (result == null) {
                // failed
                LOG.debug("Failed all for region: " +
                    Bytes.toStringBinary(e.getKey()) + ", removing from cache");
                failed.addAll(e.getValue());
              } else if (result >= 0) {
                // some failures
                List<Put> lst = e.getValue();
                failed.addAll(lst.subList(result, lst.size()));
                LOG.debug("Failed past " + result + " for region: " +
                    Bytes.toStringBinary(e.getKey()) + ", removing from cache");
              }
            }
          } catch (InterruptedException e) {
            // go into the failed list.
            LOG.debug("Failed all from " + request.address, e);
            failed.addAll(request.allPuts());
          } catch (ExecutionException e) {
            // all go into the failed list.
            LOG.debug("Failed all from " + request.address, e);
            failed.addAll(request.allPuts());

            // Just give up, leaving the batch put list in an untouched/semi-committed state
            if (e.getCause() instanceof DoNotRetryIOException) {
              throw (DoNotRetryIOException) e.getCause();
            }

            if (singletonList) {
              // be richer for reporting in a 1 row case.
              singleRowCause = e.getCause();
            }
          }
        }
        list.clear();
        if (!failed.isEmpty()) {
          for (Put failedPut: failed) {
            deleteCachedLocation(tableName, failedPut.getRow());
          }

          list.addAll(failed);

          long sleepTime = getPauseTime(tries);
          LOG.debug("processBatchOfPuts had some failures, sleeping for " + sleepTime +
              " ms!");
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException ignored) {
          }
        }
      }
      if (!list.isEmpty()) {
        if (singletonList && singleRowCause != null) {
          throw new IOException(singleRowCause);
        }

        // ran out of retries and didnt succeed everything!
        throw new RetriesExhaustedException("Still had " + list.size() + " puts left after retrying " +
            numRetries + " times.");
      }
    }


    private Callable<MultiPutResponse> createPutCallable(
        final HServerAddress address, final MultiPut puts,
        final byte [] tableName) {
      final HConnection connection = this;
      return new Callable<MultiPutResponse>() {
        public MultiPutResponse call() throws IOException {
          return getRegionServerWithoutRetries(
              new ServerCallable<MultiPutResponse>(connection, tableName, null) {
                public MultiPutResponse call() throws IOException {
                  MultiPutResponse resp = server.multiPut(puts);
                  resp.request = puts;
                  return resp;
                }
                @Override
                public void instantiateServer(boolean reload) throws IOException {
                  server = connection.getHRegionConnection(address);
                }
              }
          );
        }
      };
    }

    private Throwable translateException(Throwable t) throws IOException {
      if (t instanceof UndeclaredThrowableException) {
        t = t.getCause();
      }
      if (t instanceof RemoteException) {
        t = RemoteExceptionHandler.decodeRemoteException((RemoteException)t);
      }
      if (t instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException)t;
      }
      return t;
    }
  }
}
