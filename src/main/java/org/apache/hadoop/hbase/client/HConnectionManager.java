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

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

/**
 * A non-instantiable class that manages {@link HConnection}s.
 * This class has a static Map of {@link HConnection} instances keyed by
 * {@link Configuration}; all invocations of {@link #getConnection(Configuration)}
 * that pass the same {@link Configuration} instance will be returned the same
 * {@link  HConnection} instance (Adding properties to a Configuration
 * instance does not change its object identity).  Sharing {@link HConnection}
 * instances is usually what you want; all clients of the {@link HConnection}
 * instances share the HConnections' cache of Region locations rather than each
 * having to discover for itself the location of meta, root, etc.  It makes
 * sense for the likes of the pool of HTables class {@link HTablePool}, for
 * instance (If concerned that a single {@link HConnection} is insufficient
 * for sharing amongst clients in say an heavily-multithreaded environment,
 * in practise its not proven to be an issue.  Besides, {@link HConnection} is
 * implemented atop Hadoop RPC and as of this writing, Hadoop RPC does a
 * connection per cluster-member, exclusively).
 *
 * <p>But sharing connections
 * makes clean up of {@link HConnection} instances a little awkward.  Currently,
 * clients cleanup by calling
 * {@link #deleteConnection(Configuration, boolean)}.  This will shutdown the
 * zookeeper connection the HConnection was using and clean up all
 * HConnection resources as well as stopping proxies to servers out on the
 * cluster. Not running the cleanup will not end the world; it'll
 * just stall the closeup some and spew some zookeeper connection failed
 * messages into the log.  Running the cleanup on a {@link HConnection} that is
 * subsequently used by another will cause breakage so be careful running
 * cleanup.
 * <p>To create a {@link HConnection} that is not shared by others, you can
 * create a new {@link Configuration} instance, pass this new instance to
 * {@link #getConnection(Configuration)}, and then when done, close it up by
 * doing something like the following:
 * <pre>
 * {@code
 * Configuration newConfig = new Configuration(originalConf);
 * HConnection connection = HConnectionManager.getConnection(newConfig);
 * // Use the connection to your hearts' delight and then when done...
 * HConnectionManager.deleteConnection(newConfig, true);
 * }
 * </pre>
 * <p>Cleanup used to be done inside in a shutdown hook.  On startup we'd
 * register a shutdown hook that called {@link #deleteAllConnections(boolean)}
 * on its way out but the order in which shutdown hooks run is not defined so
 * were problematic for clients of HConnection that wanted to register their
 * own shutdown hooks so we removed ours though this shifts the onus for
 * cleanup to the client.
 */
@SuppressWarnings("serial")
public class HConnectionManager {
  static final int MAX_CACHED_HBASE_INSTANCES = 31;

  // A LRU Map of Configuration hashcode -> TableServers. We set instances to 31.
  // The zk default max connections to the ensemble from the one client is 30 so
  // should run into zk issues before hit this value of 31.
  private static final Map<Configuration, HConnectionImplementation> HBASE_INSTANCES =
    new LinkedHashMap<Configuration, HConnectionImplementation>
      ((int) (MAX_CACHED_HBASE_INSTANCES/0.75F)+1, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Configuration, HConnectionImplementation> eldest) {
        return size() > MAX_CACHED_HBASE_INSTANCES;
      }
  };

  /*
   * Non-instantiable.
   */
  protected HConnectionManager() {
    super();
  }

  /**
   * Get the connection that goes with the passed <code>conf</code>
   * configuration instance.
   * If no current connection exists, method creates a new connection for the
   * passed <code>conf</code> instance.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection getConnection(Configuration conf)
  throws ZooKeeperConnectionException {
    HConnectionImplementation connection;
    synchronized (HBASE_INSTANCES) {
      connection = HBASE_INSTANCES.get(conf);
      if (connection == null) {
        connection = new HConnectionImplementation(conf);
        HBASE_INSTANCES.put(conf, connection);
      }
    }
    return connection;
  }

  /**
   * Delete connection information for the instance specified by configuration.
   * This will close connection to the zookeeper ensemble and let go of all
   * resources.
   * @param conf configuration whose identity is used to find {@link HConnection}
   * instance.
   * @param stopProxy Shuts down all the proxy's put up to cluster members
   * including to cluster HMaster.  Calls {@link HBaseRPC#stopProxy(org.apache.hadoop.ipc.VersionedProtocol)}.
   */
  public static void deleteConnection(Configuration conf, boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      HConnectionImplementation t = HBASE_INSTANCES.remove(conf);
      if (t != null) {
        t.close(stopProxy);
      }
    }
  }

  /**
   * Delete information for all connections.
   * @param stopProxy stop the proxy as well
   * @throws IOException
   */
  public static void deleteAllConnections(boolean stopProxy) {
    synchronized (HBASE_INSTANCES) {
      for (HConnectionImplementation t : HBASE_INSTANCES.values()) {
        if (t != null) {
          t.close(stopProxy);
        }
      }
    }
  }

  /**
   * It is provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   * @return Number of cached regions for the table.
   * @throws ZooKeeperConnectionException
   */
  static int getCachedRegionCount(Configuration conf,
      byte[] tableName)
  throws ZooKeeperConnectionException {
    HConnectionImplementation connection = (HConnectionImplementation)getConnection(conf);
    return connection.getNumberOfCachedRegionLocations(tableName);
  }

  /**
   * It's provided for unit test cases which verify the behavior of region
   * location cache prefetch.
   * @return true if the region where the table and row reside is cached.
   * @throws ZooKeeperConnectionException
   */
  static boolean isRegionCached(Configuration conf,
      byte[] tableName, byte[] row) throws ZooKeeperConnectionException {
    HConnectionImplementation connection = (HConnectionImplementation)getConnection(conf);
    return connection.isRegionCached(tableName, row);
  }

  /* Encapsulates connection to zookeeper and regionservers.*/
  static class HConnectionImplementation implements HConnection {
    static final Log LOG = LogFactory.getLog(HConnectionImplementation.class);
    private final Class<? extends HRegionInterface> serverInterfaceClass;
    private final long pause;
    private final int numRetries;
    private final int maxRPCAttempts;
    private final int rpcTimeout;
    private final int prefetchRegionLimit;

    private final Object masterLock = new Object();
    private volatile boolean closed;
    private volatile HMasterInterface master;
    private volatile boolean masterChecked;
    // ZooKeeper reference
    private ZooKeeperWatcher zooKeeper;
    // ZooKeeper-based master address tracker
    private MasterAddressTracker masterAddressTracker;
    private RootRegionTracker rootRegionTracker;
    
    private final Object metaRegionLock = new Object();

    private final Object userRegionLock = new Object();

    private final Configuration conf;
    // Known region HServerAddress.toString() -> HRegionInterface

    private final Map<String, HRegionInterface> servers =
      new ConcurrentHashMap<String, HRegionInterface>();

    /**
     * Map of table to table {@link HRegionLocation}s.  The table key is made
     * by doing a {@link Bytes#mapKey(byte[])} of the table's name.
     */
    private final Map<Integer, SoftValueSortedMap<byte [], HRegionLocation>>
      cachedRegionLocations =
        new HashMap<Integer, SoftValueSortedMap<byte [], HRegionLocation>>();

    // region cache prefetch is enabled by default. this set contains all
    // tables whose region cache prefetch are disabled.
    private final Set<Integer> regionCachePrefetchDisabledTables =
      new CopyOnWriteArraySet<Integer>();

    /**
     * constructor
     * @param conf Configuration object
     */
    @SuppressWarnings("unchecked")
    public HConnectionImplementation(Configuration conf)
    throws ZooKeeperConnectionException {
      this.conf = conf;
      String serverClassName = conf.get(HConstants.REGION_SERVER_CLASS,
        HConstants.DEFAULT_REGION_SERVER_CLASS);
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
      this.rpcTimeout = conf.getInt(
          HConstants.HBASE_RPC_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

      this.prefetchRegionLimit = conf.getInt("hbase.client.prefetch.limit",
          10);

      setupZookeeperTrackers();

      this.master = null;
      this.masterChecked = false;
    }

    private synchronized void setupZookeeperTrackers()
        throws ZooKeeperConnectionException{
      // initialize zookeeper and master address manager
      this.zooKeeper = getZooKeeperWatcher();
      masterAddressTracker = new MasterAddressTracker(this.zooKeeper, this);
      zooKeeper.registerListener(masterAddressTracker);
      masterAddressTracker.start();

      this.rootRegionTracker = new RootRegionTracker(this.zooKeeper, this);
      this.rootRegionTracker.start();
    }

    private synchronized void resetZooKeeperTrackers()
        throws ZooKeeperConnectionException {
      LOG.info("Trying to reconnect to zookeeper");
      masterAddressTracker.stop();
      masterAddressTracker = null;
      rootRegionTracker.stop();
      rootRegionTracker = null;
      this.zooKeeper = null;
      setupZookeeperTrackers();
    }

    public Configuration getConfiguration() {
      return this.conf;
    }

    private long getPauseTime(int tries) {
      int ntries = tries;
      if (ntries >= HConstants.RETRY_BACKOFF.length) {
        ntries = HConstants.RETRY_BACKOFF.length - 1;
      }
      return this.pause * HConstants.RETRY_BACKOFF[ntries];
    }

    public HMasterInterface getMaster()
    throws MasterNotRunningException, ZooKeeperConnectionException {

      // Check if we already have a good master connection
      if (master != null) {
        if (master.isMasterRunning()) {
          return master;
        }
      }

      HServerAddress masterLocation = null;
      synchronized (this.masterLock) {
        for (int tries = 0;
          !this.closed &&
          !this.masterChecked && this.master == null &&
          tries < numRetries;
        tries++) {

          try {
            masterLocation = masterAddressTracker.getMasterAddress();
            if(masterLocation == null) {
              LOG.info("ZooKeeper available but no active master location found");
              throw new MasterNotRunningException();
            }

            HMasterInterface tryMaster = (HMasterInterface)HBaseRPC.getProxy(
                HMasterInterface.class, HMasterInterface.VERSION,
                masterLocation.getInetSocketAddress(), this.conf, this.rpcTimeout);

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
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while trying to connect to master.");
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

    public boolean isMasterRunning()
    throws MasterNotRunningException, ZooKeeperConnectionException {
      if (this.master == null) {
        getMaster();
      }
      boolean isRunning = master.isMasterRunning();
      if(isRunning) {
        return true;
      }
      throw new MasterNotRunningException();
    }

    public HRegionLocation getRegionLocation(final byte [] name,
        final byte [] row, boolean reload)
    throws IOException {
      return reload? relocateRegion(name, row): locateRegion(name, row);
    }

    public HTableDescriptor[] listTables() throws IOException {
      final TreeSet<HTableDescriptor> uniqueTables =
        new TreeSet<HTableDescriptor>();
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        public boolean processRow(Result result) throws IOException {
          try {
            byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER);
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
          byte[] value = row.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = Writables.getHRegionInfoOrNull(value);
          if (info != null) {
            if (Bytes.equals(tableName, info.getTableDesc().getName())) {
              value = row.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.SERVER_QUALIFIER);
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
     * @param True if table is online
     */
    private boolean testTableOnlineState(byte [] tableName, boolean online)
    throws IOException {
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        // The root region is always enabled
        return online;
      }
      String tableNameStr = Bytes.toString(tableName);
      try {
        if (online) {
          return ZKTable.isEnabledTable(this.zooKeeper, tableNameStr);
        }
        return ZKTable.isDisabledTable(this.zooKeeper, tableNameStr);
      } catch (KeeperException e) {
        throw new IOException("Enable/Disable failed", e);
      }
    }

    private static class HTableDescriptorFinder
    implements MetaScanner.MetaScannerVisitor {
        byte[] tableName;
        HTableDescriptor result;
        protected HTableDescriptorFinder(byte[] tableName) {
          this.tableName = tableName;
        }
        public boolean processRow(Result rowResult) throws IOException {
          HRegionInfo info = Writables.getHRegionInfoOrNull(
              rowResult.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER));
          if (info == null) return true;
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

    @Override
    public HRegionLocation locateRegion(final byte [] regionName)
    throws IOException {
      // TODO implement.  use old stuff or new stuff?
      return null;
    }

    @Override
    public List<HRegionLocation> locateRegions(final byte [] tableName)
    throws IOException {
      // TODO implement.  use old stuff or new stuff?
      return null;
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
    throws IOException {
      if (this.closed) throw new IOException(toString() + " closed");
      if (tableName == null || tableName.length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }

      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        try {
          HServerAddress hsa =
            this.rootRegionTracker.waitRootRegionLocation(this.rpcTimeout);
          LOG.debug("Lookedup root region location, connection=" + this +
            "; hsa=" + hsa);
          if (hsa == null) return null;
          return new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, hsa);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
        return locateRegionInMeta(HConstants.ROOT_TABLE_NAME, tableName, row,
            useCache, metaRegionLock);
      } else {
        // Region not in the cache - have to go to the meta RS
        return locateRegionInMeta(HConstants.META_TABLE_NAME, tableName, row,
            useCache, userRegionLock);
      }
    }

    /*
     * Search .META. for the HRegionLocation info that contains the table and
     * row we're seeking. It will prefetch certain number of regions info and
     * save them to the global region cache.
     */
    private void prefetchRegionCache(final byte[] tableName,
        final byte[] row) {
      // Implement a new visitor for MetaScanner, and use it to walk through
      // the .META.
      MetaScannerVisitor visitor = new MetaScannerVisitor() {
        public boolean processRow(Result result) throws IOException {
          try {
            byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER);
            HRegionInfo regionInfo = null;

            if (value != null) {
              // convert the row result into the HRegionLocation we need!
              regionInfo = Writables.getHRegionInfo(value);

              // possible we got a region of a different table...
              if (!Bytes.equals(regionInfo.getTableDesc().getName(),
                  tableName)) {
                return false; // stop scanning
              }
              if (regionInfo.isOffline()) {
                // don't cache offline regions
                return true;
              }
              value = result.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.SERVER_QUALIFIER);
              if (value == null) {
                return true;  // don't cache it
              }
              final String serverAddress = Bytes.toString(value);

              // instantiate the location
              HRegionLocation loc = new HRegionLocation(regionInfo,
                new HServerAddress(serverAddress));
              // cache this meta entry
              cacheLocation(tableName, loc);
            }
            return true;
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        }
      };
      try {
        // pre-fetch certain number of regions info at region cache.
        MetaScanner.metaScan(conf, visitor, tableName, row,
            this.prefetchRegionLimit);
      } catch (IOException e) {
        LOG.warn("Encountered problems when prefetch META table: ", e);
      }
    }

    /*
      * Search one of the meta tables (-ROOT- or .META.) for the HRegionLocation
      * info that contains the table and row we're seeking.
      */
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

        HRegionLocation metaLocation = null;
        try {
          // locate the root or meta region
          metaLocation = locateRegion(parentTable, metaKey);
          // If null still, go around again.
          if (metaLocation == null) continue;
          HRegionInterface server =
            getHRegionConnection(metaLocation.getServerAddress());

          Result regionInfoRow = null;
          // This block guards against two threads trying to load the meta
          // region at the same time. The first will load the meta region and
          // the second will use the value that the first one found.
          synchronized (regionLockObject) {
            // If the parent table is META, we may want to pre-fetch some
            // region info into the global region cache for this table.
            if (Bytes.equals(parentTable, HConstants.META_TABLE_NAME) &&
                (getRegionCachePrefetch(tableName)) )  {
              prefetchRegionCache(tableName, row);
            }

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
          byte[] value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
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

          value = regionInfoRow.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
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
              LOG.debug("locateRegionInMeta parentTable=" +
                Bytes.toString(parentTable) + ", metaLocation=" +
                ((metaLocation == null)? "null": metaLocation) + ", attempt=" +
                tries + " of " +
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
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Giving up trying to location region in " +
            "meta: thread is interrupted.");
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
    HRegionLocation getCachedLocation(final byte [] tableName,
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
          // this one. the exception case is when the endkey is
          // HConstants.EMPTY_START_ROW, signifying that the region we're
          // checking is actually the last region in the table.
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

    /**
     * Delete a cached location
     * @param tableName tableName
     * @param row
     */
    void deleteCachedLocation(final byte [] tableName, final byte [] row) {
      synchronized (this.cachedRegionLocations) {
        SoftValueSortedMap<byte [], HRegionLocation> tableLocations =
            getTableLocations(tableName);
        // start to examine the cache. we can only do cache actions
        // if there's something in the cache for this table.
        if (!tableLocations.isEmpty()) {
          HRegionLocation rl = getCachedLocation(tableName, row);
          if (rl != null) {
            tableLocations.remove(rl.getRegionInfo().getStartKey());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removed " +
                rl.getRegionInfo().getRegionNameAsString() +
                " for tableName=" + Bytes.toString(tableName) +
                " from cache " + "because of " + Bytes.toStringBinary(row));
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

    @Override
    public void clearRegionCache() {
      synchronized(this.cachedRegionLocations) {
        this.cachedRegionLocations.clear();
      }
    }

    @Override
    public void clearRegionCache(final byte [] tableName) {
      synchronized (this.cachedRegionLocations) {
        this.cachedRegionLocations.remove(Bytes.mapKey(tableName));
      }
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
                serverInterfaceClass, HRegionInterface.VERSION,
                regionServer.getInetSocketAddress(), this.conf,
                this.maxRPCAttempts, this.rpcTimeout, this.rpcTimeout);
          } catch (RemoteException e) {
            LOG.warn("RemoteException connecting to RS", e);
            // Throw what the RemoteException was carrying.
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

    /**
     * Get the ZooKeeper instance for this TableServers instance.
     *
     * If ZK has not been initialized yet, this will connect to ZK.
     * @returns zookeeper reference
     * @throws ZooKeeperConnectionException if there's a problem connecting to zk
     */
    public synchronized ZooKeeperWatcher getZooKeeperWatcher()
        throws ZooKeeperConnectionException {
      if(zooKeeper == null) {
        try {
          this.zooKeeper = new ZooKeeperWatcher(conf, "hconnection", this);
        } catch (IOException e) {
          throw new ZooKeeperConnectionException(e);
        }
      }
      return zooKeeper;
    }

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
          Thread.currentThread().interrupt();
          throw new IOException("Giving up trying to get region server: thread is interrupted.");
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
      if (this.zooKeeper != null) {
        LOG.info("Closed zookeeper sessionid=0x" +
          Long.toHexString(this.zooKeeper.getZooKeeper().getSessionId()));
        this.zooKeeper.close();
        this.zooKeeper = null;
      }
      this.closed = true;
    }

    private <R> Callable<MultiResponse> createCallable(
        final HServerAddress address,
        final MultiAction<R> multi,
        final byte [] tableName) {
      final HConnection connection = this;
      return new Callable<MultiResponse>() {
       public MultiResponse call() throws IOException {
         return getRegionServerWithoutRetries(
             new ServerCallable<MultiResponse>(connection, tableName, null) {
               public MultiResponse call() throws IOException {
                 return server.multi(multi);
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

    public void processBatch(List<Row> list,
        final byte[] tableName,
        ExecutorService pool,
        Object[] results) throws IOException, InterruptedException {

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException("argument results must be the same size as argument list");
      }

      processBatchCallback(list, tableName, pool, results, null);
    }

    /**
     * Executes the given
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call}
     * callable for each row in the
     * given list and invokes
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
     * for each result returned.
     *
     * @param protocol the protocol interface being called
     * @param rows a list of row keys for which the callable should be invoked
     * @param tableName table name for the coprocessor invoked
     * @param pool ExecutorService used to submit the calls per row
     * @param callable instance on which to invoke
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Call#call(Object)}
     * for each row
     * @param callback instance on which to invoke
     * {@link org.apache.hadoop.hbase.client.coprocessor.Batch.Callback#update(byte[], byte[], Object)}
     * for each result
     * @param <T> the protocol interface type
     * @param <R> the callable's return type
     * @throws IOException
     */
    public <T extends CoprocessorProtocol,R> void processExecs(
        final Class<T> protocol,
        List<byte[]> rows,
        final byte[] tableName,
        ExecutorService pool,
        final Batch.Call<T,R> callable,
        final Batch.Callback<R> callback)
      throws IOException, Throwable {

      Map<byte[],Future<R>> futures =
          new TreeMap<byte[],Future<R>>(Bytes.BYTES_COMPARATOR);
      for (final byte[] r : rows) {
        final ExecRPCInvoker invoker =
            new ExecRPCInvoker(conf, this, protocol, tableName, r);
        Future<R> future = pool.submit(
            new Callable<R>() {
              public R call() throws Exception {
                T instance = (T)Proxy.newProxyInstance(conf.getClassLoader(),
                    new Class[]{protocol},
                    invoker);
                R result = callable.call(instance);
                byte[] region = invoker.getRegionName();
                if (callback != null) {
                  callback.update(region, r, result);
                }
                return result;
              }
            });
        futures.put(r, future);
      }
      for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
        try {
          e.getValue().get();
        } catch (ExecutionException ee) {
          LOG.warn("Error executing for row "+Bytes.toStringBinary(e.getKey()), ee);
          throw ee.getCause();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted executing for row " +
              Bytes.toStringBinary(e.getKey()), ie);
        }
      }
    }

    /**
     * Parameterized batch processing, allowing varying return types for
     * different {@link Row} implementations.
     */
    public <R> void processBatchCallback(
        List<? extends Row> list,
        byte[] tableName,
        ExecutorService pool,
        Object[] results,
        Batch.Callback<R> callback)
    throws IOException, InterruptedException {

      // results must be the same size as list
      if (results.length != list.size()) {
        throw new IllegalArgumentException(
            "argument results must be the same size as argument list");
      }
      if (list.size() == 0) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("expecting "+results.length+" results");
      }

      // Keep track of the most recent servers for any given item for better
      // exceptional reporting.
      HServerAddress [] lastServers = new HServerAddress[results.length];
      List<Row> workingList = new ArrayList<Row>(list);
      boolean retry = true;
      Throwable singleRowCause = null;

      for (int tries = 0; tries < numRetries && retry; ++tries) {

        // sleep first, if this is a retry
        if (tries >= 1) {
          long sleepTime = getPauseTime(tries);
          LOG.debug("Retry " +tries+ ", sleep for " +sleepTime+ "ms!");
          Thread.sleep(sleepTime);
        }
        // step 1: break up into regionserver-sized chunks and build the data structs
        Map<HServerAddress, MultiAction<R>> actionsByServer =
          new HashMap<HServerAddress, MultiAction<R>>();
        for (int i = 0; i < workingList.size(); i++) {
          Row row = workingList.get(i);
          if (row != null) {
            HRegionLocation loc = locateRegion(tableName, row.getRow(), true);
            HServerAddress address = loc.getServerAddress();
            byte[] regionName = loc.getRegionInfo().getRegionName();

            MultiAction<R> actions = actionsByServer.get(address);
            if (actions == null) {
              actions = new MultiAction<R>();
              actionsByServer.put(address, actions);
            }

            Action<R> action = new Action<R>(regionName, row, i);
            lastServers[i] = address;
            actions.add(regionName, action);
          }
        }

        // step 2: make the requests

        Map<HServerAddress,Future<MultiResponse>> futures =
            new HashMap<HServerAddress, Future<MultiResponse>>(
                actionsByServer.size());

        for (Entry<HServerAddress, MultiAction<R>> e
             : actionsByServer.entrySet()) {
          futures.put(e.getKey(), pool.submit(createCallable(e.getKey(), e.getValue(), tableName)));
        }

        // step 3: collect the failures and successes and prepare for retry

        for (Entry<HServerAddress, Future<MultiResponse>> responsePerServer
             : futures.entrySet()) {
          HServerAddress address = responsePerServer.getKey();

          try {
            Future<MultiResponse> future = responsePerServer.getValue();
            MultiResponse resp = future.get();

            if (resp == null) {
              // Entire server failed
              LOG.debug("Failed all for server: " + address + ", removing from cache");
              continue;
            }

            for (Entry<byte[], List<Pair<Integer,Object>>> e : resp.getResults().entrySet()) {
              byte[] regionName = e.getKey();
              List<Pair<Integer, Object>> regionResults = e.getValue();
              for (Pair<Integer, Object> regionResult : regionResults) {
                if (regionResult == null) {
                  // if the first/only record is 'null' the entire region failed.
                  LOG.debug("Failures for region: " +
                      Bytes.toStringBinary(regionName) +
                      ", removing from cache");
                } else {
                  // Result might be an Exception, including DNRIOE
                  results[regionResult.getFirst()] = regionResult.getSecond();
                  if (callback != null && !(regionResult.getSecond() instanceof Throwable)) {
                    callback.update(e.getKey(),
                        list.get(regionResult.getFirst()).getRow(),
                        (R)regionResult.getSecond());
                  }
                }
              }
            }
          } catch (ExecutionException e) {
            LOG.debug("Failed all from " + address, e);
          }
        }

        // step 4: identify failures and prep for a retry (if applicable).

        // Find failures (i.e. null Result), and add them to the workingList (in
        // order), so they can be retried.
        retry = false;
        workingList.clear();
        for (int i = 0; i < results.length; i++) {
          // if null (fail) or instanceof Throwable && not instanceof DNRIOE
          // then retry that row. else dont.
          if (results[i] == null ||
              (results[i] instanceof Throwable &&
                  !(results[i] instanceof DoNotRetryIOException))) {

            retry = true;

            Row row = list.get(i);
            workingList.add(row);
            deleteCachedLocation(tableName, row.getRow());
          } else {
            // add null to workingList, so the order remains consistent with the original list argument.
            workingList.add(null);
          }
        }
      }

      if (retry) {
        // Simple little check for 1 item failures.
        if (singleRowCause != null) {
          throw new IOException(singleRowCause);
        }
      }


      List<Throwable> exceptions = new ArrayList<Throwable>();
      List<Row> actions = new ArrayList<Row>();
      List<HServerAddress> addresses = new ArrayList<HServerAddress>();

      for (int i = 0 ; i < results.length; i++) {
        if (results[i] == null || results[i] instanceof Throwable) {
          exceptions.add((Throwable)results[i]);
          actions.add(list.get(i));
          addresses.add(lastServers[i]);
        }
      }

      if (!exceptions.isEmpty()) {
        throw new RetriesExhaustedWithDetailsException(exceptions,
            actions,
            addresses);
      }
    }

    /**
     * @deprecated Use HConnectionManager::processBatch instead.
     */
    public void processBatchOfPuts(List<Put> list,
        final byte[] tableName,
        ExecutorService pool) throws IOException {
      Object[] results = new Object[list.size()];
      try {
        processBatch((List) list, tableName, pool, results);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {

        // mutate list so that it is empty for complete success, or contains only failed records
        // results are returned in the same order as the requests in list
        // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
        for (int i = results.length - 1; i>=0; i--) {
          if (results[i] instanceof Result) {
            // successful Puts are removed from the list here.
            list.remove(i);
          }
        }
      }
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

    /*
     * Return the number of cached region for a table. It will only be called
     * from a unit test.
     */
    int getNumberOfCachedRegionLocations(final byte[] tableName) {
      Integer key = Bytes.mapKey(tableName);
      synchronized (this.cachedRegionLocations) {
        SoftValueSortedMap<byte[], HRegionLocation> tableLocs =
          this.cachedRegionLocations.get(key);

        if (tableLocs == null) {
          return 0;
        }
        return tableLocs.values().size();
      }
    }

    /**
     * Check the region cache to see whether a region is cached yet or not.
     * Called by unit tests.
     * @param tableName tableName
     * @param row row
     * @return Region cached or not.
     */
    boolean isRegionCached(final byte[] tableName, final byte[] row) {
      HRegionLocation location = getCachedLocation(tableName, row);
      return location != null;
    }

    public void setRegionCachePrefetch(final byte[] tableName,
        final boolean enable) {
      if (!enable) {
        regionCachePrefetchDisabledTables.add(Bytes.mapKey(tableName));
      }
      else {
        regionCachePrefetchDisabledTables.remove(Bytes.mapKey(tableName));
      }
    }

    public boolean getRegionCachePrefetch(final byte[] tableName) {
      return !regionCachePrefetchDisabledTables.contains(Bytes.mapKey(tableName));
    }

    public void prewarmRegionCache(final byte[] tableName,
        final Map<HRegionInfo, HServerAddress> regions) {
      for (Map.Entry<HRegionInfo, HServerAddress> e : regions.entrySet()) {
        cacheLocation(tableName,
            new HRegionLocation(e.getKey(), e.getValue()));
      }
    }

    @Override
    public void abort(final String msg, Throwable t) {
      if (t instanceof KeeperException.SessionExpiredException) {
        try {
          LOG.info("This client just lost it's session with ZooKeeper, trying" +
              " to reconnect.");
          resetZooKeeperTrackers();
          LOG.info("Reconnected successfully. This disconnect could have been" +
              " caused by a network partition or a long-running GC pause," +
              " either way it's recommended that you verify your environment.");
          return;
        } catch (ZooKeeperConnectionException e) {
          LOG.error("Could not reconnect to ZooKeeper after session" +
              " expiration, aborting");
          t = e;
        }
      }
      if (t != null) LOG.fatal(msg, t);
      else LOG.fatal(msg);
      this.closed = true;
    }
  }
}
