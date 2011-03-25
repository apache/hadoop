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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

/**
 * Used to communicate with a single HBase table.
 *
 * This class is not thread safe for updates; the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 *
 * <p>Instances of HTable passed the same {@link Configuration} instance will
 * share connections to servers out on the cluster and to the zookeeper ensemble
 * as well as caches of  region locations.  This is usually a *good* thing.
 * This happens because they will all share the same underlying
 * {@link HConnection} instance.  See {@link HConnectionManager} for more on
 * how this mechanism works.
 *
 * <p>{@link HConnection} will read most of the
 * configuration it needs from the passed {@link Configuration} on initial
 * construction.  Thereafter, for settings such as
 * <code>hbase.client.pause</code>, <code>hbase.client.retries.number</code>,
 * and <code>hbase.client.rpc.maxattempts</code> updating their values in the
 * passed {@link Configuration} subsequent to {@link HConnection} construction
 * will go unnoticed.  To run with changed values, make a new
 * {@link HTable} passing a new {@link Configuration} instance that has the
 * new configuration.
 *
 * @see HBaseAdmin for create, drop, list, enable and disable of tables.
 * @see HConnection
 * @see HConnectionManager
 */
public class HTable implements HTableInterface {
  private static final Log LOG = LogFactory.getLog(HTable.class);
  private final HConnection connection;
  private final byte [] tableName;
  protected final int scannerTimeout;
  private volatile Configuration configuration;
  private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private int maxKeyValueSize;
  private ExecutorService pool;  // For Multi
  private long maxScannerResultSize;

  /**
   * Creates an object to access a HBase table.
   * Internally it creates a new instance of {@link Configuration} and a new
   * client to zookeeper as well as other resources.  It also comes up with
   * a fresh view of the cluster and must do discovery from scratch of region
   * locations; i.e. it will not make use of already-cached region locations if
   * available. Use only when being quick and dirty.
   * @throws IOException if a remote or network exception occurs
   * @see #HTable(Configuration, String)
   */
  public HTable(final String tableName)
  throws IOException {
    this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
  }

  /**
   * Creates an object to access a HBase table.
   * Internally it creates a new instance of {@link Configuration} and a new
   * client to zookeeper as well as other resources.  It also comes up with
   * a fresh view of the cluster and must do discovery from scratch of region
   * locations; i.e. it will not make use of already-cached region locations if
   * available. Use only when being quick and dirty.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   * @see #HTable(Configuration, String)
   */
  public HTable(final byte [] tableName)
  throws IOException {
    this(HBaseConfiguration.create(), tableName);
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }


  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte [] tableName)
  throws IOException {
    this.tableName = tableName;
    if (conf == null) {
      this.scannerTimeout = 0;
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.scannerTimeout =
      (int) conf.getLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);
    this.configuration = conf;
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
    this.writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = conf.getInt("hbase.client.scanner.caching", 1);

    this.maxScannerResultSize = conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    this.maxKeyValueSize = conf.getInt("hbase.client.keyvalue.maxsize", -1);

    int nrThreads = conf.getInt("hbase.htable.threads.max", getCurrentNrHRS());
    if (nrThreads == 0) {
      nrThreads = 1; // is there a better default?
    }

    // Unfortunately Executors.newCachedThreadPool does not allow us to
    // set the maximum size of the pool, so we have to do it ourselves.
    // Must also set set corethreadpool size as with a LinkedBlockingQueue,
    // a new thread will not be started until the queue is full
    this.pool = new ThreadPoolExecutor(nrThreads, nrThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new DaemonThreadFactory());
    ((ThreadPoolExecutor)this.pool).allowCoreThreadTimeOut(true);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * @return the number of region servers that are currently running
   * @throws IOException if a remote or network exception occurs
   */
  public int getCurrentNrHRS() throws IOException {
    try {
      // We go to zk rather than to master to get count of regions to avoid
      // HTable having a Master dependency.  See HBase-2828
      return ZKUtil.getNumberOfChildren(this.connection.getZooKeeperWatcher(),
          this.connection.getZooKeeperWatcher().rsZNode);
    } catch (KeeperException ke) {
      throw new IOException("Unexpected ZooKeeper exception", ke);
    }
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   */
  public static boolean isTableEnabled(Configuration conf, byte[] tableName)
  throws IOException {
    return HConnectionManager.getConnection(conf).isTableEnabled(tableName);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  @Override
  public byte [] getTableName() {
    return this.tableName;
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   */
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by
   * {@code hbase.client.scanner.caching}.
   * Increasing this value will reduce the amount of work needed each time
   * {@code next()} is called on a scanner, at the expense of memory use
   * (since more rows will need to be maintained in memory by the scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   */
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte [][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  @SuppressWarnings("unchecked")
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    final List<byte[]> startKeyList = new ArrayList<byte[]>();
    final List<byte[]> endKeyList = new ArrayList<byte[]>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result rowResult) throws IOException {
        byte [] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
        if (bytes == null) {
          LOG.warn("Null " + HConstants.REGIONINFO_QUALIFIER + " cell in " +
            rowResult);
          return true;
        }
        HRegionInfo info = Writables.getHRegionInfo(bytes);
        if (Bytes.equals(info.getTableDesc().getName(), getTableName())) {
          if (!(info.isOffline() || info.isSplit())) {
            startKeyList.add(info.getStartKey());
            endKeyList.add(info.getEndKey());
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(configuration, visitor, this.tableName);
    return new Pair(startKeyList.toArray(new byte[startKeyList.size()][]),
                endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public Map<HRegionInfo, HServerAddress> getRegionsInfo() throws IOException {
    final Map<HRegionInfo, HServerAddress> regionMap =
      new TreeMap<HRegionInfo, HServerAddress>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result rowResult) throws IOException {
        HRegionInfo info = Writables.getHRegionInfo(
            rowResult.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));

        if (!(Bytes.equals(info.getTableDesc().getName(), getTableName()))) {
          return false;
        }

        HServerAddress server = new HServerAddress();
        byte [] value = rowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SERVER_QUALIFIER);
        if (value != null && value.length > 0) {
          String address = Bytes.toString(value);
          server = new HServerAddress(address);
        }

        if (!(info.isOffline() || info.isSplit())) {
          regionMap.put(new UnmodifyableHRegionInfo(info), server);
        }
        return true;
      }

    };
    MetaScanner.metaScan(configuration, visitor, tableName);
    return regionMap;
  }

  /**
   * Save the passed region information and the table's regions
   * cache.
   * <p>
   * This is mainly useful for the MapReduce integration. You can call
   * {@link #deserializeRegionInfo deserializeRegionInfo}
   * to deserialize regions information from a
   * {@link DataInput}, then call this method to load them to cache.
   *
   * <pre>
   * {@code
   * HTable t1 = new HTable("foo");
   * FileInputStream fis = new FileInputStream("regions.dat");
   * DataInputStream dis = new DataInputStream(fis);
   *
   * Map<HRegionInfo, HServerAddress> hm = t1.deserializeRegionInfo(dis);
   * t1.prewarmRegionCache(hm);
   * }
   * </pre>
   * @param regionMap This piece of regions information will be loaded
   * to region cache.
   */
  public void prewarmRegionCache(Map<HRegionInfo, HServerAddress> regionMap) {
    this.connection.prewarmRegionCache(this.getTableName(), regionMap);
  }

  /**
   * Serialize the regions information of this table and output
   * to <code>out</code>.
   * <p>
   * This is mainly useful for the MapReduce integration. A client could
   * perform a large scan for all the regions for the table, serialize the
   * region info to a file. MR job can ship a copy of the meta for the table in
   * the DistributedCache.
   * <pre>
   * {@code
   * FileOutputStream fos = new FileOutputStream("regions.dat");
   * DataOutputStream dos = new DataOutputStream(fos);
   * table.serializeRegionInfo(dos);
   * dos.flush();
   * dos.close();
   * }
   * </pre>
   * @param out {@link DataOutput} to serialize this object into.
   * @throws IOException if a remote or network exception occurs
   */
  public void serializeRegionInfo(DataOutput out) throws IOException {
    Map<HRegionInfo, HServerAddress> allRegions = this.getRegionsInfo();
    // first, write number of regions
    out.writeInt(allRegions.size());
    for (Map.Entry<HRegionInfo, HServerAddress> es : allRegions.entrySet()) {
      es.getKey().write(out);
      es.getValue().write(out);
    }
  }

  /**
   * Read from <code>in</code> and deserialize the regions information.
   *
   * <p>It behaves similarly as {@link #getRegionsInfo getRegionsInfo}, except
   * that it loads the region map from a {@link DataInput} object.
   *
   * <p>It is supposed to be followed immediately by  {@link
   * #prewarmRegionCache prewarmRegionCache}.
   *
   * <p>
   * Please refer to {@link #prewarmRegionCache prewarmRegionCache} for usage.
   *
   * @param in {@link DataInput} object.
   * @return A map of HRegionInfo with its server address.
   * @throws IOException if an I/O exception occurs.
   */
  public Map<HRegionInfo, HServerAddress> deserializeRegionInfo(DataInput in)
  throws IOException {
    final Map<HRegionInfo, HServerAddress> allRegions =
      new TreeMap<HRegionInfo, HServerAddress>();

    // the first integer is expected to be the size of records
    int regionsCount = in.readInt();
    for (int i = 0; i < regionsCount; ++i) {
      HRegionInfo hri = new HRegionInfo();
      hri.readFields(in);
      HServerAddress hsa = new HServerAddress();
      hsa.readFields(in);
      allRegions.put(hri, hsa);
    }
    return allRegions;
  }

   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return connection.getRegionServerWithRetries(
         new ServerCallable<Result>(connection, tableName, row) {
       public Result call() throws IOException {
         return server.getClosestRowBefore(location.getRegionInfo().getRegionName(),
           row, family);
       }
     });
   }

  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    ClientScanner s = new ClientScanner(scan);
    s.initialize();
    return s;
  }

  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  public Result get(final Get get) throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, get.getRow()) {
          public Result call() throws IOException {
            return server.get(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

   public Result[] get(List<Get> gets) throws IOException {
     try {
       Object [] r1 = batch((List)gets);

       // translate.
       Result [] results = new Result[r1.length];
       int i=0;
       for (Object o : r1) {
         // batch ensures if there is a failure we get an exception instead
         results[i++] = (Result) o;
       }

       return results;
     } catch (InterruptedException e) {
       throw new IOException(e);
     }
   }

  /**
   * Method that does a batch call on Deletes, Gets and Puts.  The ordering of
   * execution of the actions is not defined. Meaning if you do a Put and a
   * Get in the same {@link #batch} call, you will not necessarily be
   * guaranteed that the Get returns what the Put had put.
   *
   * @param actions list of Get, Put, Delete objects
   * @param results Empty Result[], same size as actions. Provides access to
   * partial results, in case an exception is thrown. If there are any failures,
   * there will be a null or Throwable will be in the results array, AND an
   * exception will be thrown.
   * @throws IOException
   */
  @Override
  public synchronized void batch(final List<Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    connection.processBatch(actions, tableName, pool, results);
  }

  /**
   * Method that does a batch call on Deletes, Gets and Puts.
   *
   * @param actions list of Get, Put, Delete objects
   * @return the results from the actions. A null in the return array means that
   * the call for that action failed, even after retries
   * @throws IOException
   */
  @Override
  public synchronized Object[] batch(final List<Row> actions) throws InterruptedException, IOException {
    Object[] results = new Object[actions.size()];
    connection.processBatch(actions, tableName, pool, results);
    return results;
  }

  /**
   * Deletes the specified cells/row.
   *
   * @param delete The object that specifies what to delete.
   * @throws IOException if a remote or network exception occurs.
   * @since 0.20.0
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, delete.getRow()) {
          public Boolean call() throws IOException {
            server.delete(location.getRegionInfo().getRegionName(), delete);
            return null; // FindBugs NP_BOOLEAN_RETURN_NULL
          }
        }
    );
  }

  /**
   * Deletes the specified cells/rows in bulk.
   * @param deletes List of things to delete. As a side effect, it will be modified:
   * successful {@link Delete}s are removed. The ordering of the list will not change.
   * @throws IOException if a remote or network exception occurs. In that case
   * the {@code deletes} argument will contain the {@link Delete} instances
   * that have not be successfully applied.
   * @since 0.20.1
   * @see #batch(java.util.List, Object[])
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      connection.processBatch((List) deletes, tableName, pool, results);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list
      // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    doPut(Arrays.asList(put));
  }

  @Override
  public void put(final List<Put> puts) throws IOException {
    doPut(puts);
  }

  private void doPut(final List<Put> puts) throws IOException {
    for (Put put : puts) {
      validatePut(put);
      writeBuffer.add(put);
      currentWriteBufferSize += put.heapSize();
    }
    if (autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  @Override
  public Result increment(final Increment increment) throws IOException {
    if (!increment.hasFamilies()) {
      throw new IOException(
          "Invalid arguments to increment, no columns specified");
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Result>(connection, tableName, increment.getRow()) {
          public Result call() throws IOException {
            return server.increment(
                location.getRegionInfo().getRegionName(), increment);
          }
        }
    );
  }

  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("column is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }
    return connection.getRegionServerWithRetries(
        new ServerCallable<Long>(connection, tableName, row) {
          public Long call() throws IOException {
            return server.incrementColumnValue(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
          }
        }
    );
  }

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put.  If value == null, checks for non-existence
   * of the value.
   *
   * @param row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put put to execute if value matches.
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row) {
          public Boolean call() throws IOException {
            return server.checkAndPut(location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, put) ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the delete.  If value == null, checks for non-existence
   * of the value.
   *
   * @param row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to execute if value matches.
   * @throws IOException
   * @return true if the new delete was executed, false otherwise
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, row) {
          public Boolean call() throws IOException {
            return server.checkAndDelete(
                location.getRegionInfo().getRegionName(),
                row, family, qualifier, value, delete)
            ? Boolean.TRUE : Boolean.FALSE;
          }
        }
    );
  }

  /**
   * Test for the existence of columns in the table, as specified in the Get.<p>
   *
   * This will return true if the Get matches one or more keys, false if not.<p>
   *
   * This is a server-side call so it prevents any data from being transfered
   * to the client.
   * @param get param to check for
   * @return true if the specified Get matches one or more keys, false if not
   * @throws IOException
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    return connection.getRegionServerWithRetries(
        new ServerCallable<Boolean>(connection, tableName, get.getRow()) {
          public Boolean call() throws IOException {
            return server.
                exists(location.getRegionInfo().getRegionName(), get);
          }
        }
    );
  }

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch
   * of {@link Put}s (when {@link #batch(List)} is used) when
   * {@link #isAutoFlush()} is {@code true}.
   * @throws IOException if a remote or network exception occurs.
   */
  @Override
  public void flushCommits() throws IOException {
    try {
      connection.processBatchOfPuts(writeBuffer, tableName, pool);
    } finally {
      // the write buffer was adjusted by processBatchOfPuts
      currentWriteBufferSize = 0;
      for (Put aPut : writeBuffer) {
        currentWriteBufferSize += aPut.heapSize();
      }
    }
  }

  @Override
  public void close() throws IOException {
    flushCommits();
  }

  // validate for well-formedness
  private void validatePut(final Put put) throws IllegalArgumentException{
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  @Override
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return connection.getRegionServerWithRetries(
      new ServerCallable<RowLock>(connection, tableName, row) {
        public RowLock call() throws IOException {
          long lockId =
              server.lockRow(location.getRegionInfo().getRegionName(), row);
          return new RowLock(row,lockId);
        }
      }
    );
  }

  @Override
  public void unlockRow(final RowLock rl)
  throws IOException {
    connection.getRegionServerWithRetries(
      new ServerCallable<Boolean>(connection, tableName, rl.getRow()) {
        public Boolean call() throws IOException {
          server.unlockRow(location.getRegionInfo().getRegionName(),
              rl.getLockId());
          return null; // FindBugs NP_BOOLEAN_RETURN_NULL
        }
      }
    );
  }

  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed
   * and are immediately executed.  This is slower but safer.
   * <p>
   * Turning this off means that multiple {@link Put}s will be accepted before
   * any RPC is actually sent to do the write operations.  If the application
   * dies before pending writes get flushed to HBase, data will be lost.
   * Other side effects may include the fact that the application thinks a
   * {@link Put} was executed successfully whereas it was in fact only
   * buffered and the operation may fail when attempting to flush all pending
   * writes.  In that case though, the code will retry the failed {@link Put}
   * upon its next attempt to flush the buffer.
   *
   * @param autoFlush Whether or not to enable 'auto-flush'.
   * @see #flushCommits
   */
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public ArrayList<Put> getWriteBuffer() {
    return writeBuffer;
  }

  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements ResultScanner {
    private final Log CLIENT_LOG = LogFactory.getLog(this.getClass());
    // HEADSUP: The scan internal start row can change as we move through table.
    private Scan scan;
    private boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    private HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    private final LinkedList<Result> cache = new LinkedList<Result>();
    private final int caching;
    private long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    private Result lastResult = null;

    protected ClientScanner(final Scan scan) {
      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Creating scanner over "
            + Bytes.toString(getTableName())
            + " starting at key '" + Bytes.toStringBinary(scan.getStartRow()) + "'");
      }
      this.scan = scan;
      this.lastNext = System.currentTimeMillis();

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = HTable.this.scannerCaching;
      }

      // Removed filter validation.  We have a new format now, only one of all
      // the current filters has a validate() method.  We can add it back,
      // need to decide on what we're going to do re: filter redesign.
      // Need, at the least, to break up family from qualifier as separate
      // checks, I think it's important server-side filters are optimal in that
      // respect.
    }

    public void initialize() throws IOException {
      nextScanner(this.caching, false);
    }

    protected Scan getScan() {
      return scan;
    }

    protected long getTimestamp() {
      return lastNext;
    }

    // returns true if the passed region endKey
    private boolean checkScanStopRow(final byte [] endKey) {
      if (this.scan.getStopRow().length > 0) {
        // there is a stop row, check to see if we are past it.
        byte [] stopRow = scan.getStopRow();
        int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
        if (cmp <= 0) {
          // stopRow <= endKey (endKey is equals to or larger than stopRow)
          // This is a stop.
          return true;
        }
      }
      return false; //unlikely.
    }

    /*
     * Gets a scanner for the next region.  If this.currentRegion != null, then
     * we will move to the endrow of this.currentRegion.  Else we will get
     * scanner at the scan.getStartRow().  We will go no further, just tidy
     * up outstanding scanners, if <code>currentRegion != null</code> and
     * <code>done</code> is true.
     * @param nbRows
     * @param done Server-side says we're done scanning.
     */
    private boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        getConnection().getRegionServerWithRetries(callable);
        this.callable = null;
      }

      // Where to start the next scanner
      byte [] localStartKey;

      // if we're at end of table, close and return false to stop iterating
      if (this.currentRegion != null) {
        byte [] endKey = this.currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            checkScanStopRow(endKey) ||
            done) {
          close();
          if (CLIENT_LOG.isDebugEnabled()) {
            CLIENT_LOG.debug("Finished with scanning at " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (CLIENT_LOG.isDebugEnabled()) {
          CLIENT_LOG.debug("Finished with region " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        getConnection().getRegionServerWithRetries(callable);
        this.currentRegion = callable.getHRegionInfo();
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(getConnection(),
        getTableName(), scan);
      s.setCaching(nbRows);
      return s;
    }

    public Result next() throws IOException {
      // If the scanner is closed but there is some rows left in the cache,
      // it will first empty it before returning null
      if (cache.size() == 0 && this.closed) {
        return null;
      }
      if (cache.size() == 0) {
        Result [] values = null;
        long remainingResultSize = maxScannerResultSize;
        int countdown = this.caching;
        // We need to reset it if it's a new callable that was created
        // with a countdown in nextScanner
        callable.setCaching(this.caching);
        // This flag is set when we want to skip the result returned.  We do
        // this when we reset scanner because it split under us.
        boolean skipFirst = false;
        do {
          try {
            if (skipFirst) {
              // Skip only the first row (which was the last row of the last
              // already-processed batch).
              callable.setCaching(1);
              values = getConnection().getRegionServerWithRetries(callable);
              callable.setCaching(this.caching);
              skipFirst = false;
            }
            // Server returns a null values if scanning is to stop.  Else,
            // returns an empty array if scanning is to go on and we've just
            // exhausted current region.
            values = getConnection().getRegionServerWithRetries(callable);
          } catch (DoNotRetryIOException e) {
            if (e instanceof UnknownScannerException) {
              long timeout = lastNext + scannerTimeout;
              // If we are over the timeout, throw this exception to the client
              // Else, it's because the region moved and we used the old id
              // against the new region server; reset the scanner.
              if (timeout < System.currentTimeMillis()) {
                long elapsed = System.currentTimeMillis() - lastNext;
                ScannerTimeoutException ex = new ScannerTimeoutException(
                    elapsed + "ms passed since the last invocation, " +
                        "timeout is currently set to " + scannerTimeout);
                ex.initCause(e);
                throw ex;
              }
            } else {
              Throwable cause = e.getCause();
              if (cause == null || !(cause instanceof NotServingRegionException)) {
                throw e;
              }
            }
            // Else, its signal from depths of ScannerCallable that we got an
            // NSRE on a next and that we need to reset the scanner.
            if (this.lastResult != null) {
              this.scan.setStartRow(this.lastResult.getRow());
              // Skip first row returned.  We already let it out on previous
              // invocation.
              skipFirst = true;
            }
            // Clear region
            this.currentRegion = null;
            continue;
          }
          lastNext = System.currentTimeMillis();
          if (values != null && values.length > 0) {
            for (Result rs : values) {
              cache.add(rs);
              for (KeyValue kv : rs.raw()) {
                  remainingResultSize -= kv.heapSize();
              }
              countdown--;
              this.lastResult = rs;
            }
          }
          // Values == null means server-side filter has determined we must STOP
        } while (remainingResultSize > 0 && countdown > 0 && nextScanner(countdown, values == null));
      }

      if (cache.size() > 0) {
        return cache.poll();
      }
      return null;
    }

    /**
     * Get <param>nbRows</param> rows.
     * How many RPCs are made is determined by the {@link Scan#setCaching(int)}
     * setting (or hbase.client.scanner.caching in hbase-site.xml).
     * @param nbRows number of rows to return
     * @return Between zero and <param>nbRows</param> RowResults.  Scan is done
     * if returned array is of zero-length (We never return null).
     * @throws IOException
     */
    public Result [] next(int nbRows) throws IOException {
      // Collect values to be returned here
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for(int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next != null) {
          resultSets.add(next);
        } else {
          break;
        }
      }
      return resultSets.toArray(new Result[resultSets.size()]);
    }

    public void close() {
      if (callable != null) {
        callable.setClose();
        try {
          getConnection().getRegionServerWithRetries(callable);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        callable = null;
      }
      closed = true;
    }

    public Iterator<Result> iterator() {
      return new Iterator<Result>() {
        // The next RowResult, possibly pre-read
        Result next = null;

        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
        public boolean hasNext() {
          if (next == null) {
            try {
              next = ClientScanner.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return true;
        }

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public Result next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }

          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          Result temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  static class DaemonThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        DaemonThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null)? s.getThreadGroup() :
                                 Thread.currentThread().getThreadGroup();
            namePrefix = "pool-" +
                          poolNumber.getAndIncrement() +
                         "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (!t.isDaemon()) {
              t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
              t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws ZooKeeperConnectionException
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      boolean enable) throws ZooKeeperConnectionException {
    HConnectionManager.getConnection(HBaseConfiguration.create()).
    setRegionCachePrefetch(tableName, enable);
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param conf The Configuration object to use.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws ZooKeeperConnectionException
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, boolean enable) throws ZooKeeperConnectionException {
    HConnectionManager.getConnection(conf).setRegionCachePrefetch(
        tableName, enable);
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws ZooKeeperConnectionException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws ZooKeeperConnectionException {
    return HConnectionManager.getConnection(conf).getRegionCachePrefetch(
        tableName);
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws ZooKeeperConnectionException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) throws ZooKeeperConnectionException {
    return HConnectionManager.getConnection(HBaseConfiguration.create()).
    getRegionCachePrefetch(tableName);
  }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    this.connection.clearRegionCache();
  }

  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(
      Class<T> protocol, byte[] row) {
    return (T)Proxy.newProxyInstance(this.getClass().getClassLoader(),
        new Class[]{protocol},
        new ExecRPCInvoker(configuration,
            connection,
            protocol,
            tableName,
            row));
  }

  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[],R> coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable)
      throws IOException, Throwable {

    final Map<byte[],R> results = new TreeMap<byte[],R>(
        Bytes.BYTES_COMPARATOR);
    coprocessorExec(protocol, startKey, endKey, callable,
        new Batch.Callback<R>(){
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(
      Class<T> protocol, byte[] startKey, byte[] endKey,
      Batch.Call<T,R> callable, Batch.Callback<R> callback)
      throws IOException, Throwable {

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);
    connection.processExecs(protocol, keys, tableName, pool, callable,
        callback);
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    Pair<byte[][],byte[][]> startEndKeys = getStartEndKeys();
    byte[][] startKeys = startEndKeys.getFirst();
    byte[][] endKeys = startEndKeys.getSecond();

    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }

    List<byte[]> rangeKeys = new ArrayList<byte[]>();
    for (int i=0; i<startKeys.length; i++) {
      if (Bytes.compareTo(start, startKeys[i]) >= 0 ) {
        if (Bytes.equals(endKeys[i], HConstants.EMPTY_END_ROW) ||
            Bytes.compareTo(start, endKeys[i]) < 0) {
          rangeKeys.add(start);
        }
      } else if (Bytes.equals(end, HConstants.EMPTY_END_ROW) ||
          Bytes.compareTo(startKeys[i], end) <= 0) {
        rangeKeys.add(startKeys[i]);
      } else {
        break; // past stop
      }
    }

    return rangeKeys;
  }

}
