/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.zookeeper.ZooKeeper;

/**
 * Facility for testing HBase. Added as tool to abet junit4 testing.  Replaces
 * old HBaseTestCase and HBaseCluserTestCase functionality.
 * Create an instance and keep it around doing HBase testing.  This class is
 * meant to be your one-stop shop for anything you might need testing.  Manages
 * one cluster at a time only.  Depends on log4j being on classpath and
 * hbase-site.xml for logging and test-run configuration.  It does not set
 * logging levels nor make changes to configuration parameters.
 */
public class HBaseTestingUtility {
  private final Log LOG = LogFactory.getLog(getClass());
  private final Configuration conf;
  private MiniZooKeeperCluster zkCluster = null;
  private MiniDFSCluster dfsCluster = null;
  private MiniHBaseCluster hbaseCluster = null;
  private MiniMRCluster mrCluster = null;
  // If non-null, then already a cluster running.
  private File clusterTestBuildDir = null;
  private HBaseAdmin hbaseAdmin = null;

  /**
   * System property key to get test directory value.
   */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";

  /**
   * Default parent directory for test output.
   */
  public static final String DEFAULT_TEST_DIRECTORY = "target/build/data";

  public HBaseTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public HBaseTestingUtility(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return Instance of Configuration.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #DEFAULT_TEST_DIRECTORY}
   * @see #setupClusterTestBuildDir()
   */
  public static Path getTestDir() {
    return new Path(System.getProperty(TEST_DIRECTORY_KEY,
      DEFAULT_TEST_DIRECTORY));
  }

  /**
   * @param subdirName
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getTestDir()}.
   * @see #setupClusterTestBuildDir()
   */
  public static Path getTestDir(final String subdirName) {
    return new Path(getTestDir(), subdirName);
  }

  /**
   * Home our cluster in a dir under target/test.  Give it a random name
   * so can have many concurrent clusters running if we need to.  Need to
   * amend the test.build.data System property.  Its what minidfscluster bases
   * it data dir on.  Moding a System property is not the way to do concurrent
   * instances -- another instance could grab the temporary
   * value unintentionally -- but not anything can do about it at moment;
   * single instance only is how the minidfscluster works.
   * @return The calculated cluster test build directory.
   */
  File setupClusterTestBuildDir() {
    String randomStr = UUID.randomUUID().toString();
    String dirStr = getTestDir(randomStr).toString();
    File dir = new File(dirStr).getAbsoluteFile();
    // Have it cleaned up on exit
    dir.deleteOnExit();
    return dir;
  }

  /**
   * @throws IOException If a cluster -- zk, dfs, or hbase -- already running.
   */
  void isRunningCluster() throws IOException {
    if (this.clusterTestBuildDir == null) return;
    throw new IOException("Cluster already running at " +
      this.clusterTestBuildDir);
  }

  /**
   * Start a minidfscluster.
   * @param servers How many DNs to start.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null);
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param dir Where to home your dfs cluster.
   * @param servers How many DNs to start.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers, final File dir)
  throws Exception {
    // This does the following to home the minidfscluster
    //     base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
    // Some tests also do this:
    //  System.getProperty("test.cache.data", "build/test/cache");
    if (dir == null) this.clusterTestBuildDir = setupClusterTestBuildDir();
    else this.clusterTestBuildDir = dir;
    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.toString());
    System.setProperty("test.cache.data", this.clusterTestBuildDir.toString());
    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
      true, null, null, null, null);
    return this.dfsCluster;
  }

  /**
   * Shuts down instance created by call to {@link #startMiniDFSCluster(int, File)}
   * or does nothing.
   * @throws Exception
   */
  public void shutdownMiniDFSCluster() throws Exception {
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
    }
  }

  /**
   * Call this if you only want a zk cluster.
   * @see #startMiniZKCluster() if you want zk + dfs + hbase mini cluster.
   * @throws Exception
   * @see #shutdownMiniZKCluster()
   * @return zk cluster started.
   */
  public MiniZooKeeperCluster startMiniZKCluster() throws Exception {
    return startMiniZKCluster(setupClusterTestBuildDir());

  }

  private MiniZooKeeperCluster startMiniZKCluster(final File dir)
  throws Exception {
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + dir);
    }
    this.zkCluster = new MiniZooKeeperCluster();
    int clientPort = this.zkCluster.startup(dir);
    this.conf.set("hbase.zookeeper.property.clientPort",
      Integer.toString(clientPort));
    return this.zkCluster;
  }

  /**
   * Shuts down zk cluster created by call to {@link #startMiniZKCluster(File)}
   * or does nothing.
   * @throws IOException
   * @see #startMiniZKCluster()
   */
  public void shutdownMiniZKCluster() throws IOException {
    if (this.zkCluster != null) this.zkCluster.shutdown();
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * @throws Exception
   * @return Mini hbase cluster instance created.
   * @see {@link #shutdownMiniDFSCluster()}
   */
  public MiniHBaseCluster startMiniCluster() throws Exception {
    return startMiniCluster(1);
  }

  /**
   * Start up a minicluster of hbase, optionally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * @param servers Number of servers to start up.  We'll start this many
   * datanodes and regionservers.  If servers is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int servers)
  throws Exception {
    LOG.info("Starting up minicluster");
    // If we already put up a cluster, fail.
    isRunningCluster();
    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    this.clusterTestBuildDir = setupClusterTestBuildDir();
    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.getPath());
    // Bring up mini dfs cluster. This spews a bunch of warnings about missing
    // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
    startMiniDFSCluster(servers, this.clusterTestBuildDir);

    // Mangle conf so fs parameter points to minidfs we just started up
    FileSystem fs = this.dfsCluster.getFileSystem();
    this.conf.set("fs.defaultFS", fs.getUri().toString());
    // Do old style too just to be safe.
    this.conf.set("fs.default.name", fs.getUri().toString());
    this.dfsCluster.waitClusterUp();
   
    // Start up a zk cluster.
    if (this.zkCluster == null) {
      startMiniZKCluster(this.clusterTestBuildDir);
    }

    // Now do the mini hbase cluster.  Set the hbase.rootdir in config.
    Path hbaseRootdir = fs.makeQualified(fs.getHomeDirectory());
    this.conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    this.hbaseCluster = new MiniHBaseCluster(this.conf, servers);
    // Don't leave here till we've done a successful scan of the .META.
    HTable t = new HTable(this.conf, HConstants.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) continue;
    LOG.info("Minicluster is up");
    return this.hbaseCluster;
  }

  /**
   * @return Current mini hbase cluster. Only has something in it after a call
   * to {@link #startMiniCluster()}.
   * @see #startMiniCluster()
   */
  public MiniHBaseCluster getMiniHBaseCluster() {
    return this.hbaseCluster;
  }

  /**
   * @throws IOException
   * @see {@link #startMiniCluster(int)}
   */
  public void shutdownMiniCluster() throws IOException {
    LOG.info("Shutting down minicluster");
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
      // Wait till hbase is down before going on to shutdown zk.
      this.hbaseCluster.join();
    }
    shutdownMiniZKCluster();
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
    }
    // Clean up our directory.
    if (this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
      // Need to use deleteDirectory because File.delete required dir is empty.
      if (!FSUtils.deleteDirectory(FileSystem.getLocal(this.conf),
          new Path(this.clusterTestBuildDir.toString()))) {
        LOG.warn("Failed delete of " + this.clusterTestBuildDir.toString());
      }
    }
    LOG.info("Minicluster is down");
  }

  /**
   * Flushes all caches in the mini hbase cluster
   * @throws IOException
   */
  public void flush() throws IOException {
    this.hbaseCluster.flushcache();
  }


  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[] family)
  throws IOException{
    return createTable(tableName, new byte[][]{family});
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte[] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[] family, int numVersions)
  throws IOException {
    return createTable(tableName, new byte[][]{family}, numVersions);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER,
          HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
      desc.addFamily(hcd);
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      int[] numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    int i = 0;
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions[i],
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER,
          HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
      desc.addFamily(hcd);
      i++;
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Provide an existing table name to truncate
   * @param tableName existing table
   * @return HTable to that new table
   * @throws IOException
   */
  public HTable truncateTable(byte [] tableName) throws IOException {
    HTable table = new HTable(getConfiguration(), tableName);
    Scan scan = new Scan();
    ResultScanner resScan = table.getScanner(scan);
    for(Result res : resScan) {
      Delete del = new Delete(res.getRow());
      table.delete(del);
    }
    return table;
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[] f) throws IOException {
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, null, k);
          t.put(put);
          rowCount++;
        }
      }
    }
    return rowCount;
  }

  /**
   * Return the number of rows in the given table.
   */
  public int countRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (@SuppressWarnings("unused") Result res : results) {
      count++;
    }
    results.close();
    return count;
  }

  
  /**
   * Creates many regions names "aaa" to "zzz".
   *
   * @param table  The table to use for the data.
   * @param columnFamily  The family to insert the data into.
   * @return count of regions created.
   * @throws IOException When creating the regions fails.
   */
  public int createMultiRegions(HTable table, byte[] columnFamily)
  throws IOException {
    return createMultiRegions(getConfiguration(), table, columnFamily);
  }

  /**
   * Creates many regions names "aaa" to "zzz".
   * @param c Configuration to use.
   * @param table  The table to use for the data.
   * @param columnFamily  The family to insert the data into.
   * @return count of regions created.
   * @throws IOException When creating the regions fails.
   */
  public int createMultiRegions(final Configuration c, final HTable table,
      final byte[] columnFamily)
  throws IOException {
    byte[][] KEYS = {
      HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
      Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
      Bytes.toBytes("iii"), Bytes.toBytes("jjj"), Bytes.toBytes("kkk"),
      Bytes.toBytes("lll"), Bytes.toBytes("mmm"), Bytes.toBytes("nnn"),
      Bytes.toBytes("ooo"), Bytes.toBytes("ppp"), Bytes.toBytes("qqq"),
      Bytes.toBytes("rrr"), Bytes.toBytes("sss"), Bytes.toBytes("ttt"),
      Bytes.toBytes("uuu"), Bytes.toBytes("vvv"), Bytes.toBytes("www"),
      Bytes.toBytes("xxx"), Bytes.toBytes("yyy")
    };

    HTable meta = new HTable(c, HConstants.META_TABLE_NAME);
    HTableDescriptor htd = table.getTableDescriptor();
    if(!htd.hasFamily(columnFamily)) {
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
    }
    // remove empty region - this is tricky as the mini cluster during the test
    // setup already has the "<tablename>,,123456789" row with an empty start
    // and end key. Adding the custom regions below adds those blindly,
    // including the new start region from empty to "bbb". lg
    List<byte[]> rows = getMetaTableRows();
    // add custom ones
    int count = 0;
    for (int i = 0; i < KEYS.length; i++) {
      int j = (i + 1) % KEYS.length;
      HRegionInfo hri = new HRegionInfo(table.getTableDescriptor(),
        KEYS[i], KEYS[j]);
      Put put = new Put(hri.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      meta.put(put);
      LOG.info("createMultiRegions: inserted " + hri.toString());
      count++;
    }
    // see comment above, remove "old" (or previous) single region
    for (byte[] row : rows) {
      LOG.info("createMultiRegions: deleting meta row -> " +
        Bytes.toStringBinary(row));
      meta.delete(new Delete(row));
    }
    // flush cache of regions
    HConnection conn = table.getConnection();
    conn.clearRegionCache();
    return count;
  }

  /**
   * Returns all rows from the .META. table.
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows() throws IOException {
    HTable t = new HTable(this.conf, HConstants.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<byte[]>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      LOG.info("getMetaTableRows: row -> " +
        Bytes.toStringBinary(result.getRow()));
      rows.add(result.getRow());
    }
    s.close();
    return rows;
  }

  /**
   * Starts a <code>MiniMRCluster</code> with a default number of
   * <code>TaskTracker</code>'s.
   *
   * @throws IOException When starting the cluster fails.
   */
  public void startMiniMapReduceCluster() throws IOException {
    startMiniMapReduceCluster(2);
  }

  /**
   * Starts a <code>MiniMRCluster</code>.
   *
   * @param servers  The number of <code>TaskTracker</code>'s to start.
   * @throws IOException When starting the cluster fails.
   */
  public void startMiniMapReduceCluster(final int servers) throws IOException {
    LOG.info("Starting mini mapreduce cluster...");
    // These are needed for the new and improved Map/Reduce framework
    Configuration c = getConfiguration();
    System.setProperty("hadoop.log.dir", c.get("hadoop.log.dir"));
    c.set("mapred.output.dir", c.get("hadoop.tmp.dir"));
    mrCluster = new MiniMRCluster(servers,
      FileSystem.get(c).getUri().toString(), 1);
    LOG.info("Mini mapreduce cluster started");
  }

  /**
   * Stops the previously started <code>MiniMRCluster</code>.
   */
  public void shutdownMiniMapReduceCluster() {
    LOG.info("Stopping mini mapreduce cluster...");
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    LOG.info("Mini mapreduce cluster stopped");
  }

  /**
   * Switches the logger for the given class to DEBUG level.
   *
   * @param clazz  The class for which to switch to debug logging.
   */
  public void enableDebug(Class<?> clazz) {
    Log l = LogFactory.getLog(clazz);
    if (l instanceof Log4JLogger) {
      ((Log4JLogger) l).getLogger().setLevel(org.apache.log4j.Level.DEBUG);
    } else if (l instanceof Jdk14Logger) {
      ((Jdk14Logger) l).getLogger().setLevel(java.util.logging.Level.ALL);
    }
  }

  /**
   * Expire the Master's session
   * @throws Exception
   */
  public void expireMasterSession() throws Exception {
    HMaster master = hbaseCluster.getMaster();
    expireSession(master.getZooKeeperWrapper());
  }

  /**
   * Expire a region server's session
   * @param index which RS
   * @throws Exception
   */
  public void expireRegionServerSession(int index) throws Exception {
    HRegionServer rs = hbaseCluster.getRegionServer(index);
    expireSession(rs.getZooKeeperWrapper());
  }

  public void expireSession(ZooKeeperWrapper nodeZK) throws Exception{
    ZooKeeperWrapper zkw = new ZooKeeperWrapper(conf, EmptyWatcher.instance);
    String quorumServers = zkw.getQuorumServers();
    int sessionTimeout = 5 * 1000; // 5 seconds

    byte[] password = nodeZK.getSessionPassword();
    long sessionID = nodeZK.getSessionID();

    ZooKeeper zk = new ZooKeeper(quorumServers,
        sessionTimeout, EmptyWatcher.instance, sessionID, password);
    zk.close();
    final long sleep = sessionTimeout * 5L;
    LOG.info("ZK Closed; sleeping=" + sleep);

    Thread.sleep(sleep);

    new HTable(conf, HConstants.META_TABLE_NAME);
  }

  /**
   * Get the HBase cluster.
   *
   * @return hbase cluster
   */
  public MiniHBaseCluster getHBaseCluster() {
    return hbaseCluster;
  }

  /**
   * Returns a HBaseAdmin instance.
   *
   * @return The HBaseAdmin instance.
   * @throws MasterNotRunningException
   */
  public HBaseAdmin getHBaseAdmin() throws MasterNotRunningException {
    if (hbaseAdmin == null) {
      hbaseAdmin = new HBaseAdmin(getConfiguration());
    }
    return hbaseAdmin;
  }

  /**
   * Closes the named region.
   *
   * @param regionName  The region to close.
   * @throws IOException
   */
  public void closeRegion(String regionName) throws IOException {
    closeRegion(Bytes.toBytes(regionName));
  }

  /**
   * Closes the named region.
   *
   * @param regionName  The region to close.
   * @throws IOException
   */
  public void closeRegion(byte[] regionName) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    admin.closeRegion(regionName, (Object[]) null);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   * @throws IOException
   */
  public void closeRegionByRow(String row, HTable table) throws IOException {
    closeRegionByRow(Bytes.toBytes(row), table);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   * @throws IOException
   */
  public void closeRegionByRow(byte[] row, HTable table) throws IOException {
    HRegionLocation hrl = table.getRegionLocation(row);
    closeRegion(hrl.getRegionInfo().getRegionName());
  }

  public MiniZooKeeperCluster getZkCluster() {
    return zkCluster;
  }

  public void setZkCluster(MiniZooKeeperCluster zkCluster) {
    this.zkCluster = zkCluster;
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.get(conf);
  }
}
