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
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Facility for testing HBase. Added as tool to abet junit4 testing.  Replaces
 * old HBaseTestCase and HBaseCluserTestCase functionality.
 * Create an instance and keep it around doing HBase testing.  This class is
 * meant to be your one-stop shop for anything you mind need testing.  Manages
 * one cluster at a time only.  Depends on log4j on classpath and hbase-site.xml
 * for logging and test-run configuration.  It does not set logging levels nor
 * make changes to configuration parameters.
 */
public class HBaseTestingUtility {
  private final Log LOG = LogFactory.getLog(getClass());
  private final HBaseConfiguration conf = new HBaseConfiguration();
  private MiniZooKeeperCluster zkCluster = null;
  private MiniDFSCluster dfsCluster = null;
  private MiniHBaseCluster hbaseCluster = null;
  private File clusterTestBuildDir = null;

  /** System property key to get test directory value.
   */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";

  /**
   * @return Instance of HBaseConfiguration.
   */
  public HBaseConfiguration getConfiguration() {
    return this.conf;
  }

  /**
   * @return Where to write test data on local filesystem; usually build/test/data
   */
  public Path getTestDir() {
    return new Path(System.getProperty(TEST_DIRECTORY_KEY, "build/test/data"));
  }

  /**
   * @param subdirName
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getTestDir()}.
   */
  public Path getTestDir(final String subdirName) {
    return new Path(getTestDir(), subdirName);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * @throws Exception 
   */
  public void startMiniCluster() throws Exception {
    startMiniCluster(1);
  }

  /**
   * Start up a minicluster of hbase, optinally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * @param servers Number of servers to start up.  We'll start this many
   * datanodes and regionservers.  If servers is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   */
  public void startMiniCluster(final int servers)
  throws Exception {
    LOG.info("Starting up minicluster");
    // If we already put up a cluster, fail.
    if (this.clusterTestBuildDir != null) {
      throw new IOException("Cluster already running at " +
        this.clusterTestBuildDir);
    }
    // Now, home our cluster in a dir under build/test.  Give it a random name
    // so can have many concurrent clusters running if we need to.  Need to
    // amend the test.build.data System property.  Its what minidfscluster bases
    // it data dir on.  Moding a System property is not the way to do concurrent
    // instances -- another instance could grab the temporary
    // value unintentionally -- but not anything can do about it at moment; its
    // how the minidfscluster works.
    String oldTestBuildDir =
      System.getProperty(TEST_DIRECTORY_KEY, "build/test/data");
    String randomStr = UUID.randomUUID().toString();
    String clusterTestBuildDirStr = oldTestBuildDir + "." + randomStr;
    this.clusterTestBuildDir =
      new File(clusterTestBuildDirStr).getAbsoluteFile();
    // Have it cleaned up on exit
    this.clusterTestBuildDir.deleteOnExit();
    // Set our random dir while minidfscluster is being constructed.
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDirStr);
    // Bring up mini dfs cluster. This spews a bunch of warnings about missing
    // scheme. TODO: fix.
    // Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true,
      true, true, null, null, null, null);
    // Restore System property. minidfscluster accesses content of
    // the TEST_DIRECTORY_KEY to make bad blocks, a feature we are not using,
    // but otherwise, just in constructor.
    System.setProperty(TEST_DIRECTORY_KEY, oldTestBuildDir);
    // Mangle conf so fs parameter points to minidfs we just started up
    FileSystem fs = this.dfsCluster.getFileSystem();
    this.conf.set("fs.defaultFS", fs.getUri().toString());
    this.dfsCluster.waitClusterUp();

    // Note that this is done before we create the MiniHBaseCluster because we
    // need to edit the config to add the ZooKeeper servers.
    this.zkCluster = new MiniZooKeeperCluster();
    int clientPort = this.zkCluster.startup(this.clusterTestBuildDir);
    this.conf.set("hbase.zookeeper.property.clientPort",
      Integer.toString(clientPort));

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
  }

  /**
   * @throws IOException
   * @see {@link #startMiniCluster(boolean, int)}
   */
  public void shutdownMiniCluster() throws IOException {
    LOG.info("Shutting down minicluster");
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
      // Wait till hbase is down before going on to shutdown zk.
      this.hbaseCluster.join();
    }
    if (this.zkCluster != null) this.zkCluster.shutdown();
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
   * Flusheds all caches in the mini hbase cluster
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
  public HTable createTable(byte [] tableName, byte [] family) 
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
  public HTable createTable(byte [] tableName, byte [][] families) 
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte [] family : families) {
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
  public HTable createTable(byte [] tableName, byte [] family, int numVersions)
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
  public HTable createTable(byte [] tableName, byte [][] families,
      int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL, false);
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
  public HTable createTable(byte [] tableName, byte [][] families,
      int [] numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    int i = 0;
    for (byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family, numVersions[i],
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          Integer.MAX_VALUE, HColumnDescriptor.DEFAULT_TTL, false);
      desc.addFamily(hcd);
      i++;
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte [] f) throws IOException {
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 < 'z'; b1++) {
      for (byte b2 = 'a'; b2 < 'z'; b2++) {
        for (byte b3 = 'a'; b3 < 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, new byte[0], k);
          t.put(put);
          rowCount++;
        }
      }
    }
    return rowCount;
  }
}