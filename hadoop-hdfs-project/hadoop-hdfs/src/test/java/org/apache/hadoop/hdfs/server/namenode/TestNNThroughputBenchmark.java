/**
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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestNNThroughputBenchmark {

  @BeforeClass
  public static void setUp() {
    ExitUtil.disableSystemExit();
  }

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark}.
   */
  @Test
  public void testNNThroughput() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());
    DFSTestUtil.formatNameNode(conf);
    NNThroughputBenchmark.runBenchmark(conf, new String[] {"-op", "all"});
  }

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark},
   * with explicit local -fs option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputWithFsOption() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());
    DFSTestUtil.formatNameNode(conf);
    NNThroughputBenchmark.runBenchmark(conf,
        new String[] {"-fs", "file:///", "-op", "all"});
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster.
   */
  @Test(timeout = 120000)
  public void testNNThroughputAgainstRemoteNN() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf, new String[]{"-op", "all"});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster with
   * nonSuperUser option (useful when testing any authorization framework e.g.
   * Ranger since only super user e.g. hdfs can enter/exit safemode
   * but any request from super user is not sent for authorization).
   */
  @Test(timeout = 120000)
  public void testNNThroughputAgainstRemoteNNNonSuperUser() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf, new String[]{"-op", "all", "-nonSuperUser"});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * with explicit -fs option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputRemoteAgainstNNWithFsOption() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[]{"-fs", cluster.getURI().toString(), "-op", "all"});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * for append operation.
   */
  @Test(timeout = 120000)
  public void testNNThroughputForAppendOp() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[] {"-op", "create", "-keepResults", "-files", "3",
              "-close" });
      FSNamesystem fsNamesystem = cluster.getNamesystem();
      DirectoryListing listing =
          fsNamesystem.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
      HdfsFileStatus[] partialListing = listing.getPartialListing();

      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[] {"-op", "append", "-files", "3", "-useExisting" });
      listing = fsNamesystem.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
      HdfsFileStatus[] partialListingAfter = listing.getPartialListing();

      Assert.assertEquals(partialListing.length, partialListingAfter.length);
      for (int i = 0; i < partialListing.length; i++) {
        //Check the modification time after append operation
        Assert.assertNotEquals(partialListing[i].getModificationTime(),
            partialListingAfter[i].getModificationTime());
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * for block report operation.
   */
  @Test(timeout = 120000)
  public void testNNThroughputForBlockReportOp() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(3).build()) {
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[]{"-fs", cluster.getURI().toString(), "-op",
              "blockReport", "-datanodes", "3", "-reports", "2"});
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * with explicit -baseDirName option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputWithBaseDir() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      DistributedFileSystem fs = cluster.getFileSystem();

      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[] {"-op", "create", "-keepResults", "-files", "3", "-baseDirName",
              "/nnThroughputBenchmark1", "-close"});
      Assert.assertTrue(fs.exists(new Path("/nnThroughputBenchmark1")));
      Assert.assertFalse(fs.exists(new Path("/nnThroughputBenchmark")));

      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[] {"-op", "all", "-baseDirName", "/nnThroughputBenchmark1"});
      Assert.assertTrue(fs.exists(new Path("/nnThroughputBenchmark1")));
      Assert.assertFalse(fs.exists(new Path("/nnThroughputBenchmark")));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * for blockSize  with letter suffix.
   */
  @Test(timeout = 120000)
  public void testNNThroughputForBlockSizeWithLetterSuffix() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    conf.set(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, "1m");
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
      benchConf.set(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, "1m");
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[]{"-op", "create", "-keepResults", "-files", "3", "-close"});
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * with explicit -blockSize option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputWithBlockSize() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[]{"-op", "create", "-keepResults", "-files", "3",
              "-blockSize", "32", "-close"});
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * with explicit -blockSize option like 1m.
   */
  @Test(timeout = 120000)
  public void testNNThroughputBlockSizeArgWithLetterSuffix() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();
      final Configuration benchConf = new HdfsConfiguration();
      benchConf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf,
          new String[]{"-op", "create", "-keepResults", "-files", "3",
              "-blockSize", "1m", "-close"});
    }
  }

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark}
   * against a mini QJMHA DFS cluster.
   */
  @Test(timeout = 120000)
  public void testNNThroughputWithHA() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
    String baseDir = GenericTestUtils.getRandomizedTempPath();
    MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
    builder.getDfsBuilder().numDataNodes(3);
    MiniQJMHACluster qjmhaCluster = builder.baseDir(baseDir).build();
    MiniDFSCluster cluster = qjmhaCluster.getDfsCluster();
    cluster.waitActive();
    cluster.transitionToActive(0);

    String nsId = "ns1";
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://" + nsId);
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsId);
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, nsId);
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, nsId), "nn1,nn2");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, "nn1"),
        cluster.getNameNode(0).getHostAndPort());
    conf.set(DFSUtil.addKeySuffixes(
            DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, "nn2"),
        cluster.getNameNode(1).getHostAndPort());

    // Reduce the number of retries to speed up the tests.
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
        500);
    conf.setInt(HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY, 2);
    conf.setInt(HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY, 2);
    conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY, 0);
    conf.setInt(HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY, 0);

    assertTrue(HAUtil.isHAEnabled(conf, "ns1"));

    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 16);
    NNThroughputBenchmark.runBenchmark(conf, new String[] {"-op", "all"});
  }
}
