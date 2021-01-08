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
package org.apache.hadoop.hdfs.server.mover;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MOVER_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MOVER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MOVER_KEYTAB_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MOVER_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.balancer.TestBalancer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

public class TestMover {
  private static final Logger LOG = LoggerFactory.getLogger(TestMover.class);
  private static final int DEFAULT_BLOCK_SIZE = 100;
  private File keytabFile;
  private String principal;

  static {
    TestBalancer.initTestSetup();
  }

  static void initConf(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 2000L);
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
  }

  static Mover newMover(Configuration conf) throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(1, namenodes.size());
    Map<URI, List<Path>> nnMap = Maps.newHashMap();
    for (URI nn : namenodes) {
      nnMap.put(nn, null);
    }

    final List<NameNodeConnector> nncs = NameNodeConnector.
        newNameNodeConnectors(nnMap, Mover.class.getSimpleName(),
            HdfsServerConstants.MOVER_ID_PATH, conf,
            NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
    return new Mover(nncs.get(0), conf, new AtomicInteger(0), new HashMap<>());
  }

  @Test
  public void testScheduleSameBlock() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleSameBlock/file";
      
      {
        final FSDataOutputStream out = dfs.create(new Path(file));
        out.writeChars("testScheduleSameBlock");
        out.close();
      }

      final Mover mover = newMover(conf);
      mover.init();
      final Mover.Processor processor = mover.new Processor();

      final LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      final List<MLocation> locations = MLocation.toLocations(lb);
      final MLocation ml = locations.get(0);
      final DBlock db = mover.newDBlock(lb, locations, null);

      final List<StorageType> storageTypes = new ArrayList<StorageType>(
          Arrays.asList(StorageType.DEFAULT, StorageType.DEFAULT));
      Assert.assertTrue(processor.scheduleMoveReplica(db, ml, storageTypes));
      Assert.assertFalse(processor.scheduleMoveReplica(db, ml, storageTypes));
    } finally {
      cluster.shutdown();
    }
  }

  private void testWithinSameNode(Configuration conf) throws Exception {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testScheduleWithinSameNode/file";
      Path dir = new Path("/testScheduleWithinSameNode");
      dfs.mkdirs(dir);
      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testScheduleWithinSameNode");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(dir, "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", dir.toString()});
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 3);
    } finally {
      cluster.shutdown();
    }
  }

  private void setupStoragePoliciesAndPaths(DistributedFileSystem dfs1,
                                           DistributedFileSystem dfs2,
                                            Path dir, String file)
      throws Exception {

    dfs1.mkdirs(dir);
    dfs2.mkdirs(dir);

    //Write to DISK on nn1
    dfs1.setStoragePolicy(dir, "HOT");
    FSDataOutputStream out = dfs1.create(new Path(file));
    out.writeChars("testScheduleWithinSameNode");
    out.close();

    //Write to Archive on nn2
    dfs2.setStoragePolicy(dir, "COLD");
    out = dfs2.create(new Path(file));
    out.writeChars("testScheduleWithinSameNode");
    out.close();

    //verify before movement
    LocatedBlock lb = dfs1.getClient().getLocatedBlocks(file, 0).get(0);
    StorageType[] storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.DISK == storageType);
    }

    //verify before movement
    lb = dfs2.getClient().getLocatedBlocks(file, 0).get(0);
    storageTypes = lb.getStorageTypes();
    for (StorageType storageType : storageTypes) {
      Assert.assertTrue(StorageType.ARCHIVE == storageType);
    }
  }

  private void waitForLocatedBlockWithDiskStorageType(
      final DistributedFileSystem dfs, final String file,
      int expectedDiskCount) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int diskCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.DISK == storageType) {
            diskCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
            expectedDiskCount, diskCount);
        return expectedDiskCount == diskCount;
      }
    }, 100, 3000);
  }

  @Test(timeout = 300000)
  public void testWithFederateClusterWithinSameNode() throws
      Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(4).storageTypes( new StorageType[] {StorageType.DISK,
            StorageType.ARCHIVE}).nnTopology(MiniDFSNNTopology
            .simpleFederatedTopology(2)).build();
    DFSTestUtil.setFederatedConfiguration(cluster, conf);

    try {
      cluster.waitActive();

      final String file = "/test/file";
      Path dir = new Path ("/test");

      final DistributedFileSystem dfs1 = cluster.getFileSystem(0);
      final DistributedFileSystem dfs2 = cluster.getFileSystem(1);

      URI nn1 = dfs1.getUri();
      URI nn2 = dfs2.getUri();

      setupStoragePoliciesAndPaths(dfs1, dfs2, dir, file);


      // move to ARCHIVE
      dfs1.setStoragePolicy(dir, "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", nn1 + dir.toString()});
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);


      //move to DISK
      dfs2.setStoragePolicy(dir, "HOT");
      rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", nn2 + dir.toString()});
      Assert.assertEquals("Movement to DISK should be successful", 0, rc);


      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs1, file, 3);
      waitForLocatedBlockWithDiskStorageType(dfs2, file, 3);

    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testWithFederatedCluster() throws Exception{

    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .storageTypes(new StorageType[]{StorageType.DISK,
            StorageType.ARCHIVE})
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .numDataNodes(4).build();
    DFSTestUtil.setFederatedConfiguration(cluster, conf);
    try {
      cluster.waitActive();

      final String file = "/test/file";
      Path dir = new Path ("/test");

      final DistributedFileSystem dfs1 = cluster.getFileSystem(0);
      final DistributedFileSystem dfs2 = cluster.getFileSystem(1);

      URI nn1 = dfs1.getUri();
      URI nn2 = dfs2.getUri();

      setupStoragePoliciesAndPaths(dfs1, dfs2, dir, file);

      //Changing storage policies
      dfs1.setStoragePolicy(dir, "COLD");
      dfs2.setStoragePolicy(dir, "HOT");

      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", nn1 + dir.toString(), nn2 + dir.toString()});

      Assert.assertEquals("Movement to DISK should be successful", 0, rc);

      waitForLocatedBlockWithArchiveStorageType(dfs1, file, 3);
      waitForLocatedBlockWithDiskStorageType(dfs2, file, 3);

    } finally {
      cluster.shutdown();
    }

  }

  @Test(timeout = 300000)
  public void testWithFederatedHACluster() throws Exception{

    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .storageTypes(new StorageType[]{StorageType.DISK,
            StorageType.ARCHIVE})
        .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(2))
        .numDataNodes(4).build();
    DFSTestUtil.setFederatedHAConfiguration(cluster, conf);


    try {

      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();

      cluster.transitionToActive(0);
      cluster.transitionToActive(2);

      final String file = "/test/file";
      Path dir = new Path ("/test");

      final DistributedFileSystem dfs1  = (DistributedFileSystem) FileSystem
          .get(nn1, conf);

      final DistributedFileSystem dfs2  = (DistributedFileSystem) FileSystem
          .get(nn2, conf);

     setupStoragePoliciesAndPaths(dfs1, dfs2, dir, file);

      //Changing Storage Policies
      dfs1.setStoragePolicy(dir, "COLD");
      dfs2.setStoragePolicy(dir, "HOT");

      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", nn1 + dir.toString(), nn2 + dir.toString()});

      Assert.assertEquals("Movement to DISK should be successful", 0, rc);

      waitForLocatedBlockWithArchiveStorageType(dfs1, file, 3);
      waitForLocatedBlockWithDiskStorageType(dfs2, file, 3);
    } finally {
      cluster.shutdown();
    }

  }

  private void waitForLocatedBlockWithArchiveStorageType(
      final DistributedFileSystem dfs, final String file,
      int expectedArchiveCount) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlock lb = null;
        try {
          lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int archiveCount = 0;
        for (StorageType storageType : lb.getStorageTypes()) {
          if (StorageType.ARCHIVE == storageType) {
            archiveCount++;
          }
        }
        LOG.info("Archive replica count, expected={} and actual={}",
            expectedArchiveCount, archiveCount);
        return expectedArchiveCount == archiveCount;
      }
    }, 100, 3000);
  }

  @Test
  public void testScheduleBlockWithinSameNode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    testWithinSameNode(conf);
  }

  private void checkMovePaths(List<Path> actual, Path... expected) {
    Assert.assertEquals(expected.length, actual.size());
    for (Path p : expected) {
      Assert.assertTrue(actual.contains(p));
    }
  }

  /**
   * Test Mover Cli by specifying a list of files/directories using option "-p".
   * There is only one namenode (and hence name service) specified in the conf.
   */
  @Test
  public void testMoverCli() throws Exception {
    final Configuration clusterConf = new HdfsConfiguration();
    clusterConf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(clusterConf).numDataNodes(0).build();
    try {
      final Configuration conf = cluster.getConfiguration(0);
      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "bar");
        Assert.fail("Expected exception for illegal path bar");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("bar is not absolute", e);
      }

      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf);
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      Assert.assertNull(movePaths.get(nn));

      movePaths = Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo", "/bar");
      namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, movePaths.size());
      nn = namenodes.iterator().next();
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithHAConf() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0).build();
    HATestUtil.setFailoverConfigurations(cluster, conf, "MyCluster");
    try {
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", "/foo", "/bar");
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(1, namenodes.size());
      Assert.assertEquals(1, movePaths.size());
      URI nn = namenodes.iterator().next();
      Assert.assertEquals(new URI("hdfs://MyCluster"), nn);
      Assert.assertTrue(movePaths.containsKey(nn));
      checkMovePaths(movePaths.get(nn), new Path("/foo"), new Path("/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederation() throws Exception {
    final Configuration clusterConf = new HdfsConfiguration();
    clusterConf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(clusterConf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(3))
        .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    clusterConf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    DFSTestUtil.setFederatedConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "/foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
            "does not contain scheme and authority", e);
      }

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "hdfs:///foo");
        Assert.fail("Expect exception for missing authority information");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains(
            "does not contain scheme and authority", e);
      }

      try {
        Mover.Cli.getNameNodePathsToMove(conf, "-p", "wrong-hdfs://ns1/foo");
        Assert.fail("Expect exception for wrong scheme");
      } catch (IllegalArgumentException e) {
        GenericTestUtils.assertExceptionContains("Cannot resolve the path", e);
      }

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar");
      Assert.assertEquals(2, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverCliWithFederationHA() throws Exception {
    final Configuration clusterConf = new HdfsConfiguration();
    clusterConf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(clusterConf)
        .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(3))
        .numDataNodes(0).build();
    final Configuration conf = new HdfsConfiguration();
    clusterConf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    DFSTestUtil.setFederatedHAConfiguration(cluster, conf);
    try {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      Assert.assertEquals(3, namenodes.size());

      Iterator<URI> iter = namenodes.iterator();
      URI nn1 = iter.next();
      URI nn2 = iter.next();
      URI nn3 = iter.next();
      Map<URI, List<Path>> movePaths = Mover.Cli.getNameNodePathsToMove(conf,
          "-p", nn1 + "/foo", nn1 + "/bar", nn2 + "/foo/bar", nn3 + "/foobar");
      Assert.assertEquals(3, movePaths.size());
      checkMovePaths(movePaths.get(nn1), new Path("/foo"), new Path("/bar"));
      checkMovePaths(movePaths.get(nn2), new Path("/foo/bar"));
      checkMovePaths(movePaths.get(nn3), new Path("/foobar"));
    } finally {
       cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testTwoReplicaSameStorageTypeShouldNotSelect() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK, StorageType.ARCHIVE },
                { StorageType.DISK, StorageType.DISK },
                { StorageType.DISK, StorageType.ARCHIVE } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testForTwoReplicaSameStorageTypeShouldNotSelect";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testForTwoReplicaSameStorageTypeShouldNotSelect");
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 2);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testMoveWhenStoragePolicyNotSatisfying() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] { { StorageType.DISK }, { StorageType.DISK },
                { StorageType.DISK } }).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveWhenStoragePolicyNotSatisfying";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testMoveWhenStoragePolicyNotSatisfying");
      out.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", file.toString() });
      int exitcode = ExitStatus.NO_MOVE_BLOCK.getExitCode();
      Assert.assertEquals("Exit code should be " + exitcode, exitcode, rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testMoveWhenStoragePolicySatisfierIsRunning() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK}, {StorageType.DISK},
                {StorageType.DISK}}).build();
    try {
      cluster.waitActive();
      // Simulate External sps by creating #getNameNodeConnector instance.
      DFSTestUtil.getNameNodeConnector(conf, HdfsServerConstants.MOVER_ID_PATH,
          1, true);
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoveWhenStoragePolicySatisfierIsRunning";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file));
      out.writeChars("testMoveWhenStoragePolicySatisfierIsRunning");
      out.close();

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      int exitcode = ExitStatus.IO_EXCEPTION.getExitCode();
      Assert.assertEquals("Exit code should be " + exitcode, exitcode, rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMoverFailedRetry() throws Exception {
    // HDFS-8147
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.set(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, "2");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverFailedRetry";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file), (short) 2);
      out.writeChars("testMoverFailedRetry");
      out.close();

      // Delete block file so, block move will fail with FileNotFoundException
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(lb.getBlock());
      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement should fail after some retry",
          ExitStatus.NO_MOVE_PROGRESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=100000)
  public void testBalancerMaxIterationTimeNotAffectMover() throws Exception {
    long blockSize = 10*1024*1024;
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.setInt(DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY, 1);
    conf.setInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 1);
    // set a fairly large block size to run into the limitation
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, blockSize);
    // set a somewhat grater than zero max iteration time to have the move time
    // to surely exceed it
    conf.setLong(DFSConfigKeys.DFS_BALANCER_MAX_ITERATION_TIME_KEY, 200L);
    conf.setInt(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, 1);
    // set client socket timeout to have an IN_PROGRESS notification back from
    // the DataNode about the copy in every second.
    conf.setLong(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 1000L);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.DISK},
                {StorageType.ARCHIVE, StorageType.ARCHIVE}})
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();
      final String file = "/testMaxIterationTime.dat";
      final Path path = new Path(file);
      short rep_factor = 1;
      int seed = 0xFAFAFA;
      // write to DISK
      DFSTestUtil.createFile(fs, path, 4L * blockSize, rep_factor, seed);

      // move to ARCHIVE
      fs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file});
      Assert.assertEquals("Retcode expected to be ExitStatus.SUCCESS (0).",
          ExitStatus.SUCCESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 4;
  private final int defaultBlockSize = cellSize * stripesPerBlock;

  void initConfWithStripe(Configuration conf) {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
  }

  @Test(timeout = 300000)
  public void testMoverWithStripedFile() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConfWithStripe(conf);

    // start 10 datanodes
    int numOfDatanodes =10;
    int storagesPerDatanode=2;
    long capacity = 10 * defaultBlockSize;
    long[][] capacities = new long[numOfDatanodes][storagesPerDatanode];
    for (int i = 0; i < numOfDatanodes; i++) {
      for(int j=0;j<storagesPerDatanode;j++){
        capacities[i][j]=capacity;
      }
    }
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageTypes(new StorageType[][]{
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.DISK},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE},
            {StorageType.DISK, StorageType.ARCHIVE}})
        .storageCapacities(capacities)
        .build();

    try {
      cluster.waitActive();
      cluster.getFileSystem().enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // set "/bar" directory with HOT storage policy.
      ClientProtocol client = NameNodeProxies.createProxy(conf,
          cluster.getFileSystem(0).getUri(), ClientProtocol.class).getProxy();
      String barDir = "/bar";
      client.mkdirs(barDir, new FsPermission((short) 777), true);
      client.setStoragePolicy(barDir,
          HdfsConstants.HOT_STORAGE_POLICY_NAME);
      // set an EC policy on "/bar" directory
      client.setErasureCodingPolicy(barDir,
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // write file to barDir
      final String fooFile = "/bar/foo";
      long fileLen = 20 * defaultBlockSize;
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fooFile),
          fileLen,(short) 3, 0);

      // verify storage types and locations
      LocatedBlocks locatedBlocks =
          client.getBlockLocations(fooFile, 0, fileLen);
      for(LocatedBlock lb : locatedBlocks.getLocatedBlocks()){
        for( StorageType type : lb.getStorageTypes()){
          Assert.assertEquals(StorageType.DISK, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 5 more datanodes
      numOfDatanodes +=5;
      capacities = new long[5][storagesPerDatanode];
      for (int i = 0; i < 5; i++) {
        for(int j=0;j<storagesPerDatanode;j++){
          capacities[i][j]=capacity;
        }
      }
      cluster.startDataNodes(conf, 5,
          new StorageType[][]{
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE},
              {StorageType.ARCHIVE, StorageType.ARCHIVE}},
          true, null, null, null,capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // move file to ARCHIVE
      client.setStoragePolicy(barDir, "COLD");
      // run Mover
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] { "-p", barDir });
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // verify storage types and locations
      locatedBlocks = client.getBlockLocations(fooFile, 0, fileLen);
      for(LocatedBlock lb : locatedBlocks.getLocatedBlocks()){
        for( StorageType type : lb.getStorageTypes()){
          Assert.assertEquals(StorageType.ARCHIVE, type);
        }
      }
      StripedFileTestUtil.verifyLocatedStripedBlocks(locatedBlocks,
          dataBlocks + parityBlocks);

      // start 5 more datanodes
      numOfDatanodes += 5;
      capacities = new long[5][storagesPerDatanode];
      for (int i = 0; i < 5; i++) {
        for (int j = 0; j < storagesPerDatanode; j++) {
          capacities[i][j] = capacity;
        }
      }
      cluster.startDataNodes(conf, 5,
          new StorageType[][] { { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK },
              { StorageType.SSD, StorageType.DISK } },
          true, null, null, null, capacities, null, false, false, false, null);
      cluster.triggerHeartbeats();

      // move file blocks to ONE_SSD policy
      client.setStoragePolicy(barDir, "ONE_SSD");

      // run Mover
      rc = ToolRunner.run(conf, new Mover.Cli(), new String[] { "-p", barDir });

      // verify storage types and locations
      // Movements should have been ignored for the unsupported policy on
      // striped file
      locatedBlocks = client.getBlockLocations(fooFile, 0, fileLen);
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (StorageType type : lb.getStorageTypes()) {
          Assert.assertEquals(StorageType.ARCHIVE, type);
        }
      }
    }finally{
      cluster.shutdown();
    }
  }

  private void initSecureConf(Configuration conf) throws Exception {
    String username = "mover";
    File baseDir = GenericTestUtils.getTestDir(TestMover.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    Assert.assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    MiniKdc kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    KerberosName.resetDefaultRealm();
    Assert.assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    keytabFile = new File(baseDir, username + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    principal = username + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();
    kdc.createPrincipal(keytabFile, username, username + "/" + krbInstance,
        "HTTP/" + krbInstance);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    conf.setBoolean(DFS_MOVER_KEYTAB_ENABLED_KEY, true);
    conf.set(DFS_MOVER_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_MOVER_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_MOVER_KERBEROS_PRINCIPAL_KEY, principal);

    String keystoresDir = baseDir.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestMover.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);

    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
    initConf(conf);
  }

  /**
   * Test Mover runs fine when logging in with a keytab in kerberized env.
   * Reusing testWithinSameNode here for basic functionality testing.
   */
  @Test(timeout = 300000)
  public void testMoverWithKeytabs() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    try {
      initSecureConf(conf);
      final UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal,
              keytabFile.getAbsolutePath());
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          // verify that mover runs Ok.
          testWithinSameNode(conf);
          // verify that UGI was logged in using keytab.
          Assert.assertTrue(UserGroupInformation.isLoginKeytabBased());
          return null;
        }
      });
    } finally {
      // Reset UGI so that other tests are not affected.
      UserGroupInformation.reset();
      UserGroupInformation.setConfiguration(new Configuration());
    }
  }

  /**
   * Test to verify that mover can't move pinned blocks.
   */
  @Test(timeout = 90000)
  public void testMoverWithPinnedBlocks() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);

    // Sets bigger retry max attempts value so that test case will timed out if
    // block pinning errors are not handled properly during block movement.
    conf.setInt(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, 10000);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverWithPinnedBlocks/file";
      Path dir = new Path("/testMoverWithPinnedBlocks");
      dfs.mkdirs(dir);

      // write to DISK
      dfs.setStoragePolicy(dir, "HOT");
      final FSDataOutputStream out = dfs.create(new Path(file));
      byte[] fileData = StripedFileTestUtil
          .generateBytes(DEFAULT_BLOCK_SIZE * 3);
      out.write(fileData);
      out.close();

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }

      // Adding one SSD based data node to the cluster.
      StorageType[][] newtypes = new StorageType[][] {{StorageType.SSD}};
      startAdditionalDNs(conf, 1, newtypes, cluster);

      // Mock FsDatasetSpi#getPinning to show that the block is pinned.
      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        DataNode dn = cluster.getDataNodes().get(i);
        LOG.info("Simulate block pinning in datanode {}", dn);
        InternalDataNodeTestUtils.mockDatanodeBlkPinning(dn, true);
      }

      // move file blocks to ONE_SSD policy
      dfs.setStoragePolicy(dir, "ONE_SSD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", dir.toString()});

      int exitcode = ExitStatus.NO_MOVE_BLOCK.getExitCode();
      Assert.assertEquals("Movement should fail", exitcode, rc);

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to verify that mover should work well with pinned blocks as well as
   * failed blocks. Mover should continue retrying the failed blocks only.
   */
  @Test(timeout = 90000)
  public void testMoverFailedRetryWithPinnedBlocks() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    conf.set(DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY, "2");
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE},
                {StorageType.DISK, StorageType.ARCHIVE}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String parenDir = "/parent";
      dfs.mkdirs(new Path(parenDir));
      final String file1 = "/parent/testMoverFailedRetryWithPinnedBlocks1";
      // write to DISK
      final FSDataOutputStream out = dfs.create(new Path(file1), (short) 2);
      byte[] fileData = StripedFileTestUtil
          .generateBytes(DEFAULT_BLOCK_SIZE * 2);
      out.write(fileData);
      out.close();

      // Adding pinned blocks.
      createFileWithFavoredDatanodes(conf, cluster, dfs);

      // Delete block file so, block move will fail with FileNotFoundException
      LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(file1, 0);
      Assert.assertEquals("Wrong block count", 2,
          locatedBlocks.locatedBlockCount());
      LocatedBlock lb = locatedBlocks.get(0);
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(lb.getBlock());

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(parenDir), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", parenDir.toString()});
      Assert.assertEquals("Movement should fail after some retry",
          ExitStatus.NO_MOVE_PROGRESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300000)
  public void testMoverWhenStoragePolicyUnset() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    initConf(conf);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .storageTypes(
            new StorageType[][] {{StorageType.DISK, StorageType.ARCHIVE}})
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final String file = "/testMoverWhenStoragePolicyUnset";
      // write to DISK
      DFSTestUtil.createFile(dfs, new Path(file), 1L, (short) 1, 0L);

      // move to ARCHIVE
      dfs.setStoragePolicy(new Path(file), "COLD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement to ARCHIVE should be successful", 0, rc);

      // Wait till namenode notified about the block location details
      waitForLocatedBlockWithArchiveStorageType(dfs, file, 1);

      // verify before unset policy
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      Assert.assertTrue(StorageType.ARCHIVE == (lb.getStorageTypes())[0]);

      // unset storage policy
      dfs.unsetStoragePolicy(new Path(file));
      rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement to DISK should be successful", 0, rc);

      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      Assert.assertTrue(StorageType.DISK == (lb.getStorageTypes())[0]);
    } finally {
      cluster.shutdown();
    }
  }

  private void createFileWithFavoredDatanodes(final Configuration conf,
      final MiniDFSCluster cluster, final DistributedFileSystem dfs)
          throws IOException {
    // Adding two DISK based data node to the cluster.
    // Also, ensure that blocks are pinned in these new data nodes.
    StorageType[][] newtypes =
        new StorageType[][] {{StorageType.DISK}, {StorageType.DISK}};
    startAdditionalDNs(conf, 2, newtypes, cluster);
    ArrayList<DataNode> dataNodes = cluster.getDataNodes();
    InetSocketAddress[] favoredNodes = new InetSocketAddress[2];
    int j = 0;
    for (int i = dataNodes.size() - 1; i >= 2; i--) {
      favoredNodes[j++] = dataNodes.get(i).getXferAddress();
    }
    final String file = "/parent/testMoverFailedRetryWithPinnedBlocks2";
    final FSDataOutputStream out = dfs.create(new Path(file),
        FsPermission.getDefault(), true, DEFAULT_BLOCK_SIZE, (short) 2,
        DEFAULT_BLOCK_SIZE, null, favoredNodes);
    byte[] fileData = StripedFileTestUtil.generateBytes(DEFAULT_BLOCK_SIZE * 2);
    out.write(fileData);
    out.close();

    // Mock FsDatasetSpi#getPinning to show that the block is pinned.
    LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(file, 0);
    Assert.assertEquals("Wrong block count", 2,
        locatedBlocks.locatedBlockCount());
    LocatedBlock lb = locatedBlocks.get(0);
    DatanodeInfo datanodeInfo = lb.getLocations()[0];
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getDatanodeUuid()
          .equals(datanodeInfo.getDatanodeUuid())) {
        LOG.info("Simulate block pinning in datanode {}", datanodeInfo);
        InternalDataNodeTestUtils.mockDatanodeBlkPinning(dn, true);
        break;
      }
    }
  }

  private void startAdditionalDNs(final Configuration conf,
      int newNodesRequired, StorageType[][] newTypes,
      final MiniDFSCluster cluster) throws IOException {

    cluster.startDataNodes(conf, newNodesRequired, newTypes, true, null, null,
        null, null, null, false, false, false, null);
    cluster.triggerHeartbeats();
  }
}
