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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verify open files listing.
 */
public class TestListOpenFiles {
  private static final int NUM_DATA_NODES = 3;
  private static final int BATCH_SIZE = 5;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem fs = null;
  private static NamenodeProtocols nnRpc = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestListOpenFiles.class);

  @Before
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, BATCH_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    nnRpc = cluster.getNameNodeRpc();
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120000L)
  public void testListOpenFilesViaNameNodeRPC() throws Exception {
    HashMap<Path, FSDataOutputStream> openFiles = new HashMap<>();
    createFiles(fs, "closed", 10);
    verifyOpenFiles(openFiles);

    BatchedEntries<OpenFileEntry> openFileEntryBatchedEntries =
        nnRpc.listOpenFiles(0, EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
            OpenFilesIterator.FILTER_PATH_DEFAULT);
    assertTrue("Open files list should be empty!",
        openFileEntryBatchedEntries.size() == 0);
    BatchedEntries<OpenFileEntry> openFilesBlockingDecomEntries =
        nnRpc.listOpenFiles(0, EnumSet.of(OpenFilesType.BLOCKING_DECOMMISSION),
            OpenFilesIterator.FILTER_PATH_DEFAULT);
    assertTrue("Open files list blocking decommission should be empty!",
        openFilesBlockingDecomEntries.size() == 0);

    openFiles.putAll(
        DFSTestUtil.createOpenFiles(fs, "open-1", 1));
    verifyOpenFiles(openFiles);

    openFiles.putAll(
        DFSTestUtil.createOpenFiles(fs, "open-2",
        (BATCH_SIZE * 2 + BATCH_SIZE / 2)));
    verifyOpenFiles(openFiles);

    DFSTestUtil.closeOpenFiles(openFiles, openFiles.size() / 2);
    verifyOpenFiles(openFiles);

    openFiles.putAll(
        DFSTestUtil.createOpenFiles(fs, "open-3", (BATCH_SIZE * 5)));
    verifyOpenFiles(openFiles);

    while(openFiles.size() > 0) {
      DFSTestUtil.closeOpenFiles(openFiles, 1);
      verifyOpenFiles(openFiles);
    }
  }

  private void verifyOpenFiles(Map<Path, FSDataOutputStream> openFiles,
      EnumSet<OpenFilesType> openFilesTypes, String path) throws IOException {
    HashSet<Path> remainingFiles = new HashSet<>(openFiles.keySet());
    OpenFileEntry lastEntry = null;
    BatchedEntries<OpenFileEntry> batchedEntries;
    do {
      if (lastEntry == null) {
        batchedEntries = nnRpc.listOpenFiles(0, openFilesTypes, path);
      } else {
        batchedEntries = nnRpc.listOpenFiles(lastEntry.getId(),
            openFilesTypes, path);
      }
      assertTrue("Incorrect open files list size!",
          batchedEntries.size() <= BATCH_SIZE);
      for (int i = 0; i < batchedEntries.size(); i++) {
        lastEntry = batchedEntries.get(i);
        String filePath = lastEntry.getFilePath();
        LOG.info("OpenFile: " + filePath);
        assertTrue("Unexpected open file: " + filePath,
            remainingFiles.remove(new Path(filePath)));
      }
    } while (batchedEntries.hasMore());
    assertTrue(remainingFiles.size() + " open files not listed!",
        remainingFiles.size() == 0);
  }

  /**
   * Verify all open files.
   */
  private void verifyOpenFiles(Map<Path, FSDataOutputStream> openFiles)
      throws IOException {
    verifyOpenFiles(openFiles, OpenFilesIterator.FILTER_PATH_DEFAULT);
  }

  /**
   * Verify open files with specified filter path.
   */
  private void verifyOpenFiles(Map<Path, FSDataOutputStream> openFiles,
      String path) throws IOException {
    verifyOpenFiles(openFiles, EnumSet.of(OpenFilesType.ALL_OPEN_FILES), path);
    verifyOpenFiles(new HashMap<>(),
        EnumSet.of(OpenFilesType.BLOCKING_DECOMMISSION), path);
  }

  private Set<Path> createFiles(FileSystem fileSystem, String fileNamePrefix,
      int numFilesToCreate) throws IOException {
    HashSet<Path> files = new HashSet<>();
    for (int i = 0; i < numFilesToCreate; i++) {
      Path filePath = new Path(fileNamePrefix + "-" + i);
      DFSTestUtil.createFile(fileSystem, filePath, 1024, (short) 3, 1);
    }
    return files;
  }

  /**
   * Verify dfsadmin -listOpenFiles command in HA mode.
   */
  @Test(timeout = 120000)
  public void testListOpenFilesInHA() throws Exception {
    fs.close();
    cluster.shutdown();
    HdfsConfiguration haConf = new HdfsConfiguration();
    haConf.setLong(
        DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, BATCH_SIZE);
    MiniDFSCluster haCluster =
        new MiniDFSCluster.Builder(haConf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(0)
        .build();
    try {
      HATestUtil.setFailoverConfigurations(haCluster, haConf);
      FileSystem fileSystem = HATestUtil.configureFailoverFs(haCluster, haConf);

      List<ClientProtocol> namenodes =
          HAUtil.getProxiesForAllNameNodesInNameservice(haConf,
              HATestUtil.getLogicalHostname(haCluster));
      haCluster.transitionToActive(0);
      assertTrue(HAUtil.isAtLeastOneActive(namenodes));

      final byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      DFSTestUtil.createOpenFiles(fileSystem, "ha-open-file",
          ((BATCH_SIZE * 4) + (BATCH_SIZE / 2)));

      final DFSAdmin dfsAdmin = new DFSAdmin(haConf);
      final AtomicBoolean failoverCompleted = new AtomicBoolean(false);
      final AtomicBoolean listOpenFilesError = new AtomicBoolean(false);
      final int listingIntervalMsec = 250;
      Thread clientThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while(!failoverCompleted.get()) {
            try {
              assertEquals(0, ToolRunner.run(dfsAdmin,
                  new String[] {"-listOpenFiles"}));
              assertEquals(0, ToolRunner.run(dfsAdmin,
                  new String[] {"-listOpenFiles", "-blockingDecommission"}));
              // Sleep for some time to avoid
              // flooding logs with listing.
              Thread.sleep(listingIntervalMsec);
            } catch (Exception e) {
              listOpenFilesError.set(true);
              LOG.info("Error listing open files: ", e);
              break;
            }
          }
        }
      });
      clientThread.start();

      // Let client list open files for few
      // times before the NN failover.
      Thread.sleep(listingIntervalMsec * 2);

      LOG.info("Shutting down Active NN0!");
      haCluster.shutdownNameNode(0);
      LOG.info("Transitioning NN1 to Active!");
      haCluster.transitionToActive(1);
      failoverCompleted.set(true);

      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles"}));
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-blockingDecommission"}));
      assertFalse("Client Error!", listOpenFilesError.get());

      clientThread.join();
    } finally {
      if (haCluster != null) {
        haCluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testListOpenFilesWithFilterPath() throws IOException {
    HashMap<Path, FSDataOutputStream> openFiles = new HashMap<>();
    createFiles(fs, "closed", 10);
    verifyOpenFiles(openFiles, OpenFilesIterator.FILTER_PATH_DEFAULT);

    BatchedEntries<OpenFileEntry> openFileEntryBatchedEntries = nnRpc
        .listOpenFiles(0, EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
            OpenFilesIterator.FILTER_PATH_DEFAULT);
    assertTrue("Open files list should be empty!",
        openFileEntryBatchedEntries.size() == 0);
    BatchedEntries<OpenFileEntry> openFilesBlockingDecomEntries = nnRpc
        .listOpenFiles(0, EnumSet.of(OpenFilesType.BLOCKING_DECOMMISSION),
            OpenFilesIterator.FILTER_PATH_DEFAULT);
    assertTrue("Open files list blocking decommission should be empty!",
        openFilesBlockingDecomEntries.size() == 0);

    openFiles.putAll(
        DFSTestUtil.createOpenFiles(fs, new Path("/base"), "open-1", 1));
    Map<Path, FSDataOutputStream> baseOpen =
        DFSTestUtil.createOpenFiles(fs, new Path("/base-open"), "open-1", 1);
    verifyOpenFiles(openFiles, "/base");
    verifyOpenFiles(openFiles, "/base/");

    openFiles.putAll(baseOpen);
    while (openFiles.size() > 0) {
      DFSTestUtil.closeOpenFiles(openFiles, 1);
      verifyOpenFiles(openFiles, OpenFilesIterator.FILTER_PATH_DEFAULT);
    }
  }

  @Test
  public void testListOpenFilesWithInvalidPathServerSide() throws Exception {
    HashMap<Path, FSDataOutputStream> openFiles = new HashMap<>();
    openFiles.putAll(
        DFSTestUtil.createOpenFiles(fs, new Path("/base"), "open-1", 1));
    verifyOpenFiles(openFiles, EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
        "/base");
    intercept(AssertionError.class, "Absolute path required",
        "Expect InvalidPathException", () -> verifyOpenFiles(new HashMap<>(),
            EnumSet.of(OpenFilesType.ALL_OPEN_FILES), "hdfs://cluster/base"));
    while(openFiles.size() > 0) {
      DFSTestUtil.closeOpenFiles(openFiles, 1);
      verifyOpenFiles(openFiles);
    }
  }

  @Test
  public void testListOpenFilesWithInvalidPathClientSide() throws Exception {
    intercept(IllegalArgumentException.class, "Wrong FS",
        "Expect IllegalArgumentException", () -> fs
            .listOpenFiles(EnumSet.of(OpenFilesType.ALL_OPEN_FILES),
                "hdfs://non-cluster/"));
    fs.listOpenFiles(EnumSet.of(OpenFilesType.ALL_OPEN_FILES), "/path");
  }
}
