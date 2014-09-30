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
  package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;
  import org.apache.commons.io.IOUtils;
  import org.apache.commons.logging.Log;
  import org.apache.commons.logging.LogFactory;
  import org.apache.commons.logging.impl.Log4JLogger;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.CreateFlag;
  import org.apache.hadoop.fs.FSDataInputStream;
  import org.apache.hadoop.fs.FSDataOutputStream;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.fs.permission.FsPermission;
  import org.apache.hadoop.hdfs.*;
  import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
  import org.apache.hadoop.hdfs.protocol.LocatedBlock;
  import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
  import org.apache.hadoop.hdfs.server.datanode.DataNode;
  import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
  import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
  import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
  import org.apache.hadoop.hdfs.server.namenode.NameNode;
  import org.apache.hadoop.net.unix.DomainSocket;
  import org.apache.hadoop.net.unix.TemporarySocketDirectory;
  import org.apache.hadoop.security.UserGroupInformation;
  import org.apache.hadoop.test.GenericTestUtils;
  import org.apache.hadoop.util.NativeCodeLoader;
  import org.apache.log4j.Level;
  import org.junit.*;

  import java.io.File;
  import java.io.IOException;
  import java.util.Arrays;
  import java.util.EnumSet;
  import java.util.List;
  import java.util.UUID;

  import static org.apache.hadoop.fs.CreateFlag.CREATE;
  import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
  import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
  import static org.apache.hadoop.hdfs.StorageType.DEFAULT;
  import static org.apache.hadoop.hdfs.StorageType.RAM_DISK;
  import static org.hamcrest.CoreMatchers.equalTo;
  import static org.hamcrest.core.Is.is;
  import static org.junit.Assert.assertThat;

public class TestScrLazyPersistFiles {
  public static final Log LOG = LogFactory.getLog(TestLazyPersistFiles.class);

  static {
    ((Log4JLogger) NameNode.blockStateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FsDatasetImpl.LOG).getLogger().setLevel(Level.ALL);
  }

  private static short REPL_FACTOR = 1;
  private static final int BLOCK_SIZE = 10485760;   // 10 MB
  private static final int LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC = 3;
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MSEC = 500;
  private static final int LAZY_WRITER_INTERVAL_SEC = 1;
  private static final int BUFFER_LENGTH = 4096;
  private static TemporarySocketDirectory sockDir;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;
  private Configuration conf;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }

  @Before
  public void before() {
    Assume.assumeThat(NativeCodeLoader.isNativeCodeLoaded() && !Path.WINDOWS,
        equalTo(true));
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  @After
  public void shutDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
      client = null;
    }

    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Read in-memory block with Short Circuit Read
   * Note: the test uses faked RAM_DISK from physical disk.
   */
  @Test (timeout=300000)
  public void testRamDiskShortCircuitRead()
    throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR,
      new StorageType[]{RAM_DISK, DEFAULT},
      2 * BLOCK_SIZE - 1, true);  // 1 replica + delta, SCR read
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final int SEED = 0xFADED;
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeRandomTestFile(path, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    //assertThat(verifyReadRandomFile(path, BLOCK_SIZE, SEED), is(true));
    FSDataInputStream fis = fs.open(path);

    // Verify SCR read counters
    try {
      fis = fs.open(path);
      byte[] buf = new byte[BUFFER_LENGTH];
      fis.read(0, buf, 0, BUFFER_LENGTH);
      HdfsDataInputStream dfsis = (HdfsDataInputStream) fis;
      Assert.assertEquals(BUFFER_LENGTH,
        dfsis.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(BUFFER_LENGTH,
        dfsis.getReadStatistics().getTotalShortCircuitBytesRead());
    } finally {
      fis.close();
      fis = null;
    }
  }

  /**
   * Eviction of lazy persisted blocks with Short Circuit Read handle open
   * Note: the test uses faked RAM_DISK from physical disk.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000000)
  public void testRamDiskEvictionWithShortCircuitReadHandle()
    throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR, new StorageType[] { RAM_DISK, DEFAULT },
      (6 * BLOCK_SIZE -1), true);  // 5 replica + delta, SCR.
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");
    final int SEED = 0xFADED;

    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    // However the block replica should not be evicted from RAM_DISK yet.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    // No eviction should happen as the free ratio is below the threshold
    FSDataInputStream fis = fs.open(path1);
    try {
      // Keep and open read handle to path1 while creating path2
      byte[] buf = new byte[BUFFER_LENGTH];
      fis.read(0, buf, 0, BUFFER_LENGTH);

      // Create the 2nd file that will trigger RAM_DISK eviction.
      makeTestFile(path2, BLOCK_SIZE * 2, true);
      ensureFileReplicasOnStorageType(path2, RAM_DISK);

      // Ensure path1 is still readable from the open SCR handle.
      fis.read(fis.getPos(), buf, 0, BUFFER_LENGTH);
      HdfsDataInputStream dfsis = (HdfsDataInputStream) fis;
      Assert.assertEquals(2 * BUFFER_LENGTH,
        dfsis.getReadStatistics().getTotalBytesRead());
      Assert.assertEquals(2 * BUFFER_LENGTH,
        dfsis.getReadStatistics().getTotalShortCircuitBytesRead());
    } finally {
      IOUtils.closeQuietly(fis);
    }

    // After the open handle is closed, path1 should be evicted to DISK.
    triggerBlockReport();
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  // ---- Utility functions for all test cases -------------------------------

  /**
   * If ramDiskStorageLimit is >=0, then RAM_DISK capacity is artificially
   * capped. If ramDiskStorageLimit < 0 then it is ignored.
   */
  private void startUpCluster(final int numDataNodes,
                              final StorageType[] storageTypes,
                              final long ramDiskStorageLimit,
                              final boolean useSCR)
    throws IOException {

    conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
      LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
      HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
      LAZY_WRITER_INTERVAL_SEC);

    if (useSCR)
    {
      conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY,useSCR);
      conf.set(DFSConfigKeys.DFS_CLIENT_CONTEXT,
        UUID.randomUUID().toString());
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestShortCircuitLocalReadHandle._PORT.sock").getAbsolutePath());
      conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    }

    REPL_FACTOR = 1; //Reset in case a test has modified the value

    cluster = new MiniDFSCluster
      .Builder(conf)
      .numDataNodes(numDataNodes)
      .storageTypes(storageTypes != null ? storageTypes : new StorageType[] { DEFAULT, DEFAULT })
      .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();

    // Artificially cap the storage capacity of the RAM_DISK volume.
    if (ramDiskStorageLimit >= 0) {
      List<? extends FsVolumeSpi> volumes =
        cluster.getDataNodes().get(0).getFSDataset().getVolumes();

      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageType() == RAM_DISK) {
          ((FsVolumeImpl) volume).setCapacityForTesting(ramDiskStorageLimit);
        }
      }
    }

    LOG.info("Cluster startup complete");
  }

  private void makeTestFile(Path path, long length, final boolean isLazyPersist)
    throws IOException {

    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);

    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    FSDataOutputStream fos = null;
    try {
      fos =
        fs.create(path,
          FsPermission.getFileDefault(),
          createFlags,
          BUFFER_LENGTH,
          REPL_FACTOR,
          BLOCK_SIZE,
          null);

      // Allocate a block.
      byte[] buffer = new byte[BUFFER_LENGTH];
      for (int bytesWritten = 0; bytesWritten < length; ) {
        fos.write(buffer, 0, buffer.length);
        bytesWritten += buffer.length;
      }
      if (length > 0) {
        fos.hsync();
      }
    } finally {
      IOUtils.closeQuietly(fos);
    }
  }

  private LocatedBlocks ensureFileReplicasOnStorageType(
    Path path, StorageType storageType) throws IOException {
    // Ensure that returned block locations returned are correct!
    LOG.info("Ensure path: " + path + " is on StorageType: " + storageType);
    assertThat(fs.exists(path), is(true));
    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
      client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      assertThat(locatedBlock.getStorageTypes()[0], is(storageType));
    }
    return locatedBlocks;
  }

  private void makeRandomTestFile(Path path, long length, final boolean isLazyPersist,
                                  long seed) throws IOException {
    DFSTestUtil.createFile(fs, path, isLazyPersist, BUFFER_LENGTH, length,
      BLOCK_SIZE, REPL_FACTOR, seed, true);
  }

  private void triggerBlockReport()
    throws IOException, InterruptedException {
    // Trigger block report to NN
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    Thread.sleep(10 * 1000);
  }
}
