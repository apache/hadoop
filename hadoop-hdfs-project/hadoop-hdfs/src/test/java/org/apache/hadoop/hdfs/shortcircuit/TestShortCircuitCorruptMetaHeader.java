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
package org.apache.hadoop.hdfs.shortcircuit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

/**
 * HDFS-17986: a replica whose block meta file header is corrupt should be
 * reported to the NameNode when a client detects it during a short-circuit
 * read, even if the client has checksum verification disabled.
 *
 * Only the replica on the first DataNode is corrupted. The block scanner is
 * disabled so that any report must come from the read path.
 */
public class TestShortCircuitCorruptMetaHeader {
  private static final int FILE_LEN = 8192;
  private static final int SEED = 0xFADED;
  private static final Path TEST_FILE = new Path("/testCorruptMetaHeader");
  private static final int MAX_OPEN_ATTEMPTS = 200;

  /** The ways in which the meta file header is damaged. */
  enum Corruption {
    /** Header present, but its checksum type byte maps to no type. */
    INVALID_CHECKSUM_TYPE,
    /** Meta file truncated to zero length, so no header at all. */
    TRUNCATED
  }

  private TemporarySocketDirectory sockDir;
  private MiniDFSCluster cluster;
  private LogCapturer factoryLogs;

  static List<Arguments> params() {
    List<Arguments> params = new ArrayList<>();
    for (boolean verifyChecksum : new boolean[] {true, false}) {
      for (Corruption corruption : Corruption.values()) {
        for (int replication : new int[] {1, 3, 5, 7}) {
          params.add(Arguments.of(verifyChecksum, corruption, replication));
        }
      }
    }
    return params;
  }

  @BeforeEach
  public void setUp() {
    DomainSocket.disableBindPathValidation();
    assumeTrue(DomainSocket.getLoadingFailureReason() == null,
        "native domain socket support is required");
    sockDir = new TemporarySocketDirectory();
    factoryLogs = LogCapturer.captureLogs(
        LoggerFactory.getLogger(BlockReaderFactory.class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (factoryLogs != null) {
      factoryLogs.stopCapturing();
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    if (sockDir != null) {
      sockDir.close();
    }
  }

  /**
   * The short-circuit read fails on the corrupt meta header and the client
   * falls back to a remote read from the same DataNode. With checksum
   * verification enabled, that remote read fails and the DataNode reports
   * the replica (HDFS-14706); with more than one replica the client then
   * reads from a healthy one. With checksum verification disabled (as HBase
   * does when it verifies its own checksums), the remote read never opens
   * the meta file and succeeds, so the replica must be reported when the
   * DataNode passes the short-circuit file descriptors.
   */
  @ParameterizedTest(name = "verifyChecksum={0}, {1}, replication={2}")
  @MethodSource("params")
  @Timeout(value = 120)
  public void testCorruptMetaHeaderIsReported(boolean verifyChecksum,
      Corruption corruption, int replication) throws Exception {
    DistributedFileSystem fs = startClusterAndCorruptMetaHeader(
        "test_" + verifyChecksum + "_" + corruption + "_" + replication,
        replication, corruption);
    fs.setVerifyChecksum(verifyChecksum);

    if (verifyChecksum && replication == 1) {
      LambdaTestUtils.intercept(BlockMissingException.class,
          () -> readFromCorruptReplica(fs));
    } else {
      assertArrayEquals(expectedContents(), readFromCorruptReplica(fs));
    }
    assertShortCircuitReplicaCreationFailed();
    waitForCorruptReplicaReport();
  }

  private DistributedFileSystem startClusterAndCorruptMetaHeader(
      String testName, int replication, Corruption corruption)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFS_CLIENT_CONTEXT, testName);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(), testName + "._PORT").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    // Fail fast when the only replica is unreadable.
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    // Keep the block scanner from finding the corrupt replica on its own.
    conf.setLong(DFS_BLOCK_SCANNER_VOLUME_BYTES_PER_SECOND, 0);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(replication)
        .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, TEST_FILE, FILE_LEN, (short) replication,
        SEED);
    DFSTestUtil.waitReplication(fs, TEST_FILE, (short) replication);

    // Look the block up without reading it: a read would cache a
    // short-circuit replica holding the still-valid header.
    ExtendedBlock block =
        DFSTestUtil.getAllBlocks(fs, TEST_FILE).get(0).getBlock();
    File metaFile = cluster.getBlockMetadataFile(0, block);
    try (RandomAccessFile raf = new RandomAccessFile(metaFile, "rw")) {
      switch (corruption) {
      case INVALID_CHECKSUM_TYPE:
        // The header is a 2-byte version followed by the 1-byte checksum
        // type; overwrite the type with a value that maps to no type.
        raf.seek(2);
        raf.write(0x7f);
        break;
      case TRUNCATED:
        raf.setLength(0);
        break;
      default:
        throw new IllegalArgumentException("Unknown corruption " + corruption);
      }
    }
    return fs;
  }

  /**
   * Reads the whole file through a stream whose first choice of DataNode is
   * the one holding the corrupt replica. The NameNode orders equally distant
   * replicas randomly, so reopen until the corrupt replica comes first.
   */
  private byte[] readFromCorruptReplica(DistributedFileSystem fs)
      throws Exception {
    int corruptPort = cluster.getDataNodes().get(0).getXferPort();
    for (int i = 0; i < MAX_OPEN_ATTEMPTS; i++) {
      try (HdfsDataInputStream in =
          (HdfsDataInputStream) fs.open(TEST_FILE)) {
        DatanodeInfo first = in.getAllBlocks().get(0).getLocations()[0];
        if (first.getXferPort() != corruptPort) {
          continue;
        }
        byte[] contents = new byte[FILE_LEN];
        IOUtils.readFully(in, contents, 0, FILE_LEN);
        return contents;
      }
    }
    throw new IOException("The corrupt replica was never listed first in "
        + MAX_OPEN_ATTEMPTS + " attempts");
  }

  private static byte[] expectedContents() {
    return DFSTestUtil.calculateFileContentsFromSeed(SEED, FILE_LEN);
  }

  private void assertShortCircuitReplicaCreationFailed() {
    String out = factoryLogs.getOutput();
    assertTrue(out.contains("error creating ShortCircuitReplica")
            && out.contains("CorruptMetaHeaderException"),
        "Expected the short-circuit read to fail on the corrupt meta header");
  }

  private void waitForCorruptReplicaReport() throws Exception {
    try {
      GenericTestUtils.waitFor(() -> cluster.getNamesystem()
          .getBlockManager().getCorruptBlocks() == 1, 100, 10000);
    } catch (TimeoutException e) {
      fail("Corrupt replica was not reported to the NameNode via the read "
          + "path (block scanner disabled): expected 1 corrupt block but "
          + "found " + cluster.getNamesystem().getBlockManager()
          .getCorruptBlocks());
    }
  }
}
