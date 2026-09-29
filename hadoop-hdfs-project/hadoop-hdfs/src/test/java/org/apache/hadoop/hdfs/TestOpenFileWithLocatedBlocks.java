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

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that opening a file with pre-fetched block locations via the
 * {@code openFile().withFileStatus()} builder API skips the NameNode RPC
 * for block locations.
 */
public class TestOpenFileWithLocatedBlocks {
  private static final String NN_METRICS = "NameNodeActivity";
  private static final int BLOCK_SIZE = 1024;
  private static final short REPLICATION = 1;
  private static final String TEST_KEY = "test_key";
  private static final EnumSet<CreateEncryptionZoneFlag> NO_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.NO_TRASH);

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static Configuration conf;
  private static Path ezPath;

  @BeforeAll
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    FileSystemTestHelper fsHelper = new FileSystemTestHelper();
    File testRootDir = new File(fsHelper.getTestRootDir()).getAbsoluteFile();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file" +
            new Path(testRootDir.toString(), "test.jks").toUri());
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    createEncryptionZone();
  }

  private static void createEncryptionZone() throws Exception {
    DFSTestUtil.createKey(TEST_KEY, cluster, conf);
    fs.getClient().setKeyProvider(
        cluster.getNameNode().getNamesystem().getProvider());
    ezPath = new Path("/ez");
    fs.mkdirs(ezPath);
    new HdfsAdmin(cluster.getURI(), conf)
        .createEncryptionZone(ezPath, TEST_KEY, NO_TRASH);
  }

  @AfterAll
  public static void teardownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private Path createFile(String name, byte[] data) throws IOException {
    Path path = new Path("/" + name);
    ContractTestUtils.createFile(fs, path, true, data);
    return path;
  }

  @Test
  public void testOpenFileWithLocatedFileStatus() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'a', 26);
    Path path = createFile("testfile", data);

    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    HdfsLocatedFileStatus locatedStatus =
        (HdfsLocatedFileStatus) iter.next();

    long countBefore = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");

    try (FSDataInputStream in = fs.openFile(path)
        .withFileStatus(locatedStatus)
        .build()
        .get()) {
      ContractTestUtils.verifyRead(in, data, 0, data.length);
    }

    long countAfter = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");
    assertEquals(countBefore, countAfter,
        "Opening with pre-fetched locations should not trigger " +
        "GetBlockLocations RPC");
  }

  @Test
  public void testOpenFileWithoutStatusFallsBack() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE, 'b', 26);
    Path path = createFile("testfile_no_status", data);

    long countBefore = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");

    try (FSDataInputStream in = fs.openFile(path)
        .build()
        .get()) {
      ContractTestUtils.verifyRead(in, data, 0, data.length);
    }

    long countAfter = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");
    assertEquals(countBefore + 1, countAfter,
        "Opening without status should trigger GetBlockLocations RPC");
  }

  @Test
  public void testOpenFileWithPlainFileStatusFallsBack() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE, 'c', 26);
    Path path = createFile("testfile_plain_status", data);

    FileStatus plainStatus = fs.getFileStatus(path);

    long countBefore = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");

    try (FSDataInputStream in = fs.openFile(path)
        .withFileStatus(plainStatus)
        .build()
        .get()) {
      ContractTestUtils.verifyRead(in, data, 0, data.length);
    }

    long countAfter = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");
    assertEquals(countBefore + 1, countAfter,
        "Opening with plain FileStatus should trigger " +
        "GetBlockLocations RPC");
  }

  @Test
  public void testCloneDataInputStream() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 3, 'd', 26);
    Path path = createFile("testfile_clone", data);

    try (FSDataInputStream original = fs.open(path)) {
      long countBefore = getLong(getMetrics(NN_METRICS),
          "GetBlockLocations");

      try (FSDataInputStream cloned = fs.cloneDataInputStream(original)) {
        long countAfter = getLong(getMetrics(NN_METRICS),
            "GetBlockLocations");
        assertEquals(countBefore, countAfter,
            "Cloning should not trigger GetBlockLocations RPC");

        ContractTestUtils.verifyRead(cloned, data, 0, data.length);
      }

      ContractTestUtils.verifyRead(original, data, 0, data.length);
    }
  }

  @Test
  public void testCloneDataInputStreamIndependentPosition() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'e', 26);
    Path path = createFile("testfile_clone_pos", data);

    try (FSDataInputStream original = fs.open(path)) {
      byte[] buf1 = new byte[BLOCK_SIZE];
      original.readFully(buf1);

      try (FSDataInputStream cloned = fs.cloneDataInputStream(original)) {
        assertEquals(0, cloned.getPos());
        assertEquals(BLOCK_SIZE, original.getPos());

        ContractTestUtils.verifyRead(cloned, data, 0, data.length);
      }
    }
  }

  @Test
  public void testMultipleClonesFromSameHandle() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'f', 26);
    Path path = createFile("testfile_multi_clone", data);

    try (FSDataInputStream original = fs.open(path)) {
      long countBefore = getLong(getMetrics(NN_METRICS),
          "GetBlockLocations");

      int numClones = 5;
      FSDataInputStream[] clones = new FSDataInputStream[numClones];
      for (int i = 0; i < numClones; i++) {
        clones[i] = fs.cloneDataInputStream(original);
      }

      long countAfter = getLong(getMetrics(NN_METRICS),
          "GetBlockLocations");
      assertEquals(countBefore, countAfter,
          "Multiple clones should not trigger GetBlockLocations RPC");

      for (int i = 0; i < numClones; i++) {
        ContractTestUtils.verifyRead(clones[i], data, 0, data.length);
        clones[i].close();
      }
    }
  }

  @Test
  public void testCloneOfClone() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'g', 26);
    Path path = createFile("testfile_clone_of_clone", data);

    try (FSDataInputStream original = fs.open(path)) {
      long countBefore = getLong(getMetrics(NN_METRICS),
          "GetBlockLocations");

      try (FSDataInputStream clone1 = fs.cloneDataInputStream(original);
           FSDataInputStream clone2 = fs.cloneDataInputStream(clone1)) {
        long countAfter = getLong(getMetrics(NN_METRICS),
            "GetBlockLocations");
        assertEquals(countBefore, countAfter,
            "Clone of clone should not trigger GetBlockLocations RPC");

        ContractTestUtils.verifyRead(clone2, data, 0, data.length);
      }
    }
  }

  @Test
  public void testConcurrentPreadFromClones() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 4, 'h', 26);
    Path path = createFile("testfile_concurrent", data);

    try (FSDataInputStream original = fs.open(path)) {
      int numReaders = 4;
      FSDataInputStream[] clones = new FSDataInputStream[numReaders];
      for (int i = 0; i < numReaders; i++) {
        clones[i] = fs.cloneDataInputStream(original);
      }

      ExecutorService executor = Executors.newFixedThreadPool(numReaders);
      List<Future<byte[]>> futures = new ArrayList<>();
      for (int i = 0; i < numReaders; i++) {
        final int blockIdx = i;
        final FSDataInputStream stream = clones[i];
        futures.add(executor.submit(() -> {
          byte[] buf = new byte[BLOCK_SIZE];
          stream.readFully(blockIdx * BLOCK_SIZE, buf);
          return buf;
        }));
      }

      for (int i = 0; i < numReaders; i++) {
        byte[] expected = new byte[BLOCK_SIZE];
        System.arraycopy(data, i * BLOCK_SIZE, expected, 0, BLOCK_SIZE);
        assertArrayEquals(expected, futures.get(i).get());
        clones[i].close();
      }
      executor.shutdown();
    }
  }

  @Test
  public void testCloneAfterOriginalClosed() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE, 'i', 26);
    Path path = createFile("testfile_clone_after_close", data);

    FSDataInputStream cloned;
    try (FSDataInputStream original = fs.open(path)) {
      cloned = fs.cloneDataInputStream(original);
    }
    ContractTestUtils.verifyRead(cloned, data, 0, data.length);
    cloned.close();
  }

  @Test
  public void testCloneDataInputStreamEncrypted() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'j', 26);
    Path path = new Path(ezPath, "encrypted_file");
    ContractTestUtils.createFile(fs, path, true, data);

    try (FSDataInputStream original = fs.open(path)) {
      assertInstanceOf(CryptoInputStream.class, original.getWrappedStream(),
          "Expected CryptoInputStream for encrypted file");

      long countBefore = getLong(getMetrics(NN_METRICS),
          "GetBlockLocations");

      try (FSDataInputStream cloned = fs.cloneDataInputStream(original)) {
        long countAfter = getLong(getMetrics(NN_METRICS),
            "GetBlockLocations");
        assertEquals(countBefore, countAfter,
            "Cloning encrypted stream should not trigger "
                + "GetBlockLocations RPC");

        ContractTestUtils.verifyRead(cloned, data, 0, data.length);
      }

      ContractTestUtils.verifyRead(original, data, 0, data.length);
    }
  }

  @Test
  public void testOpenFileWithStatusEncrypted() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'k', 26);
    Path path = new Path(ezPath, "encrypted_file2");
    ContractTestUtils.createFile(fs, path, true, data);

    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    HdfsLocatedFileStatus locatedStatus =
        (HdfsLocatedFileStatus) iter.next();

    long countBefore = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");

    try (FSDataInputStream in = fs.openFile(path)
        .withFileStatus(locatedStatus)
        .build()
        .get()) {
      ContractTestUtils.verifyRead(in, data, 0, data.length);
    }

    long countAfter = getLong(getMetrics(NN_METRICS),
        "GetBlockLocations");
    assertEquals(countBefore, countAfter,
        "Opening encrypted file with pre-fetched locations should not "
            + "trigger GetBlockLocations RPC");
  }

  @Test
  public void testCloneEncryptedIndependentPosition() throws Exception {
    byte[] data = ContractTestUtils.dataset(BLOCK_SIZE * 2, 'l', 26);
    Path path = new Path(ezPath, "encrypted_file3");
    ContractTestUtils.createFile(fs, path, true, data);

    try (FSDataInputStream original = fs.open(path)) {
      byte[] buf1 = new byte[BLOCK_SIZE];
      original.readFully(buf1);
      assertEquals(BLOCK_SIZE, original.getPos());

      try (FSDataInputStream cloned = fs.cloneDataInputStream(original)) {
        assertEquals(0, cloned.getPos());

        ContractTestUtils.verifyRead(cloned, data, 0, data.length);
      }
    }
  }

  private static long getLong(
      org.apache.hadoop.metrics2.MetricsRecordBuilder rb, String name) {
    return org.apache.hadoop.test.MetricsAsserts.getLongCounter(name, rb);
  }
}
