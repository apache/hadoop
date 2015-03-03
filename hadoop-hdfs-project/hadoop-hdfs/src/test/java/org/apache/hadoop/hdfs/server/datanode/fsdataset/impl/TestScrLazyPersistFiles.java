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
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestScrLazyPersistFiles extends LazyPersistTestCase {

  @BeforeClass
  public static void init() {
    DomainSocket.disableBindPathValidation();
  }

  @Before
  public void before() {
    Assume.assumeThat(NativeCodeLoader.isNativeCodeLoaded() && !Path.WINDOWS,
        equalTo(true));
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Read in-memory block with Short Circuit Read
   * Note: the test uses faked RAM_DISK from physical disk.
   */
  @Test
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
  @Test
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

  @Test
  public void testShortCircuitReadAfterEviction()
      throws IOException, InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, false);
    doShortCircuitReadAfterEvictionTest();
  }

  @Test
  public void testLegacyShortCircuitReadAfterEviction()
      throws IOException, InterruptedException {
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, true);
    doShortCircuitReadAfterEvictionTest();
  }

  private void doShortCircuitReadAfterEvictionTest() throws IOException,
      InterruptedException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);

    // Verify short-circuit read from RAM_DISK.
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    File metaFile = cluster.getBlockMetadataFile(0,
        DFSTestUtil.getFirstBlock(fs, path1));
    assertTrue(metaFile.length() <= BlockMetadataHeader.getHeaderSize());
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));

    // Sleep for a short time to allow the lazy writer thread to do its job.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    // Verify short-circuit read from RAM_DISK once again.
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    metaFile = cluster.getBlockMetadataFile(0,
        DFSTestUtil.getFirstBlock(fs, path1));
    assertTrue(metaFile.length() <= BlockMetadataHeader.getHeaderSize());
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));

    // Create another file with a replica on RAM_DISK, which evicts the first.
    makeRandomTestFile(path2, BLOCK_SIZE, true, SEED);
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    // Verify short-circuit read still works from DEFAULT storage.  This time,
    // we'll have a checksum written during lazy persistence.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
    metaFile = cluster.getBlockMetadataFile(0,
        DFSTestUtil.getFirstBlock(fs, path1));
    assertTrue(metaFile.length() > BlockMetadataHeader.getHeaderSize());
    assertTrue(verifyReadRandomFile(path1, BLOCK_SIZE, SEED));

    // In the implementation of legacy short-circuit reads, any failure is
    // trapped silently, reverts back to a remote read, and also disables all
    // subsequent legacy short-circuit reads in the ClientContext.  If the test
    // uses legacy, then assert that it didn't get disabled.
    ClientContext clientContext = client.getClientContext();
    if (clientContext.getUseLegacyBlockReaderLocal()) {
      Assert.assertFalse(clientContext.getDisableLegacyBlockReaderLocal());
    }
  }

  @Test
  public void testShortCircuitReadBlockFileCorruption() throws IOException,
      InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, false);
    doShortCircuitReadBlockFileCorruptionTest();
  }

  @Test
  public void testLegacyShortCircuitReadBlockFileCorruption() throws IOException,
      InterruptedException {
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, true);
    doShortCircuitReadBlockFileCorruptionTest();
  }

  public void doShortCircuitReadBlockFileCorruptionTest() throws IOException,
      InterruptedException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Create another file with a replica on RAM_DISK, which evicts the first.
    makeRandomTestFile(path2, BLOCK_SIZE, true, SEED);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    // Corrupt the lazy-persisted block file, and verify that checksum
    // verification catches it.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
    cluster.corruptReplica(0, DFSTestUtil.getFirstBlock(fs, path1));
    exception.expect(ChecksumException.class);
    DFSTestUtil.readFileBuffer(fs, path1);
  }

  @Test
  public void testShortCircuitReadMetaFileCorruption() throws IOException,
      InterruptedException {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, false);
    doShortCircuitReadMetaFileCorruptionTest();
  }

  @Test
  public void testLegacyShortCircuitReadMetaFileCorruption() throws IOException,
      InterruptedException {
    startUpCluster(true, 1 + EVICTION_LOW_WATERMARK, true, true);
    doShortCircuitReadMetaFileCorruptionTest();
  }

  public void doShortCircuitReadMetaFileCorruptionTest() throws IOException,
      InterruptedException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    final int SEED = 0xFADED;
    makeRandomTestFile(path1, BLOCK_SIZE, true, SEED);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Create another file with a replica on RAM_DISK, which evicts the first.
    makeRandomTestFile(path2, BLOCK_SIZE, true, SEED);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    triggerBlockReport();

    // Corrupt the lazy-persisted checksum file, and verify that checksum
    // verification catches it.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
    File metaFile = cluster.getBlockMetadataFile(0,
        DFSTestUtil.getFirstBlock(fs, path1));
    MiniDFSCluster.corruptBlock(metaFile);
    exception.expect(ChecksumException.class);
    DFSTestUtil.readFileBuffer(fs, path1);
  }
}
