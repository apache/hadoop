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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test abandoning blocks, which clients do on pipeline creation failure.
 */
public class TestAbandonBlock {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestAbandonBlock.class);
  
  private static final Configuration CONF = new HdfsConfiguration();
  static final String FILE_NAME_PREFIX
      = "/" + TestAbandonBlock.class.getSimpleName() + "_"; 
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @BeforeEach
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    fs = cluster.getFileSystem();
    cluster.waitActive();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  /* Abandon a block while creating a file */
  public void testAbandonBlock() throws IOException {
    String src = FILE_NAME_PREFIX + "foo";

    // Start writing a file but do not close it
    FSDataOutputStream fout = fs.create(new Path(src), true, 4096, (short)1, 512L);
    for (int i = 0; i < 1024; i++) {
      fout.write(123);
    }
    fout.hflush();
    long fileId = ((DFSOutputStream)fout.getWrappedStream()).getFileId();

    // Now abandon the last block
    DFSClient dfsclient = DFSClientAdapter.getDFSClient(fs);
    LocatedBlocks blocks =
      dfsclient.getNamenode().getBlockLocations(src, 0, Integer.MAX_VALUE);
    int orginalNumBlocks = blocks.locatedBlockCount();
    LocatedBlock b = blocks.getLastLocatedBlock();
    dfsclient.getNamenode().abandonBlock(b.getBlock(), fileId, src,
        dfsclient.clientName);
    
    // call abandonBlock again to make sure the operation is idempotent
    dfsclient.getNamenode().abandonBlock(b.getBlock(), fileId, src,
        dfsclient.clientName);

    // And close the file
    fout.close();

    // Close cluster and check the block has been abandoned after restart
    cluster.restartNameNode();
    blocks = dfsclient.getNamenode().getBlockLocations(src, 0,
        Integer.MAX_VALUE);
    assertEquals(orginalNumBlocks, blocks.locatedBlockCount() + 1, "Blocks " +
        b + " has not been abandoned.");
  }

  /**
   * Verify that when the disk-space quota is exceeded during a write, the
   * DataStreamer propagates the DSQuotaExceededException back to the client
   * and logs it at WARN level (HDFS-17845).
   */
  @Test
  @Timeout(60)
  public void testQuotaExceptionPropagatedToClient() throws Exception {
    // Use a small block size so we can fill it and trigger a second addBlock.
    final int blockSize = 1024;
    final Path testDir = new Path(FILE_NAME_PREFIX + "quota_dir");
    fs.mkdirs(testDir);

    // Create a partial-block file (512 bytes in a 1024-byte block).
    Path testFile = new Path(testDir, "file");
    DFSTestUtil.createFile(fs, testFile, 1024, 512, blockSize, (short) 1, 0L);

    // Set quota to 1 byte — the next addBlock call will exceed it.
    fs.setQuota(testDir, HdfsConstants.QUOTA_DONT_SET, 1L);

    GenericTestUtils.LogCapturer logs = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(DataStreamer.class));
    // Append 2*blockSize bytes: the first 512 bytes fill the current block,
    // and then addBlock for the next block fails due to the quota violation.
    boolean caughtQuota = false;
    try (FSDataOutputStream out = fs.append(testFile)) {
      out.write(new byte[2 * blockSize]);
      out.close();
    } catch (IOException e) {
      Throwable cause = e;
      while (cause != null && !(cause instanceof DSQuotaExceededException)) {
        cause = cause.getCause();
      }
      caughtQuota = (cause instanceof DSQuotaExceededException);
    } finally {
      logs.stopCapturing();
    }
    assertTrue(caughtQuota,
        "Expected DSQuotaExceededException to be propagated to the client");
    // The exception must have been logged at WARN level in DataStreamer.
    assertTrue(logs.getOutput().contains("DataStreamer Exception"),
        "Expected WARN 'DataStreamer Exception' in logs, got: "
            + logs.getOutput());
  }

  @Test
  /* Make sure that the quota is decremented correctly when a block is abandoned */
  public void testQuotaUpdatedWhenBlockAbandoned() throws IOException {
    // Setting diskspace quota to 3MB
    fs.setQuota(new Path("/"), HdfsConstants.QUOTA_DONT_SET, 3 * 1024 * 1024);

    // Start writing a file with 2 replicas to ensure each datanode has one.
    // Block Size is 1MB.
    String src = FILE_NAME_PREFIX + "test_quota1";
    FSDataOutputStream fout = fs.create(new Path(src), true, 4096, (short)2, 1024 * 1024);
    for (int i = 0; i < 1024; i++) {
      fout.writeByte(123);
    }

    // Shutdown one datanode, causing the block abandonment.
    cluster.getDataNodes().get(0).shutdown();

    // Close the file, new block will be allocated with 2MB pending size.
    try {
      fout.close();
    } catch (QuotaExceededException e) {
      fail("Unexpected quota exception when closing fout");
    }
  }
}
