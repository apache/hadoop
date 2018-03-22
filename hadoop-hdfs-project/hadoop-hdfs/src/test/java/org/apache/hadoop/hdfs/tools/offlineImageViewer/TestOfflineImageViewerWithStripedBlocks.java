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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOfflineImageViewerWithStripedBlocks {
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private int dataBlocks = ecPolicy.getNumDataUnits();
  private int parityBlocks = ecPolicy.getNumParityUnits();

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 3;
  private final int blockSize = cellSize * stripesPerBlock;

  @Before
  public void setup() throws IOException {
    int numDNs = dataBlocks + parityBlocks + 2;
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    Path eczone = new Path("/eczone");
    fs.mkdirs(eczone);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testFileEqualToOneStripe() throws Exception {
    int numBytes = cellSize;
    testFileSize(numBytes);
  }

  @Test(timeout = 60000)
  public void testFileLessThanOneStripe() throws Exception {
    int numBytes = cellSize - 100;
    testFileSize(numBytes);
  }

  @Test(timeout = 60000)
  public void testFileHavingMultipleBlocks() throws Exception {
    int numBytes = blockSize * 3;
    testFileSize(numBytes);
  }

  @Test(timeout = 60000)
  public void testFileLargerThanABlockGroup1() throws IOException {
    testFileSize(blockSize * dataBlocks + cellSize + 123);
  }

  @Test(timeout = 60000)
  public void testFileLargerThanABlockGroup2() throws IOException {
    testFileSize(blockSize * dataBlocks * 3 + cellSize * dataBlocks + cellSize
        + 123);
  }

  @Test(timeout = 60000)
  public void testFileFullBlockGroup() throws IOException {
    testFileSize(blockSize * dataBlocks);
  }

  @Test(timeout = 60000)
  public void testFileMoreThanOneStripe() throws Exception {
    int numBytes = blockSize + blockSize / 2;
    testFileSize(numBytes);
  }

  private void testFileSize(int numBytes) throws IOException,
      UnresolvedLinkException, SnapshotAccessControlException {
    fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    File orgFsimage = null;
    Path file = new Path("/eczone/striped");
    FSDataOutputStream out = fs.create(file, true);
    byte[] bytes = DFSTestUtil.generateSequentialBytes(0, numBytes);
    out.write(bytes);
    out.close();

    // Write results to the fsimage file
    fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    fs.saveNamespace();

    // Determine location of fsimage file
    orgFsimage = FSImageTestUtil.findLatestImageFile(FSImageTestUtil
        .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
    if (orgFsimage == null) {
      throw new RuntimeException("Didn't generate or can't find fsimage");
    }
    FSImageLoader loader = FSImageLoader.load(orgFsimage.getAbsolutePath());
    String fileStatus = loader.getFileStatus("/eczone/striped");
    long expectedFileSize = bytes.length;

    // Verify space consumed present in BlockInfoStriped
    FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();
    INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();
    assertEquals(StripedFileTestUtil.getDefaultECPolicy().getId(),
        fileNode.getErasureCodingPolicyID());
    assertTrue("Invalid block size", fileNode.getBlocks().length > 0);
    long actualFileSize = 0;
    for (BlockInfo blockInfo : fileNode.getBlocks()) {
      assertTrue("Didn't find block striped information",
          blockInfo instanceof BlockInfoStriped);
      actualFileSize += blockInfo.getNumBytes();
    }

    assertEquals("Wrongly computed file size contains striped blocks",
        expectedFileSize, actualFileSize);

    // Verify space consumed present in filestatus
    String EXPECTED_FILE_SIZE = "\"length\":"
        + String.valueOf(expectedFileSize);
    assertTrue(
        "Wrongly computed file size contains striped blocks, file status:"
            + fileStatus + ". Expected file size is : " + EXPECTED_FILE_SIZE,
        fileStatus.contains(EXPECTED_FILE_SIZE));
  }
}
