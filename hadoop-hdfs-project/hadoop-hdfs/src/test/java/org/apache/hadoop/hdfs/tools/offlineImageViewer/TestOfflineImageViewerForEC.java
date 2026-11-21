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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests offline image viewer support for erasure-coded files, including
 * loading the ERASURE_CODING section from an fsimage and reporting the
 * correct spaceConsumed for striped files (logical data bytes plus parity
 * bytes) instead of length * replication.
 */
public class TestOfflineImageViewerForEC {

  private static final ErasureCodingPolicy EC_POLICY =
      StripedFileTestUtil.getDefaultECPolicy();

  private static File originalFsimage;
  private static ContentSummary ecFileMidCellSummary;
  private static ContentSummary ecFileFullStripeSummary;
  private static ContentSummary ecFileSingleCellSummary;
  private static ContentSummary ecFileEmptySummary;
  private static ContentSummary ecSubDirSummary;
  private static ContentSummary ecDirSummary;
  private static ContentSummary replicatedFileSummary;

  /**
   * Create a namespace containing erasure-coded files of several shapes and a
   * replicated file, save its fsimage, and record the live DFS content
   * summaries for later comparison against the WebImageViewer results.
   */
  @BeforeAll
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      int numDNs = EC_POLICY.getNumDataUnits()
          + EC_POLICY.getNumParityUnits() + 1;
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      hdfs.enableErasureCodingPolicy(EC_POLICY.getName());

      Path ecDir = new Path("/ecDir");
      hdfs.mkdirs(ecDir);
      hdfs.setErasureCodingPolicy(ecDir, EC_POLICY.getName());

      // Ends mid-cell so the parity rounding is exercised.
      Path ecFileMidCell = new Path(ecDir, "ecFileMidCell");
      writeFile(hdfs, ecFileMidCell, EC_POLICY.getCellSize() * 2 + 123);

      // Exactly one full stripe.
      Path ecFileFullStripe = new Path(ecDir, "ecFileFullStripe");
      writeFile(hdfs, ecFileFullStripe,
          EC_POLICY.getCellSize() * EC_POLICY.getNumDataUnits());

      // A single cell, shorter than one stripe.
      Path ecFileSingleCell = new Path(ecDir, "ecFileSingleCell");
      writeFile(hdfs, ecFileSingleCell, EC_POLICY.getCellSize());

      // An empty striped file.
      Path ecFileEmpty = new Path(ecDir, "ecFileEmpty");
      try (FSDataOutputStream out = hdfs.create(ecFileEmpty)) {
        // No data written.
      }

      // Nested EC directory to exercise recursive summarization.
      Path ecSubDir = new Path(ecDir, "subDir");
      hdfs.mkdirs(ecSubDir);
      Path ecFileNested = new Path(ecSubDir, "ecFileNested");
      writeFile(hdfs, ecFileNested, EC_POLICY.getCellSize() + 1);

      // A replicated file for regression coverage of the non-EC path.
      Path repFile = new Path("/repFile");
      writeFile(hdfs, repFile, 5);

      ecFileMidCellSummary = hdfs.getContentSummary(ecFileMidCell);
      ecFileFullStripeSummary = hdfs.getContentSummary(ecFileFullStripe);
      ecFileSingleCellSummary = hdfs.getContentSummary(ecFileSingleCell);
      ecFileEmptySummary = hdfs.getContentSummary(ecFileEmpty);
      ecSubDirSummary = hdfs.getContentSummary(ecSubDir);
      ecDirSummary = hdfs.getContentSummary(ecDir);
      replicatedFileSummary = hdfs.getContentSummary(repFile);

      hdfs.setSafeMode(SafeModeAction.ENTER, false);
      hdfs.saveNamespace();
      originalFsimage = FSImageTestUtil.findLatestImageFile(
          FSImageTestUtil.getFSImage(cluster.getNameNode())
              .getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new IOException("Didn't generate or can't find fsimage");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @AfterAll
  public static void deleteOriginalFSImage() {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testGetContentSummaryForEcFileMidCell() throws Exception {
    verifyContentSummary(ecFileMidCellSummary,
        query("/ecDir/ecFileMidCell"));
  }

  @Test
  public void testGetContentSummaryForEcFileFullStripe() throws Exception {
    verifyContentSummary(ecFileFullStripeSummary,
        query("/ecDir/ecFileFullStripe"));
  }

  @Test
  public void testGetContentSummaryForEcFileSingleCell() throws Exception {
    verifyContentSummary(ecFileSingleCellSummary,
        query("/ecDir/ecFileSingleCell"));
  }

  @Test
  public void testGetContentSummaryForEcFileEmpty() throws Exception {
    verifyContentSummary(ecFileEmptySummary, query("/ecDir/ecFileEmpty"));
  }

  @Test
  public void testGetContentSummaryForEcSubDirectory() throws Exception {
    verifyContentSummary(ecSubDirSummary, query("/ecDir/subDir"));
  }

  @Test
  public void testGetContentSummaryForEcDirectory() throws Exception {
    verifyContentSummary(ecDirSummary, query("/ecDir"));
  }

  @Test
  public void testGetContentSummaryForReplicatedFile() throws Exception {
    verifyContentSummary(replicatedFileSummary, query("/repFile"));
  }

  @Test
  public void testStorageSpaceConsumedReplicatedFile() {
    INodeFile f = INodeFile.newBuilder()
        .setReplication(3)
        .addBlocks(block(100, 1L))
        .addBlocks(block(50, 2L))
        .build();
    assertEquals(450, FSImageLoader.getStorageSpaceConsumed(f));
  }

  @Test
  public void testStorageSpaceConsumedStripedFullStripe() throws Exception {
    initEcManager();
    long numBytes = (long) EC_POLICY.getCellSize()
        * EC_POLICY.getNumDataUnits();
    // Six data cells plus three parity cells, each one cell in size.
    long expected = (long) EC_POLICY.getNumDataUnits()
        * EC_POLICY.getCellSize()
        + (long) EC_POLICY.getNumParityUnits() * EC_POLICY.getCellSize();
    assertEquals(expected,
        FSImageLoader.getStorageSpaceConsumed(stripedFile(numBytes)));
  }

  @Test
  public void testStorageSpaceConsumedStripedMidCell() throws Exception {
    initEcManager();
    long numBytes = 2L * EC_POLICY.getCellSize() + 123;
    // Logical data bytes plus three full parity cells.
    long expected = numBytes
        + (long) EC_POLICY.getNumParityUnits() * EC_POLICY.getCellSize();
    assertEquals(expected,
        FSImageLoader.getStorageSpaceConsumed(stripedFile(numBytes)));
  }

  @Test
  public void testStorageSpaceConsumedStripedEmptyFile() throws Exception {
    initEcManager();
    assertEquals(0, FSImageLoader.getStorageSpaceConsumed(stripedFile(0)));
  }

  private static void writeFile(DistributedFileSystem hdfs, Path path,
      int numBytes) throws IOException {
    try (FSDataOutputStream out = hdfs.create(path)) {
      out.write(DFSTestUtil.generateSequentialBytes(0, numBytes));
    }
  }

  private static HdfsProtos.BlockProto block(long numBytes, long blockId) {
    return HdfsProtos.BlockProto.newBuilder()
        .setBlockId(blockId)
        .setGenStamp(1L)
        .setNumBytes(numBytes)
        .build();
  }

  private static INodeFile stripedFile(long numBytes) {
    return INodeFile.newBuilder()
        .setReplication(1)
        .setErasureCodingPolicyID(EC_POLICY.getId())
        .addBlocks(block(numBytes, 1L))
        .build();
  }

  private static void initEcManager() throws IOException {
    ErasureCodingPolicyManager.getInstance().init(new Configuration());
  }

  private ContentSummary query(String path) throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      URI uri = new URI("webhdfs://localhost:" + viewer.getPort());
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri,
          new Configuration());
      return webfs.getContentSummary(new Path(path));
    }
  }

  private void verifyContentSummary(ContentSummary expected,
      ContentSummary actual) {
    assertEquals(expected.getDirectoryCount(), actual.getDirectoryCount());
    assertEquals(expected.getFileCount(), actual.getFileCount());
    assertEquals(expected.getLength(), actual.getLength());
    assertEquals(expected.getSpaceConsumed(), actual.getSpaceConsumed());
    assertEquals(expected.getQuota(), actual.getQuota());
    assertEquals(expected.getSpaceQuota(), actual.getSpaceQuota());
  }
}
