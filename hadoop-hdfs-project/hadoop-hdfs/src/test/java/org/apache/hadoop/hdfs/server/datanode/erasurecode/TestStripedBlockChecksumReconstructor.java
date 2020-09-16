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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;

public class TestStripedBlockChecksumReconstructor {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestStripedBlockChecksumReconstructor.class);
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private int dataBlocks = ecPolicy.getNumDataUnits();
  private int parityBlocks = ecPolicy.getNumParityUnits();

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private DFSClient client;

  private int cellSize = ecPolicy.getCellSize();
  private int stripesPerBlock = 6;
  private int blockSize = cellSize * stripesPerBlock;
  private int numBlockGroups = 10;
  private int stripSize = cellSize * dataBlocks;
  private int blockGroupSize = stripesPerBlock * stripSize;
  private int fileSize = numBlockGroups * blockGroupSize;
  private int bytesPerCRC;

  private String ecDir = "/stripedReconstructor";
  private String stripedFile1 = ecDir + "/stripedFileReconstructor";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    int numDNs = dataBlocks + parityBlocks + 2;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    Path ecPath = new Path(ecDir);
    cluster.getFileSystem().mkdir(ecPath, FsPermission.getDirDefault());
    cluster.getFileSystem().getClient().setErasureCodingPolicy(ecDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
    fs = cluster.getFileSystem();
    client = fs.getClient();
    fs.enableErasureCodingPolicy(StripedFileTestUtil
        .getDefaultECPolicy().getName());
    bytesPerCRC = conf.getInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    GenericTestUtils.setLogLevel(
            TestStripedBlockChecksumReconstructor.LOG, Level.DEBUG);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 90000)
  public void testReconstructWithDirectBuffer() throws Exception {
    byte[] errIndices = new byte[1];
    errIndices[0] = (byte) 1;

    // create ec file
    prepareTestFiles(fileSize, new String[]{stripedFile1});
    // kill one dn
    DataNode dnToDie = cluster.getDataNodes().get(1);
    shutdownDataNode(dnToDie);
    Thread.sleep(1000);

    List<LocatedBlock> locatedBlocks = DFSTestUtil.getAllBlocks(
            fs, new Path(stripedFile1));
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;

    ExtendedBlock block = blockGroup.getBlock();

    StripedBlockInfo stripedBlockInfo = new StripedBlockInfo(
            block, blockGroup.getLocations(), blockGroup.getBlockTokens(),
            blockGroup.getBlockIndices(), ecPolicy);
    DatanodeInfo[] datanodes = blockGroup.getLocations();
    DataNode datanode = cluster.getDataNodes().get(0);
    DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

    StripedReconstructionInfo stripedReconInfo =
            new StripedReconstructionInfo(block, ecPolicy,
                stripedBlockInfo.getBlockIndices(), datanodes, errIndices);
    StripedBlockChecksumReconstructor checksumRecon =
        new StripedBlockChecksumReconstructorDirectBufferWrapper(
            datanode.getErasureCodingWorker(), stripedReconInfo,
            blockChecksumBuf, block.getNumBytes());
    // set the targetBuffer to direct buffer
    checksumRecon.targetBuffer = (ByteBuffer.allocateDirect(10000000));

    checksumRecon.reconstruct();
  }

  @Test(timeout = 90000)
  public void testReconstructWithNonDirectBuffer() throws Exception {
    byte[] errIndices = new byte[1];
    errIndices[0] = (byte) 1;

    // create ec file
    prepareTestFiles(fileSize, new String[]{stripedFile1});
    // kill one dn
    DataNode dnToDie = cluster.getDataNodes().get(1);
    shutdownDataNode(dnToDie);
    Thread.sleep(1000);

    List<LocatedBlock> locatedBlocks = DFSTestUtil.getAllBlocks(
            fs, new Path(stripedFile1));
    LocatedBlock locatedBlock = locatedBlocks.get(0);
    LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;

    ExtendedBlock block = blockGroup.getBlock();
    StripedBlockInfo stripedBlockInfo = new StripedBlockInfo(
            block, blockGroup.getLocations(), blockGroup.getBlockTokens(),
            blockGroup.getBlockIndices(), ecPolicy);
    DatanodeInfo[] datanodes = blockGroup.getLocations();

    DataNode datanode = cluster.getDataNodes().get(0);

    DataOutputBuffer blockChecksumBuf = new DataOutputBuffer();

    StripedReconstructionInfo stripedReconInfo =
            new StripedReconstructionInfo(block, ecPolicy,
                stripedBlockInfo.getBlockIndices(), datanodes, errIndices);
    StripedBlockChecksumReconstructor checksumRecon =
            new StripedBlockChecksumReconstructorNonDirectBufferWrapper(
                datanode.getErasureCodingWorker(), stripedReconInfo,
                blockChecksumBuf, block.getNumBytes());

    checksumRecon.reconstruct();
  }

  void shutdownDataNode(DataNode dataNode) throws IOException {
    dataNode.shutdown();
    cluster.setDataNodeDead(dataNode.getDatanodeId());
  }

  private void prepareTestFiles(int fileLength, String[] filePaths)
          throws IOException {
    byte[] fileData = StripedFileTestUtil.generateBytes(fileLength);
    for (String filePath : filePaths) {
      Path testPath = new Path(filePath);
      DFSTestUtil.writeFile(fs, testPath, fileData);
    }
  }

  class StripedBlockChecksumReconstructorDirectBufferWrapper
      extends StripedBlockChecksumReconstructor {

    protected StripedBlockChecksumReconstructorDirectBufferWrapper(
        ErasureCodingWorker worker, StripedReconstructionInfo stripedReconInfo,
        DataOutputBuffer checksumWriter, long requestedBlockLength)
            throws IOException {
      super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
    }

    @Override
    public Object getDigestObject() {
      return null;
    }

    @Override
    void prepareDigester() throws IOException {
    }

    @Override
    void updateDigester(byte[] checksumBytes,
        int dataBytesPerChecksum) throws IOException {
    }

    @Override
    void commitDigest() throws IOException {
    }

    @Override
    protected void reconstructTargets(int toReconstructLen) throws IOException {
    }

    @Override
    protected long checksumWithTargetOutput(byte[] outputData,
        int toReconstructLen) {
      return toReconstructLen;
    }
  }


  class StripedBlockChecksumReconstructorNonDirectBufferWrapper
          extends StripedBlockChecksumReconstructor {

    protected StripedBlockChecksumReconstructorNonDirectBufferWrapper(
        ErasureCodingWorker worker, StripedReconstructionInfo stripedReconInfo,
        DataOutputBuffer checksumWriter, long requestedBlockLength)
            throws IOException {
      super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
    }

    @Override
    public Object getDigestObject() {
      return null;
    }

    @Override
    void prepareDigester() throws IOException {
    }

    @Override
    void updateDigester(byte[] checksumBytes,
        int dataBytesPerChecksum) throws IOException {
    }

    @Override
    void commitDigest() throws IOException {
    }
  }
}
