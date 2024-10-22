/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Arrays;

public class TestDFSStripedInputStreamWithTimeout {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestDFSStripedInputStreamWithTimeout.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private ErasureCodingPolicy ecPolicy;
  private short dataBlocks;
  private short parityBlocks;
  private int cellSize;
  private final int stripesPerBlock = 2;
  private int blockSize;
  private int blockGroupSize;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setup() throws IOException {
    /*
     * Initialize erasure coding policy.
     */
    ecPolicy = getEcPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = stripesPerBlock * cellSize;
    blockGroupSize = dataBlocks * blockSize;
    System.out.println("EC policy = " + ecPolicy);

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 500);

    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getRandomizedTempPath());
    SimulatedFSDataset.setFactory(conf);
    startUp();
  }

  private void startUp() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        dataBlocks + parityBlocks).build();
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(getEcPolicy().getName());
    fs.mkdirs(dirPath);
    fs.getClient()
        .setErasureCodingPolicy(dirPath.toString(), ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testPreadTimeout() throws Exception {
    final int numBlocks = 2;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    final int fileSize = numBlocks * blockGroupSize;

    LocatedBlocks lbs = fs.getClient().namenode.
        getBlockLocations(filePath.toString(), 0, fileSize);
    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      assert lb instanceof LocatedStripedBlock;
      LocatedStripedBlock bg = (LocatedStripedBlock) (lb);
      for (int i = 0; i < dataBlocks; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            stripesPerBlock * cellSize,
            bg.getBlock().getGenerationStamp());
        blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
        cluster.injectBlocks(i, Arrays.asList(blk),
            bg.getBlock().getBlockPoolId());
      }
    }
    try {
      testReadFileWithAttempt(1);
      Assert.fail("It Should fail to read striped time out with 1 attempt . ");
    } catch (Exception e) {
      Assert.assertTrue(
          "Throw IOException error message with 4 missing blocks. ",
          e.getMessage().contains("4 missing blocks"));
    }

    try {
      testReadFileWithAttempt(3);
    } catch (Exception e) {
      Assert.fail("It Should successfully read striped file with 3 attempts. ");
    }
  }

  private  void testReadFileWithAttempt(int attempt) throws Exception {
    // set dfs client config
    cluster.getConfiguration(0)
        .setInt(HdfsClientConfigKeys.StripedRead.DATANODE_MAX_ATTEMPTS,
            attempt);
    cluster.getConfiguration(0)
        .setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 1000);
    DistributedFileSystem newFs =
        (DistributedFileSystem) cluster.getNewFileSystemInstance(0);
    try(DFSStripedInputStream in = new DFSStripedInputStream(newFs.getClient(),
        filePath.toString(), false, ecPolicy, null)){
      int bufLen = 1024 * 100;
      byte[] buf = new byte[bufLen];
      int readTotal = 0;
      in.seek(readTotal);
      int nread = in.read(buf, 0, bufLen);
      // Simulated time-consuming processing operations, such as UDF.
      // And datanodes close connect because of socket timeout.
      cluster.dataNodes.forEach(dn -> dn.getDatanode().closeDataXceiverServer());
      in.seek(nread);
      // StripeRange 6MB
      bufLen = 1024 * 1024 * 6;
      buf = new byte[bufLen];
      in.read(buf, 0, bufLen);
    }
  }
}
