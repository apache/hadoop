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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.BPOfferService;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImplTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that very large block reports can pass through the RPC server and
 * deserialization layers successfully if configured.
 */
public class TestLargeBlockReport {

  private final HdfsConfiguration conf = new HdfsConfiguration();
  private MiniDFSCluster cluster;
  private DataNode dn;
  private BPOfferService bpos;
  private DatanodeProtocolClientSideTranslatorPB nnProxy;
  private DatanodeRegistration bpRegistration;
  private String bpId;
  private DatanodeStorage dnStorage;
  private final long reportId = 1;
  private final long fullBrLeaseId = 0;
  private final boolean sorted = true;

  @BeforeClass
  public static void init() {
    DFSTestUtil.setNameNodeLogLevel(Level.WARN);
    FsDatasetImplTestUtils.setFsDatasetImplLogLevel(Level.WARN);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockReportExceedsLengthLimit() throws Exception {
    initCluster();
    // Create a large enough report that we expect it will go beyond the RPC
    // server's length validation, and also protobuf length validation.
    StorageBlockReport[] reports = createReports(6000000);
    try {
      nnProxy.blockReport(bpRegistration, bpId, reports,
          new BlockReportContext(1, 0, reportId, fullBrLeaseId, sorted));
      fail("Should have failed because of the too long RPC data length");
    } catch (Exception e) {
      // Expected.  We can't reliably assert anything about the exception type
      // or the message.  The NameNode just disconnects, and the details are
      // buried in the NameNode log.
    }
  }

  @Test
  public void testBlockReportSucceedsWithLargerLengthLimit() throws Exception {
    conf.setInt(IPC_MAXIMUM_DATA_LENGTH, 128 * 1024 * 1024); // 128 MB
    initCluster();
    StorageBlockReport[] reports = createReports(6000000);
    nnProxy.blockReport(bpRegistration, bpId, reports,
        new BlockReportContext(1, 0, reportId, fullBrLeaseId, sorted));
  }

  /**
   * Creates storage block reports, consisting of a single report with the
   * requested number of blocks.  The block data is fake, because the tests just
   * need to validate that the messages can pass correctly.  This intentionally
   * uses the old-style decoding method as a helper.  The test needs to cover
   * the new-style encoding technique.  Passing through that code path here
   * would trigger an exception before the test is ready to deal with it.
   *
   * @param numBlocks requested number of blocks
   * @return storage block reports
   */
  private StorageBlockReport[] createReports(int numBlocks) {
    int longsPerBlock = 3;
    int blockListSize = 2 + numBlocks * longsPerBlock;
    List<Long> longs = new ArrayList<Long>(blockListSize);
    longs.add(Long.valueOf(numBlocks));
    longs.add(0L);
    for (int i = 0; i < blockListSize; ++i) {
      longs.add(Long.valueOf(i));
    }
    BlockListAsLongs blockList = BlockListAsLongs.decodeLongs(longs);
    StorageBlockReport[] reports = new StorageBlockReport[] {
        new StorageBlockReport(dnStorage, blockList) };
    return reports;
  }

  /**
   * Start a mini-cluster, and set up everything the tests need to use it.
   *
   * @throws Exception if initialization fails
   */
  private void initCluster() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dn = cluster.getDataNodes().get(0);
    bpos = dn.getAllBpOs().get(0);
    nnProxy = bpos.getActiveNN();
    bpRegistration = bpos.bpRegistration;
    bpId = bpos.getBlockPoolId();
    dnStorage = dn.getFSDataset().getBlockReports(bpId).keySet().iterator()
        .next();
  }
}
