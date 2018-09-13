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

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;

/**
 * Tests that the DataNode respects
 * {@link DFSConfigKeys#DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY}
 */
public class TestDnRespectsBlockReportSplitThreshold {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestStorageReport.class);

  private static final int BLOCK_SIZE = 1024;
  private static final short REPL_FACTOR = 1;
  private static final long seed = 0xFEEDFACE;
  private static final int BLOCKS_IN_FILE = 5;

  private static Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  static String bpid;

  public void startUpCluster(long splitThreshold) throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY, splitThreshold);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPL_FACTOR)
        .build();
    fs = cluster.getFileSystem();
    bpid = cluster.getNamesystem().getBlockPoolId();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      fs.close();
      cluster.shutdown();
      cluster = null;
    }
  }

  private void createFile(String filenamePrefix, int blockCount)
      throws IOException {
    Path path = new Path("/" + filenamePrefix + ".dat");
    DFSTestUtil.createFile(fs, path, BLOCK_SIZE,
        blockCount * BLOCK_SIZE, BLOCK_SIZE, REPL_FACTOR, seed);
  }

  private void verifyCapturedArguments(
      ArgumentCaptor<StorageBlockReport[]> captor,
      int expectedReportsPerCall,
      int expectedTotalBlockCount) {

    List<StorageBlockReport[]> listOfReports = captor.getAllValues();
    int numBlocksReported = 0;
    for (StorageBlockReport[] reports : listOfReports) {
      assertThat(reports.length, is(expectedReportsPerCall));

      for (StorageBlockReport report : reports) {
        BlockListAsLongs blockList = report.getBlocks();
        numBlocksReported += blockList.getNumberOfBlocks();
      }
    }

    assert(numBlocksReported >= expectedTotalBlockCount);
  }

  /**
   * Test that if splitThreshold is zero, then we always get a separate
   * call per storage.
   */
  @Test(timeout=300000)
  public void testAlwaysSplit() throws IOException, InterruptedException {
    startUpCluster(0);
    NameNode nn = cluster.getNameNode();
    DataNode dn = cluster.getDataNodes().get(0);

    // Create a file with a few blocks.
    createFile(GenericTestUtils.getMethodName(), BLOCKS_IN_FILE);

    // Insert a spy object for the NN RPC.
    DatanodeProtocolClientSideTranslatorPB nnSpy =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);

    // Trigger a block report so there is an interaction with the spy
    // object.
    DataNodeTestUtils.triggerBlockReport(dn);

    ArgumentCaptor<StorageBlockReport[]> captor =
        ArgumentCaptor.forClass(StorageBlockReport[].class);

    Mockito.verify(nnSpy, times(cluster.getStoragesPerDatanode())).blockReport(
        any(DatanodeRegistration.class),
        anyString(),
        captor.capture(), Mockito.<BlockReportContext>anyObject());

    verifyCapturedArguments(captor, 1, BLOCKS_IN_FILE);
  }

  /**
   * Tests the behavior when the count of blocks is exactly one less than
   * the threshold.
   */
  @Test(timeout=300000)
  public void testCornerCaseUnderThreshold() throws IOException, InterruptedException {
    startUpCluster(BLOCKS_IN_FILE + 1);
    NameNode nn = cluster.getNameNode();
    DataNode dn = cluster.getDataNodes().get(0);

    // Create a file with a few blocks.
    createFile(GenericTestUtils.getMethodName(), BLOCKS_IN_FILE);

    // Insert a spy object for the NN RPC.
    DatanodeProtocolClientSideTranslatorPB nnSpy =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);

    // Trigger a block report so there is an interaction with the spy
    // object.
    DataNodeTestUtils.triggerBlockReport(dn);

    ArgumentCaptor<StorageBlockReport[]> captor =
        ArgumentCaptor.forClass(StorageBlockReport[].class);

    Mockito.verify(nnSpy, times(1)).blockReport(
        any(DatanodeRegistration.class),
        anyString(),
        captor.capture(), Mockito.<BlockReportContext>anyObject());

    verifyCapturedArguments(captor, cluster.getStoragesPerDatanode(), BLOCKS_IN_FILE);
  }

  /**
   * Tests the behavior when the count of blocks is exactly equal to the
   * threshold.
   */
  @Test(timeout=300000)
  public void testCornerCaseAtThreshold() throws IOException, InterruptedException {
    startUpCluster(BLOCKS_IN_FILE);
    NameNode nn = cluster.getNameNode();
    DataNode dn = cluster.getDataNodes().get(0);

    // Create a file with a few blocks.
    createFile(GenericTestUtils.getMethodName(), BLOCKS_IN_FILE);

    // Insert a spy object for the NN RPC.
    DatanodeProtocolClientSideTranslatorPB nnSpy =
        InternalDataNodeTestUtils.spyOnBposToNN(dn, nn);

    // Trigger a block report so there is an interaction with the spy
    // object.
    DataNodeTestUtils.triggerBlockReport(dn);

    ArgumentCaptor<StorageBlockReport[]> captor =
        ArgumentCaptor.forClass(StorageBlockReport[].class);

    Mockito.verify(nnSpy, times(cluster.getStoragesPerDatanode())).blockReport(
        any(DatanodeRegistration.class),
        anyString(),
        captor.capture(), Mockito.<BlockReportContext>anyObject());

    verifyCapturedArguments(captor, 1, BLOCKS_IN_FILE);
  }

}
