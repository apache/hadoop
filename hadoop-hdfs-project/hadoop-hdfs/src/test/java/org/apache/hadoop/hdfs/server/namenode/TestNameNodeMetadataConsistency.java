/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TestNameNodeMetadataConsistency {
  private static final Path filePath1 = new Path("/testdata1.txt");
  private static final Path filePath2 = new Path("/testdata2.txt");

  private static final int SCAN_INTERVAL = 1;
  private static final int SCAN_WAIT = 3;
  MiniDFSCluster cluster;
  HdfsConfiguration conf;

  @Before
  public void InitTest() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        SCAN_INTERVAL);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
  }

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * This test creates a file and modifies the block generation stamp to number
   * that name node has not seen yet. It then asserts that name node moves into
   * safe mode while it is in startup mode.
   */
  @Test
  public void testGenerationStampInFuture() throws
      IOException, InterruptedException {

    String testData = " This is test data";
    int datalen = testData.length();

    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    OutputStream ostream = fs.create(filePath1);
    ostream.write(testData.getBytes());
    ostream.close();

    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath1);
    long genStamp = block.getGenerationStamp();

    // Re-write the Generation Stamp to a Generation Stamp in future.
    cluster.changeGenStampOfBlock(0, block, genStamp + 1);
    MiniDFSCluster.DataNodeProperties dnProps = cluster.stopDataNode(0);


    // Simulate  Namenode forgetting a Block
    cluster.restartNameNode(true);
    BlockInfo bInfo = cluster.getNameNode().getNamesystem().getBlockManager
        ().getStoredBlock(block.getLocalBlock());
    cluster.getNameNode().getNamesystem().writeLock();
    cluster.getNameNode().getNamesystem().getBlockManager()
        .removeBlock(bInfo);
    cluster.getNameNode().getNamesystem().writeUnlock();

    // we also need to tell block manager that we are in the startup path
    FSNamesystem spyNameSystem = spy(cluster.getNameNode().getNamesystem());
    Whitebox.setInternalState(cluster.getNameNode()
            .getNamesystem().getBlockManager(),
        "namesystem", spyNameSystem);
    Whitebox.setInternalState(cluster.getNameNode(),
        "namesystem", spyNameSystem);
    Mockito.doReturn(true).when(spyNameSystem).isInStartupSafeMode();


    // Since Data Node is already shutdown we didn't remove blocks
    cluster.restartDataNode(dnProps);
    waitTil(TimeUnit.SECONDS.toMillis(SCAN_WAIT));
    cluster.triggerBlockReports();

    // Give some buffer to process the block reports
    waitTil(TimeUnit.SECONDS.toMillis(SCAN_WAIT));

    // Make sure that we find all written bytes in future block
    assertEquals(datalen, cluster.getNameNode().getBytesWithFutureGenerationStamps());

    // Assert safemode reason
    String safeModeMessage = cluster.getNameNode().getNamesystem()
        .getSafeModeTip();
    assertThat(safeModeMessage, CoreMatchers.containsString("Name node " +
        "detected blocks with generation stamps in future"));
  }

  /**
   * Pretty much the same tests as above but does not setup safeMode == true,
   * hence we should not have positive count of Blocks in future.
   */
  @Test
  public void testEnsureGenStampsIsStartupOnly() throws
      IOException, InterruptedException {

    String testData = " This is test data";
    cluster.restartDataNodes();
    cluster.restartNameNodes();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    OutputStream ostream = fs.create(filePath2);
    ostream.write(testData.getBytes());
    ostream.close();

    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath2);
    long genStamp = block.getGenerationStamp();

    // Re-write the Generation Stamp to a Generation Stamp in future.
    cluster.changeGenStampOfBlock(0, block, genStamp + 1);
    MiniDFSCluster.DataNodeProperties dnProps = cluster.stopDataNode(0);


    // Simulate  Namenode forgetting a Block
    cluster.restartNameNode(true);
    BlockInfo bInfo = cluster.getNameNode().getNamesystem().getBlockManager
        ().getStoredBlock(block.getLocalBlock());
    cluster.getNameNode().getNamesystem().writeLock();
    cluster.getNameNode().getNamesystem().getBlockManager()
        .removeBlock(bInfo);
    cluster.getNameNode().getNamesystem().writeUnlock();

    cluster.restartDataNode(dnProps);
    waitTil(TimeUnit.SECONDS.toMillis(SCAN_WAIT));
    cluster.triggerBlockReports();
    waitTil(TimeUnit.SECONDS.toMillis(SCAN_WAIT));


    // Make sure that there are no bytes in future since isInStartupSafe
    // mode is not true.
    assertEquals(0, cluster.getNameNode().getBytesWithFutureGenerationStamps());
  }

  private void waitTil(long waitPeriod) {
    try {
      Thread.sleep(waitPeriod);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}