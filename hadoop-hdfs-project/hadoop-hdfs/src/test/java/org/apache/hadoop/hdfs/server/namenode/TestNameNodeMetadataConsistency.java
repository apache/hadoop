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
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNameNodeMetadataConsistency {
  private static final Path filePath1 = new Path("/testdata1.txt");
  private static final Path filePath2 = new Path("/testdata2.txt");
  private static final String TEST_DATA_IN_FUTURE = "This is test data";

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
  public void testGenerationStampInFuture() throws Exception {
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    OutputStream ostream = fs.create(filePath1);
    ostream.write(TEST_DATA_IN_FUTURE.getBytes());
    ostream.close();

    // Re-write the Generation Stamp to a Generation Stamp in future.
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath1);
    final long genStamp = block.getGenerationStamp();
    final int datanodeIndex = 0;
    cluster.changeGenStampOfBlock(datanodeIndex, block, genStamp + 1);
    // stop the data node so that it won't remove block
    final DataNodeProperties dnProps = cluster.stopDataNode(datanodeIndex);

    // Simulate Namenode forgetting a Block
    cluster.restartNameNode(true);
    cluster.getNameNode().getNamesystem().writeLock();
    BlockInfo bInfo = cluster.getNameNode().getNamesystem().getBlockManager()
        .getStoredBlock(block.getLocalBlock());
    bInfo.delete();
    cluster.getNameNode().getNamesystem().getBlockManager()
        .removeBlock(bInfo);
    cluster.getNameNode().getNamesystem().writeUnlock();

    // we also need to tell block manager that we are in the startup path
    BlockManagerTestUtil.setStartupSafeModeForTest(
        cluster.getNameNode().getNamesystem().getBlockManager());

    cluster.restartDataNode(dnProps);
    waitForNumBytes(TEST_DATA_IN_FUTURE.length());

    // Make sure that we find all written bytes in future block
    assertEquals(TEST_DATA_IN_FUTURE.length(),
        cluster.getNameNode().getBytesWithFutureGenerationStamps());
    // Assert safemode reason
    assertTrue(cluster.getNameNode().getNamesystem().getSafeModeTip().contains(
        "Name node detected blocks with generation stamps in future"));
  }

  /**
   * Pretty much the same tests as above but does not setup safeMode == true,
   * hence we should not have positive count of Blocks in future.
   */
  @Test
  public void testEnsureGenStampsIsStartupOnly() throws Exception {
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
    bInfo.delete();
    cluster.getNameNode().getNamesystem().getBlockManager()
        .removeBlock(bInfo);
    cluster.getNameNode().getNamesystem().writeUnlock();

    cluster.restartDataNode(dnProps);
    waitForNumBytes(0);

    // Make sure that there are no bytes in future since isInStartupSafe
    // mode is not true.
    assertEquals(0, cluster.getNameNode().getBytesWithFutureGenerationStamps());
  }

  private void waitForNumBytes(final int numBytes) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          cluster.triggerBlockReports();
          // Compare the number of bytes
          if (cluster.getNameNode().getBytesWithFutureGenerationStamps()
              == numBytes) {
            return true;
          }
        } catch (Exception e) {
          // Ignore the exception
        }

        return false;
      }
    }, SCAN_WAIT * 1000, 60000);
  }
}