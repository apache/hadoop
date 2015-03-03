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

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class TestNameNodePrunesMissingStorages {
  static final Log LOG = LogFactory.getLog(TestNameNodePrunesMissingStorages.class);


  private static void runTest(final String testCaseName,
                              final boolean createFiles,
                              final int numInitialStorages,
                              final int expectedStoragesAfterTest) throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(1)
          .storagesPerDatanode(numInitialStorages)
          .build();
      cluster.waitActive();

      final DataNode dn0 = cluster.getDataNodes().get(0);

      // Ensure NN knows about the storage.
      final DatanodeID dnId = dn0.getDatanodeId();
      final DatanodeDescriptor dnDescriptor =
          cluster.getNamesystem().getBlockManager().getDatanodeManager().getDatanode(dnId);
      assertThat(dnDescriptor.getStorageInfos().length, is(numInitialStorages));

      final String bpid = cluster.getNamesystem().getBlockPoolId();
      final DatanodeRegistration dnReg = dn0.getDNRegistrationForBP(bpid);
      DataNodeTestUtils.triggerBlockReport(dn0);

      if (createFiles) {
        final Path path = new Path("/", testCaseName);
        DFSTestUtil.createFile(
            cluster.getFileSystem(), path, 1024, (short) 1, 0x1BAD5EED);
        DataNodeTestUtils.triggerBlockReport(dn0);
      }

      // Generate a fake StorageReport that is missing one storage.
      final StorageReport reports[] =
          dn0.getFSDataset().getStorageReports(bpid);
      final StorageReport prunedReports[] = new StorageReport[numInitialStorages - 1];
      System.arraycopy(reports, 0, prunedReports, 0, prunedReports.length);

      // Stop the DataNode and send fake heartbeat with missing storage.
      cluster.stopDataNode(0);
      cluster.getNameNodeRpc().sendHeartbeat(dnReg, prunedReports, 0L, 0L, 0, 0,
          0, null);

      // Check that the missing storage was pruned.
      assertThat(dnDescriptor.getStorageInfos().length, is(expectedStoragesAfterTest));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that the NameNode prunes empty storage volumes that are no longer
   * reported by the DataNode.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testUnusedStorageIsPruned() throws IOException {
    // Run the test with 1 storage, after the text expect 0 storages.
    runTest(GenericTestUtils.getMethodName(), false, 1, 0);
  }

  /**
   * Verify that the NameNode does not prune storages with blocks.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testStorageWithBlocksIsNotPruned() throws IOException {
    // Run the test with 1 storage, after the text still expect 1 storage.
    runTest(GenericTestUtils.getMethodName(), true, 1, 1);
  }
}
