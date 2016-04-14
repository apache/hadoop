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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Supplier;

import org.junit.Test;

public class TestPendingCorruptDnMessages {
  
  private static final Path filePath = new Path("/foo.txt");
  
  @Test (timeout = 60000)
  public void testChangedStorageId() throws IOException, URISyntaxException,
      InterruptedException, TimeoutException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .build();
    
    try {
      cluster.transitionToActive(0);
      
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      OutputStream out = fs.create(filePath);
      out.write("foo bar baz".getBytes());
      out.close();
      
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
          cluster.getNameNode(1));
      
      // Change the gen stamp of the block on datanode to go back in time (gen
      // stamps start at 1000)
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);
      cluster.changeGenStampOfBlock(0, block, 900);
      
      // Run directory dsscanner to update Datanode's volumeMap
      DataNodeTestUtils.runDirectoryScanner(cluster.getDataNodes().get(0));
      // Stop the DN so the replica with the changed gen stamp will be reported
      // when this DN starts up.
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      
      // Restart the namenode so that when the DN comes up it will see an initial
      // block report.
      cluster.restartNameNode(1, false);
      assertTrue(cluster.restartDataNode(dnProps, true));
      
      // Wait until the standby NN queues up the corrupt block in the pending DN
      // message queue.
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return cluster.getNamesystem(1).getBlockManager()
              .getPendingDataNodeMessageCount() == 1;
        }
      }, 1000, 30000);

      final String oldStorageId = getRegisteredDatanodeUid(cluster, 1);
      assertNotNull(oldStorageId);
      
      // Reformat/restart the DN.
      assertTrue(wipeAndRestartDn(cluster, 0));
      
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          final String newStorageId = getRegisteredDatanodeUid(cluster, 1);
          return newStorageId != null && !newStorageId.equals(oldStorageId);
        }
      }, 1000, 30000);
      
      assertEquals(0, cluster.getNamesystem(1).getBlockManager()
          .getPendingDataNodeMessageCount());
      
      // Now try to fail over.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
    } finally {
      cluster.shutdown();
    }
  }
  
  private static String getRegisteredDatanodeUid(
      MiniDFSCluster cluster, int nnIndex) {
    List<DatanodeDescriptor> registeredDatanodes = cluster.getNamesystem(nnIndex)
        .getBlockManager().getDatanodeManager()
        .getDatanodeListForReport(DatanodeReportType.ALL);
    return registeredDatanodes.isEmpty() ? null :
        registeredDatanodes.get(0).getDatanodeUuid();
  }
  
  private static boolean wipeAndRestartDn(MiniDFSCluster cluster, int dnIndex)
      throws IOException {
    // stop the DN, reformat it, then start it again with the same xfer port.
    DataNodeProperties dnProps = cluster.stopDataNode(dnIndex);
    cluster.formatDataNodeDirs();
    return cluster.restartDataNode(dnProps, true);
  }

}
