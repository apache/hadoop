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

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.contract.hdfs.HDFSContract.BLOCK_SIZE;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * This class tests RedundancyMonitor in BlockManager.
 */
public class TestRedundancyMonitor {
  private static final String FILENAME = "/dummyfile.txt";

  /**
   * RedundancyMonitor invoke choose target out of global lock when
   * #computeDatanodeWork. However it may result in NN terminate when choose
   * target meet runtime exception(ArithmeticException) since we stop all
   * DataNodes during that time.
   * Verify that NN should not terminate even stop all datanodes.
   */
  @Test
  public void testChooseTargetWhenAllDataNodesStop() throws Throwable {

    HdfsConfiguration conf = new HdfsConfiguration();
    String[] hosts = new String[]{"host1", "host2"};
    String[] racks = new String[]{"/d1/r1", "/d1/r1"};
    try (MiniDFSCluster miniCluster = new MiniDFSCluster.Builder(conf)
        .racks(racks).hosts(hosts).numDataNodes(hosts.length).build()) {
      miniCluster.waitActive();

      FSNamesystem fsn = miniCluster.getNamesystem();
      BlockManager blockManager = fsn.getBlockManager();

      BlockPlacementPolicyDefault replicator
          = (BlockPlacementPolicyDefault) blockManager
              .getBlockPlacementPolicy();
      Set<DatanodeDescriptor> dns = blockManager.getDatanodeManager()
          .getDatanodes();

      DelayAnswer delayer = new DelayAnswer(BlockPlacementPolicyDefault.LOG);
      NetworkTopology clusterMap = replicator.clusterMap;
      NetworkTopology spyClusterMap = spy(clusterMap);
      replicator.clusterMap = spyClusterMap;
      doAnswer(delayer).when(spyClusterMap).getNumOfRacks();

      ExecutorService pool = Executors.newFixedThreadPool(2);

      // Trigger chooseTarget
      Future<Void> chooseTargetFuture = pool.submit(() -> {
        replicator.chooseTarget(FILENAME, 2, dns.iterator().next(),
            new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
            TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
        return null;
      });

      // Wait until chooseTarget calls NetworkTopology#getNumOfRacks
      delayer.waitForCall();
      // Remove all DataNodes
      Future<Void> stopDatanodesFuture = pool.submit(() -> {
        for (DatanodeDescriptor dn : dns) {
          spyClusterMap.remove(dn);
        }
        return null;
      });
      // Wait stopDatanodesFuture run finish
      stopDatanodesFuture.get();

      // Allow chooseTarget to proceed
      delayer.proceed();
      try {
        chooseTargetFuture.get();
      } catch (ExecutionException ee) {
        throw ee.getCause();
      }
    }
  }
}
