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

package org.apache.hadoop.ozone.scm.node;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.test.PathUtils;

import static org.apache.hadoop.ozone.scm.TestUtils.getDatanodeIDs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for SCM node pool manager.
 */
public class TestSCMNodePoolManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMNodePoolManager.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final File testDir = PathUtils.getTestDir(
      TestSCMNodePoolManager.class);

  SCMNodePoolManager createNodePoolManager(OzoneConfiguration conf)
      throws IOException {
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    return new SCMNodePoolManager(conf);
  }

  /**
   * Test default node pool.
   *
   * @throws IOException
   */
  @Test
  public void testDefaultNodePool() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    try {
      final String defaultPool = "DefaultPool";
      NodePoolManager npMgr = createNodePoolManager(conf);

      final int nodeCount = 4;
      final List<DatanodeID> nodes = getDatanodeIDs(nodeCount);
      assertEquals(0, npMgr.getNodePools().size());
      for (DatanodeID node: nodes) {
        npMgr.addNode(defaultPool, node);
      }
      List<DatanodeID> nodesRetrieved = npMgr.getNodes(defaultPool);
      assertEquals(nodeCount, nodesRetrieved.size());
      assertTwoDatanodeListsEqual(nodes, nodesRetrieved);

      DatanodeID nodeRemoved = nodes.remove(2);
      npMgr.removeNode(defaultPool, nodeRemoved);
      List<DatanodeID> nodesAfterRemove = npMgr.getNodes(defaultPool);
      assertTwoDatanodeListsEqual(nodes, nodesAfterRemove);

      List<DatanodeID> nonExistSet = npMgr.getNodes("NonExistSet");
      assertEquals(0, nonExistSet.size());
    } finally {
      FileUtil.fullyDelete(testDir);
    }
  }


  /**
   * Test default node pool reload.
   *
   * @throws IOException
   */
  @Test
  public void testDefaultNodePoolReload() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String defaultPool = "DefaultPool";
    final int nodeCount = 4;
    final List<DatanodeID> nodes = getDatanodeIDs(nodeCount);

    try {
      try {
        SCMNodePoolManager npMgr = createNodePoolManager(conf);
        assertEquals(0, npMgr.getNodePools().size());
        for (DatanodeID node : nodes) {
          npMgr.addNode(defaultPool, node);
        }
        List<DatanodeID> nodesRetrieved = npMgr.getNodes(defaultPool);
        assertEquals(nodeCount, nodesRetrieved.size());
        assertTwoDatanodeListsEqual(nodes, nodesRetrieved);
        npMgr.close();
      } finally {
        LOG.info("testDefaultNodePoolReload: Finish adding nodes to pool" +
            " and close.");
      }

      // try reload with a new NodePoolManager instance
      try {
        SCMNodePoolManager npMgr = createNodePoolManager(conf);
        List<DatanodeID> nodesRetrieved = npMgr.getNodes(defaultPool);
        assertEquals(nodeCount, nodesRetrieved.size());
        assertTwoDatanodeListsEqual(nodes, nodesRetrieved);
      } finally {
        LOG.info("testDefaultNodePoolReload: Finish reloading node pool.");
      }
    } finally {
      FileUtil.fullyDelete(testDir);
    }
  }

  /**
   * Compare and verify that two datanode lists are equal.
   * @param list1 - datanode list 1.
   * @param list2 - datanode list 2.
   */
  private void assertTwoDatanodeListsEqual(List<DatanodeID> list1,
      List<DatanodeID> list2) {
    assertEquals(list1.size(), list2.size());
    Collections.sort(list1);
    Collections.sort(list2);
    assertTrue(ListUtils.isEqualList(list1, list2));
  }
}
