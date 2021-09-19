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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link INodeMap}.
 */
public class TestINodeMap {
  public static final Logger LOG =
    LoggerFactory.getLogger(TestINodeMap.class);

  @Test
  public void testSpaceKeyDepthAndNumRanges() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_INOD_NAMESPACE_KEY_DEPTH, 3);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_INOD_NUM_RANGES, 128);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      FSDirectory fsDirectory = new FSDirectory(fsn, conf);
      assertTrue(fsDirectory.getINodeMap() != null);
      assertEquals(INodeMap.getNumRanges(), 128);
      assertEquals(INodeMap.getNumSpaceKeyDepth(), 3);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRangeKeys() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_INOD_NAMESPACE_KEY_DEPTH, 4);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_INOD_NUM_RANGES, 8);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      FSDirectory fsDirectory = new FSDirectory(fsn, conf);
      INodeMap iNodeMap = fsDirectory.getINodeMap();
      int level = 0;
      for (Iterator<INode> it = iNodeMap.rangeKeys().iterator(); it.hasNext();) {
        INode inode = it.next();
        long[] namespaceKey = inode.getNamespaceKey(4);
        assertEquals(level, namespaceKey[0]);
        assertEquals(0, namespaceKey[1]);
        assertEquals(0, namespaceKey[2]);
        assertEquals(INodeId.ROOT_INODE_ID, namespaceKey[3]);
        level++;
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
