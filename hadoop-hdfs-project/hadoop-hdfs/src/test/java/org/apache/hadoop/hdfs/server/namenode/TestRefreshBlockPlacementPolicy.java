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
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Test refresh block placement policy.
 */
public class TestRefreshBlockPlacementPolicy {
  private MiniDFSCluster cluster;
  private Configuration config;
  private static int counter = 0;
  static class MockBlockPlacementPolicy extends BlockPlacementPolicyDefault {
    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath,
        int numOfReplicas,
        Node writer,
        List<DatanodeStorageInfo> chosen,
        boolean returnChosenNodes,
        Set<Node> excludedNodes,
        long blocksize,
        BlockStoragePolicy storagePolicy,
        EnumSet<AddBlockFlag> flags) {
      counter++;
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosen,
          returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
    }
  }

  @Before
  public void setup() throws IOException {
    config = new Configuration();
    config.setClass(DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        MockBlockPlacementPolicy.class, BlockPlacementPolicy.class);
    config.setClass(DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
        MockBlockPlacementPolicy.class, BlockPlacementPolicy.class);
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(9).build();
    cluster.waitActive();
  }

  @After
  public void cleanup() throws IOException {
    cluster.shutdown();
  }

  @Test
  public void testRefreshReplicationPolicy() throws Exception {
    Path file = new Path("/test-file");
    DistributedFileSystem dfs = cluster.getFileSystem();

    verifyRefreshPolicy(dfs, file, () -> cluster.getNameNode()
        .reconfigurePropertyImpl(DFS_BLOCK_REPLICATOR_CLASSNAME_KEY, null));
  }

  @Test
  public void testRefreshEcPolicy() throws Exception {
    Path ecDir = new Path("/ec");
    Path file = new Path("/ec/test-file");
    DistributedFileSystem dfs = cluster.getFileSystem();
    dfs.mkdir(ecDir, FsPermission.createImmutable((short)755));
    dfs.setErasureCodingPolicy(ecDir, null);

    verifyRefreshPolicy(dfs, file, () -> cluster.getNameNode()
        .reconfigurePropertyImpl(DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY, null));
  }

  @FunctionalInterface
  private interface Refresh {
    void refresh() throws ReconfigurationException;
  }

  private void verifyRefreshPolicy(DistributedFileSystem dfs, Path file,
      Refresh func) throws IOException, ReconfigurationException {
    // Choose datanode using the mock policy.
    int lastCounter = counter;
    OutputStream out = dfs.create(file, true);
    out.write("test".getBytes());
    out.close();
    assert(counter > lastCounter);

    // Refresh to the default policy.
    func.refresh();

    lastCounter = counter;
    dfs.delete(file, true);
    out = dfs.create(file, true);
    out.write("test".getBytes());
    out.close();
    assertEquals(lastCounter, counter);
  }
}
