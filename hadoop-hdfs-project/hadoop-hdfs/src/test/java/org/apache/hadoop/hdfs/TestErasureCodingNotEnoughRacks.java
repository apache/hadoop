/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY;

/**
 * Test erasure coding when racks not enough.
 */
public class TestErasureCodingNotEnoughRacks {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestErasureCodingNotEnoughRacks.class);

  private MiniDFSCluster cluster;
  private ErasureCodingPolicy ecPolicy;
  private DistributedFileSystem dfs;
  private int blockSize;
  private final int fileSize = 10 * 1024 * 1024; // 10 MiB.

  @Before
  public void setup() throws Exception {
    ecPolicy = StripedFileTestUtil.getDefaultECPolicy(); // RS_6_3
    blockSize = ecPolicy.getCellSize() * 2;
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY, false);
    cluster = DFSTestUtil.setupCluster(conf, 12, 6, 0);
    dfs = cluster.getFileSystem();
    dfs.setErasureCodingPolicy(new Path("/"), ecPolicy.getName());
  }

  @Test(timeout = 30000)
  public void testBasic() throws Exception {
    Path ecFile = new Path("/foo1");
    writeFile(ecFile);

    StripedFileTestUtil.waitBlockGroupsReported(dfs, ecFile.toString());
    checkFile(ecFile);
  }

  @Test(timeout = 30000)
  public void testDecommissionOneNode() throws Exception {
    Path ecFile = new Path("/foo2");
    writeFile(ecFile);

    BlockManager blockManager = cluster.getNameNode().getNamesystem().getBlockManager();
    DatanodeManager datanodeManager = blockManager.getDatanodeManager();

    Map<String, List<DatanodeDescriptor>> racksMap = getRacksMap(datanodeManager.getDatanodes());
    DatanodeDescriptor node = racksMap.values().iterator().next().get(0);

    datanodeManager.getDatanodeAdminManager().startDecommission(node);
    waitNodeDecommissioned(node);

    checkFile(ecFile);
  }

  @Test(timeout = 30000)
  public void testDecommissionOneRack() throws Exception {
    Path ecFile = new Path("/foo3");
    writeFile(ecFile);

    BlockManager blockManager = cluster.getNameNode().getNamesystem().getBlockManager();
    DatanodeManager datanodeManager = blockManager.getDatanodeManager();

    Map<String, List<DatanodeDescriptor>> racksMap = getRacksMap(datanodeManager.getDatanodes());
    List<DatanodeDescriptor> nodes = racksMap.values().iterator().next();

    for (DatanodeDescriptor node : nodes) {
      datanodeManager.getDatanodeAdminManager().startDecommission(node);
    }

    for (DatanodeDescriptor node : nodes) {
      waitNodeDecommissioned(node);
    }

    checkFile(ecFile);
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private Map<String, List<DatanodeDescriptor>> getRacksMap(Collection<DatanodeDescriptor> nodes) {
    Map<String, List<DatanodeDescriptor>> racksMap = new HashMap<>();
    for (DatanodeDescriptor dn : nodes) {
      racksMap.computeIfAbsent(dn.getNetworkLocation(), k -> new ArrayList<>()).add(dn);
    }
    return racksMap;
  }

  private void writeFile(Path ecFile) throws IOException {
    byte[] bytes = StripedFileTestUtil.generateBytes(fileSize);
    DFSTestUtil.writeFile(dfs, ecFile, new String(bytes));
  }

  private void checkFile(Path ecFile) throws Exception {
    StripedFileTestUtil.checkData(dfs, ecFile, fileSize, new ArrayList<>(),
        null, ecPolicy.getNumDataUnits() * blockSize);
  }

  private void waitNodeDecommissioned(DatanodeInfo node) {
    DatanodeInfo.AdminStates state = DatanodeInfo.AdminStates.DECOMMISSIONED;
    boolean done = (state == node.getAdminState());
    while (!done) {
      LOG.info("Waiting for node " + node + " to change state to "
          + state + " current state: " + node.getAdminState());
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // nothing
      }
      done = (state == node.getAdminState());
    }
  }

}
