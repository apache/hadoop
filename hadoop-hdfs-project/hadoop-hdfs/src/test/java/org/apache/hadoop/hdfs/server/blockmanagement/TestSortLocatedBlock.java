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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSortLocatedBlock {

  private Configuration conf = new HdfsConfiguration();

  @Before
  public void setup() throws Exception {
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.setStrings(DFSConfigKeys
        .FS_DEFAULT_NAME_KEY,
        "hdfs://localhost:0");
    conf.setLong(DFSConfigKeys
        .DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        60000);
  }

  /**
   * Test sorting locations.
   * @throws Exception
   */
  @Test
  public void testSortLocatedBlockAvoidStaleNodes() throws Exception {
    conf.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    MiniDFSCluster miniCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    FSNamesystem namesystem = miniCluster.getNamesystem();
    DatanodeManager dnManager =
        namesystem.getBlockManager().getDatanodeManager();
    List<DatanodeDescriptor> datanodes =
        dnManager.getDatanodeListForReport(DatanodeReportType.ALL);
    DFSTestUtil.formatNameNode(conf);
    NamenodeProtocols rpcServer = miniCluster.getNameNodeRpc();
    DistributedFileSystem fileSystem = miniCluster.getFileSystem();
    miniCluster.waitActive();
    miniCluster.triggerHeartbeats();

    // create file with 5 replication
    String path = "/test";
    final long fileLen = 512;
    DFSTestUtil.createFile(fileSystem, new Path(path),
        fileLen, (short)5, 0);

    DatanodeDescriptor dn0 = datanodes.get(0);
    DatanodeDescriptor dn1 = datanodes.get(1);
    DatanodeDescriptor dn2 = datanodes.get(2);

    miniCluster.getNamesystem().writeLock();
    // mock decommissioned node
    dn0.setDecommissioned();
    // mock maintained node
    dn1.startMaintenance();
    // mock stale
    dn2.setLastUpdateMonotonic(Time.monotonicNow() -
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT * 1000 - 1);
    miniCluster.getNamesystem().writeUnlock();

    final LocatedBlocks lb = rpcServer.getBlockLocations(path, 0, fileLen);
    LocatedBlock locatedBlock = lb.getLocatedBlocks().get(0);
    DatanodeInfo[] locations = locatedBlock.getLocations();

    // assert location order:
    // live -> entering_maintenance -> decommissioned
    // decommissioned
    assertEquals(dn0.getDatanodeUuid(), locations[4].getDatanodeUuid());
    // entering_maintenance
    assertEquals(dn1.getDatanodeUuid(), locations[3].getDatanodeUuid());
    // stale
    assertEquals(dn2.getDatanodeUuid(), locations[2].getDatanodeUuid());
  }
}
