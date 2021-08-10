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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

public class TestVolumeUsageStdDev {
  private final static int numDataNodes = 5;
  private final static int storagesPerDatanode = 3;
  private final static int defaultBlockSize = 102400;
  private final static int bufferLen = 1024;
  private static Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);

    long capacity = 8 * defaultBlockSize;
    long[][] capacities = new long[numDataNodes][storagesPerDatanode];
    String[] hostnames = new String[5];
    for (int i = 0; i < numDataNodes; i++) {
      hostnames[i] = i + "." + i + "." + i + "." + i;
      for(int j=0;j<storagesPerDatanode;j++){
        capacities[i][j]=capacity;
      }
    }

    cluster = new MiniDFSCluster.Builder(conf)
        .hosts(hostnames)
        .numDataNodes(numDataNodes)
        .storagesPerDatanode(storagesPerDatanode)
        .storageCapacities(capacities).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  /**
   * Create files of different sizes for each datanode.
   * Ensure that the file size is smaller than the blocksize
   * and only one block is generated. In this way, data will
   * be written to only one volume.
   *
   * Using favoredNodes, we can write files of a specified size
   * to specified datanodes to create a batch of datanodes with
   * different volumeUsageStdDev.
   *
   * Then, we assert the value of volumeUsageStdDev on namenode
   * and datanode is the same.
   */
  @Test
  public void testVolumeUsageStdDev() throws IOException {
    // Create file for each datanode.
    ArrayList<DataNode> dataNodes = cluster.getDataNodes();
    DFSTestUtil.createFile(fs, new Path("/file0"), false, bufferLen, 1000,
        defaultBlockSize, (short) 1, 0, false,
        new InetSocketAddress[]{dataNodes.get(0).getXferAddress()});
    DFSTestUtil.createFile(fs, new Path("/file1"), false, bufferLen, 2000,
        defaultBlockSize, (short) 1, 0, false,
        new InetSocketAddress[]{dataNodes.get(1).getXferAddress()});
    DFSTestUtil.createFile(fs, new Path("/file2"), false, bufferLen, 4000,
        defaultBlockSize, (short) 1, 0, false,
        new InetSocketAddress[]{dataNodes.get(2).getXferAddress()});
    DFSTestUtil.createFile(fs, new Path("/file3"), false, bufferLen, 8000,
        defaultBlockSize, (short) 1, 0, false,
        new InetSocketAddress[]{dataNodes.get(3).getXferAddress()});
    DFSTestUtil.createFile(fs, new Path("/file4"), false, bufferLen, 16000,
        defaultBlockSize, (short) 1, 0, false,
        new InetSocketAddress[]{dataNodes.get(4).getXferAddress()});

    // Trigger Heartbeats.
    cluster.triggerHeartbeats();

    // Assert that the volumeUsageStdDev on namenode and Datanode are the same.
    String liveNodes = cluster.getNameNode().getNamesystem().getLiveNodes();
    Map<String, Map<String, Object>> info =
        (Map<String, Map<String, Object>>) JSON.parse(liveNodes);
    for (DataNode dataNode : dataNodes) {
      String dnAddress = dataNode.getDisplayName();
      float volumeUsageStdDev = dataNode.getFSDataset().getVolumeUsageStdDev();
      Assert.assertEquals(
          String.valueOf(volumeUsageStdDev),
          String.valueOf(info.get(dnAddress).get("volumeUsageStdDev")));
    }
  }
}
