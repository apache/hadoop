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

package org.apache.hadoop.hdfs.server.diskbalancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Test Class that tests connectors.
 */
public class TestConnectors {
  private MiniDFSCluster cluster;
  private final int numDatanodes = 3;
  private final int volumeCount = 2; // default volumes in MiniDFSCluster.
  private Configuration conf;

  @BeforeEach
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes).build();
  }

  @AfterEach
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testNameNodeConnector() throws Exception {
    cluster.waitActive();
    ClusterConnector nameNodeConnector =
        ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);
    DiskBalancerCluster diskBalancerCluster =
        new DiskBalancerCluster(nameNodeConnector);
    diskBalancerCluster.readClusterInfo();
      Assertions.assertEquals(
              numDatanodes, diskBalancerCluster.getNodes().size(), "Expected number of Datanodes not found.");
      Assertions.assertEquals(
              volumeCount, diskBalancerCluster.getNodes().get(0).getVolumeCount(), "Expected number of volumes not found.");
  }

  @Test
  public void testJsonConnector() throws Exception {
    cluster.waitActive();
    ClusterConnector nameNodeConnector =
        ConnectorFactory.getCluster(cluster.getFileSystem(0).getUri(), conf);
    DiskBalancerCluster diskBalancerCluster =
        new DiskBalancerCluster(nameNodeConnector);
    diskBalancerCluster.readClusterInfo();
    String diskBalancerJson = diskBalancerCluster.toJson();
    DiskBalancerCluster serializedCluster =
        DiskBalancerCluster.parseJson(diskBalancerJson);
      Assertions.assertEquals(
              diskBalancerCluster.getNodes().size(),
              serializedCluster.getNodes().size(), "Parsed cluster is not equal to persisted info.");
  }
}
