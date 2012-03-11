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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNNHealthCheck {

  @Test
  public void testNNHealthCheck() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .build();

      NameNodeResourceChecker mockResourceChecker = Mockito.mock(
          NameNodeResourceChecker.class);
      Mockito.doReturn(true).when(mockResourceChecker).hasAvailableDiskSpace();
      cluster.getNameNode(0).getNamesystem()
          .setNNResourceChecker(mockResourceChecker);
      
      NamenodeProtocols rpc = cluster.getNameNodeRpc(0);
      
      // Should not throw error, which indicates healthy.
      rpc.monitorHealth();
      
      Mockito.doReturn(false).when(mockResourceChecker).hasAvailableDiskSpace();
      
      try {
        // Should throw error - NN is unhealthy.
        rpc.monitorHealth();
        fail("Should not have succeeded in calling monitorHealth");
      } catch (HealthCheckFailedException hcfe) {
        GenericTestUtils.assertExceptionContains(
            "The NameNode has no resources available", hcfe);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
