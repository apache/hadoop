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

package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BPOfferService;
import org.junit.Test;

/**
 * Tests datanode refresh namenode list functionality.
 */
public class TestRefreshNamenodes {
  private int nnPort1 = 2221;
  private int nnPort2 = 2224;
  private int nnPort3 = 2227;
  private int nnPort4 = 2230;

  @Test
  public void testRefreshNamenodes() throws IOException {
    // Start cluster with a single NN and DN
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numNameNodes(1)
          .nameNodePort(nnPort1).build();

      DataNode dn = cluster.getDataNodes().get(0);
      assertEquals(1, dn.getAllBpOs().length);

      cluster.addNameNode(conf, nnPort2);
      assertEquals(2, dn.getAllBpOs().length);

      cluster.addNameNode(conf, nnPort3);
      assertEquals(3, dn.getAllBpOs().length);

      cluster.addNameNode(conf, nnPort4);

      BPOfferService[] bpoList = dn.getAllBpOs();
      // Ensure a BPOfferService in the datanodes corresponds to
      // a namenode in the cluster
      for (int i = 0; i < 4; i++) {
        InetSocketAddress addr = cluster.getNameNode(i).getNameNodeAddress();
        boolean found = false;
        for (int j = 0; j < bpoList.length; j++) {
          if (bpoList[j] != null && addr.equals(bpoList[j].nnAddr)) {
            found = true;
            bpoList[j] = null; // Erase the address that matched
            break;
          }
        }
        assertTrue("NameNode address " + addr + " is not found.", found);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
