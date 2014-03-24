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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSecondaryWebUi {
  
  private static MiniDFSCluster cluster;
  private static SecondaryNameNode snn;
  private static final Configuration conf = new Configuration();
  
  @BeforeClass
  public static void setUpCluster() throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        "0.0.0.0:0");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .build();
    cluster.waitActive();
    
    snn = new SecondaryNameNode(conf);
  }
  
  @AfterClass
  public static void shutDownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (snn != null) {
      snn.shutdown();
    }
  }

  @Test
  public void testSecondaryWebUi() throws IOException {
    String pageContents = DFSTestUtil.urlGet(new URL("http://localhost:" +
        SecondaryNameNode.getHttpAddress(conf).getPort() + "/status.jsp"));
    assertTrue(pageContents.contains("Last Checkpoint Time"));
  }
  
  @Test
  public void testSecondaryWebJmx() throws MalformedURLException, IOException {
    String pageContents = DFSTestUtil.urlGet(new URL("http://localhost:" +
        SecondaryNameNode.getHttpAddress(conf).getPort() + "/jmx"));
    assertTrue(pageContents.contains(
        "Hadoop:service=SecondaryNameNode,name=JvmMetrics"));
  }
}
