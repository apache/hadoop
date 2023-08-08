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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.event.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHAFsck {
  
  static {
    GenericTestUtils.setLogLevel(DFSUtil.LOG, Level.TRACE);
  }

  @Parameter
  private String proxyProvider;

  public String getProxyProvider() {
    return proxyProvider;
  }

  @Parameterized.Parameters(name = "ProxyProvider: {0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]
        {{ConfiguredFailoverProxyProvider.class.getName()},
        {RequestHedgingProxyProvider.class.getName()}});
  }

  /**
   * Test that fsck still works with HA enabled.
   */
  @Test
  public void testHaFsck() throws Exception {
    Configuration conf = new Configuration();
    
    // need some HTTP ports
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ha-nn-uri-0")
        .addNN(new MiniDFSNNTopology.NNConf("nn1").setHttpPort(10051))
        .addNN(new MiniDFSNNTopology.NNConf("nn2").setHttpPort(10052)));
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(topology)
      .numDataNodes(0)
      .build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
    
      cluster.transitionToActive(0);
      
      // Make sure conf has the relevant HA configs.
      HATestUtil.setFailoverConfigurations(cluster, conf, "ha-nn-uri-0", getProxyProvider(), 0);
      
      fs = FileSystem.get(conf);
      fs.mkdirs(new Path("/test1"));
      fs.mkdirs(new Path("/test2"));
      
      runFsck(conf);
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      runFsck(conf);
      // Stop one standby namenode, FSCK should still be successful, since there
      // is one Active namenode available
      cluster.getNameNode(0).stop();

      runFsck(conf);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void runFsck(Configuration conf) throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    int errCode = ToolRunner.run(new DFSck(conf, out),
        new String[]{"/", "-files"});
    String result = bStream.toString();
    System.out.println("output from fsck:\n" + result);
    assertEquals(0, errCode);
    assertTrue(result.contains("/test1"));
    assertTrue(result.contains("/test2"));
  }
}
