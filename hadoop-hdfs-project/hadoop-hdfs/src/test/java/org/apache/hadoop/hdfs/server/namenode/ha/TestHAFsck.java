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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestHAFsck {
  
  static {
    ((Log4JLogger)LogFactory.getLog(DFSUtil.class)).getLogger().setLevel(Level.ALL);
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
      HATestUtil.setFailoverConfigurations(cluster, conf, "ha-nn-uri-0", 0);
      
      fs = HATestUtil.configureFailoverFs(cluster, conf);
      fs.mkdirs(new Path("/test1"));
      fs.mkdirs(new Path("/test2"));
      
      runFsck(conf);
      
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      
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
