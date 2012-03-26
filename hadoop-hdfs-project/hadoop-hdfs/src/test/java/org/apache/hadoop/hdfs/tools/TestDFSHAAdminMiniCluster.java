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
package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

/**
 * Tests for HAAdmin command with {@link MiniDFSCluster} set up in HA mode.
 */
public class TestDFSHAAdminMiniCluster {
  static {
    ((Log4JLogger)LogFactory.getLog(HAAdmin.class)).getLogger().setLevel(
        Level.ALL);
  }
  private static final Log LOG = LogFactory.getLog(TestDFSHAAdminMiniCluster.class);
  
  private MiniDFSCluster cluster;
  private Configuration conf; 
  private DFSHAAdmin tool;
  private ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();

  private String errOutput;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(0)
        .build();
    tool = new DFSHAAdmin();  
    tool.setConf(conf);
    tool.setErrOut(new PrintStream(errOutBytes));
    cluster.waitActive();
  }

  @After
  public void shutdown() throws Exception {
    cluster.shutdown();
  }
  
  @Test
  public void testGetServiceState() throws Exception {
    assertEquals(0, runTool("-getServiceState", "nn1"));
    assertEquals(0, runTool("-getServiceState", "nn2"));
    
    cluster.transitionToActive(0);
    assertEquals(0, runTool("-getServiceState", "nn1"));
    
    NameNodeAdapter.enterSafeMode(cluster.getNameNode(0), false);
    assertEquals(0, runTool("-getServiceState", "nn1"));
  }
    
  @Test 
  public void testStateTransition() throws Exception {
    NameNode nnode1 = cluster.getNameNode(0);
    assertTrue(nnode1.isStandbyState());
    assertEquals(0, runTool("-transitionToActive", "nn1"));
    assertFalse(nnode1.isStandbyState());       
    assertEquals(0, runTool("-transitionToStandby", "nn1"));
    assertTrue(nnode1.isStandbyState());
    
    NameNode nnode2 = cluster.getNameNode(1);
    assertTrue(nnode2.isStandbyState());
    assertEquals(0, runTool("-transitionToActive", "nn2"));
    assertFalse(nnode2.isStandbyState());
    assertEquals(0, runTool("-transitionToStandby", "nn2"));
    assertTrue(nnode2.isStandbyState());
  }
  
  @Test
  public void testTryFailoverToSafeMode() throws Exception {
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);

    NameNodeAdapter.enterSafeMode(cluster.getNameNode(0), false);
    assertEquals(-1, runTool("-failover", "nn2", "nn1"));
    assertTrue("Bad output: " + errOutput,
        errOutput.contains("is not ready to become active: " +
            "The NameNode is in safemode"));
  }
    
  /**
   * Test failover with various options
   */
  @Test
  public void testFencer() throws Exception { 
    // Test failover with no fencer
    assertEquals(-1, runTool("-failover", "nn1", "nn2"));
    
    // Test failover with fencer
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(0, runTool("-transitionToActive", "nn1"));
    assertEquals(0, runTool("-failover", "nn1", "nn2"));
    
    // Test failover with fencer and nameservice
    assertEquals(0, runTool("-ns", "minidfs-ns", "-failover", "nn2", "nn1"));

    // Test failover with fencer and forcefence option
    assertEquals(0, runTool("-failover", "nn1", "nn2", "--forcefence"));
      
    // Test failover with forceactive option
    assertEquals(0, runTool("-failover", "nn2", "nn1", "--forceactive"));
          
    // Test failover with not fencer and forcefence option
    conf.unset(NodeFencer.CONF_METHODS_KEY);
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "nn1", "nn2", "--forcefence"));
    
    // Test failover with bad fencer and forcefence option
    conf.set(NodeFencer.CONF_METHODS_KEY, "foobar!");
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "nn1", "nn2", "--forcefence"));

    // Test failover with force fence listed before the other arguments
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(0, runTool("-failover", "--forcefence", "nn1", "nn2"));
  }
     
  @Test
  public void testCheckHealth() throws Exception {
    assertEquals(0, runTool("-checkHealth", "nn1"));
    assertEquals(0, runTool("-checkHealth", "nn2"));
  }
  
  private int runTool(String ... args) throws Exception {
    errOutBytes.reset();
    LOG.info("Running: DFSHAAdmin " + Joiner.on(" ").join(args));
    int ret = tool.run(args);
    errOutput = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
    LOG.info("Output:\n" + errOutput);
    return ret;
  }
}
