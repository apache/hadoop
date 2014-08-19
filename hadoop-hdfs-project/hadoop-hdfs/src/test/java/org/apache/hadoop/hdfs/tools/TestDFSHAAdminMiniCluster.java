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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAAdmin;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.util.Shell;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

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
  private final ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();

  private String errOutput;

  private int nn1Port;

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
    
    nn1Port = cluster.getNameNodePort(0);
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
    conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY, 
             TestDFSHAAdmin.getFencerTrueCommand());
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

    // Set up fencer to write info about the fencing target into a
    // tmp file, so we can verify that the args were substituted right
    File tmpFile = File.createTempFile("testFencer", ".txt");
    tmpFile.deleteOnExit();
    if (Shell.WINDOWS) {
      conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
          "shell(echo %target_nameserviceid%.%target_namenodeid% " +
              "%target_port% %dfs_ha_namenode_id% > " +
              tmpFile.getAbsolutePath() + ")");
    } else {
      conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY,
          "shell(echo -n $target_nameserviceid.$target_namenodeid " +
          "$target_port $dfs_ha_namenode_id > " +
          tmpFile.getAbsolutePath() + ")");
    }

    // Test failover with fencer
    tool.setConf(conf);
    assertEquals(0, runTool("-transitionToActive", "nn1"));
    assertEquals(0, runTool("-failover", "nn1", "nn2"));
    
    // Test failover with fencer and nameservice
    assertEquals(0, runTool("-ns", "minidfs-ns", "-failover", "nn2", "nn1"));

    // Fencer has not run yet, since none of the above required fencing 
    assertEquals("", Files.toString(tmpFile, Charsets.UTF_8));

    // Test failover with fencer and forcefence option
    assertEquals(0, runTool("-failover", "nn1", "nn2", "--forcefence"));
    
    // The fence script should run with the configuration from the target
    // node, rather than the configuration from the fencing node. Strip
    // out any trailing spaces and CR/LFs which may be present on Windows.
    String fenceCommandOutput =Files.toString(tmpFile, Charsets.UTF_8).
            replaceAll(" *[\r\n]+", "");
    assertEquals("minidfs-ns.nn1 " + nn1Port + " nn1", fenceCommandOutput);
    tmpFile.delete();
    
    // Test failover with forceactive option
    assertEquals(0, runTool("-failover", "nn2", "nn1", "--forceactive"));

    // Fencing should not occur, since it was graceful
    assertFalse(tmpFile.exists());

          
    // Test failover with not fencer and forcefence option
    conf.unset(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY);
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "nn1", "nn2", "--forcefence"));
    assertFalse(tmpFile.exists());

    // Test failover with bad fencer and forcefence option
    conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY, "foobar!");
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "nn1", "nn2", "--forcefence"));
    assertFalse(tmpFile.exists());

    // Test failover with force fence listed before the other arguments
    conf.set(DFSConfigKeys.DFS_HA_FENCE_METHODS_KEY, 
             TestDFSHAAdmin.getFencerTrueCommand());
    tool.setConf(conf);
    assertEquals(0, runTool("-failover", "--forcefence", "nn1", "nn2"));
  }
     
  @Test
  public void testCheckHealth() throws Exception {
    assertEquals(0, runTool("-checkHealth", "nn1"));
    assertEquals(0, runTool("-checkHealth", "nn2"));
  }
  
  /**
   * Test case to check whether both the name node is active or not
   * @throws Exception
   */
  @Test
  public void testTransitionToActiveWhenOtherNamenodeisActive() 
      throws Exception {
    NameNode nn1 = cluster.getNameNode(0);
    NameNode nn2 = cluster.getNameNode(1);
    if(nn1.getState() != null && !nn1.getState().
        equals(HAServiceState.STANDBY.name()) ) {
      cluster.transitionToStandby(0);
    }
    if(nn2.getState() != null && !nn2.getState().
        equals(HAServiceState.STANDBY.name()) ) {
      cluster.transitionToStandby(1);
    }
    //Making sure both the namenode are in standby state
    assertTrue(nn1.isStandbyState());
    assertTrue(nn2.isStandbyState());
    // Triggering the transition for both namenode to Active
    runTool("-transitionToActive", "nn1");
    runTool("-transitionToActive", "nn2");

    assertFalse("Both namenodes cannot be active", nn1.isActiveState() 
        && nn2.isActiveState());
   
    /*  This test case doesn't allow nn2 to transition to Active even with
        forceActive switch since nn1 is already active  */
    if(nn1.getState() != null && !nn1.getState().
        equals(HAServiceState.STANDBY.name()) ) {
      cluster.transitionToStandby(0);
    }
    if(nn2.getState() != null && !nn2.getState().
        equals(HAServiceState.STANDBY.name()) ) {
      cluster.transitionToStandby(1);
    }
    //Making sure both the namenode are in standby state
    assertTrue(nn1.isStandbyState());
    assertTrue(nn2.isStandbyState());
    
    runTool("-transitionToActive", "nn1");
    runTool("-transitionToActive", "nn2","--forceactive");
    
    assertFalse("Both namenodes cannot be active even though with forceActive",
        nn1.isActiveState() && nn2.isActiveState());

    /*  In this test case, we have deliberately shut down nn1 and this will
        cause HAAAdmin#isOtherTargetNodeActive to throw an Exception 
        and transitionToActive for nn2 with  forceActive switch will succeed 
        even with Exception  */
    cluster.shutdownNameNode(0);
    if(nn2.getState() != null && !nn2.getState().
        equals(HAServiceState.STANDBY.name()) ) {
      cluster.transitionToStandby(1);
    }
    //Making sure both the namenode (nn2) is in standby state
    assertTrue(nn2.isStandbyState());
    assertFalse(cluster.isNameNodeUp(0));
    
    runTool("-transitionToActive", "nn2", "--forceactive");
    assertTrue("Namenode nn2 should be active", nn2.isActiveState());
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
