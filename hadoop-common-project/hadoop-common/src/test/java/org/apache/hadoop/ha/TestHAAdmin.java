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
package org.apache.hadoop.ha;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

public class TestHAAdmin {
  private static final Log LOG = LogFactory.getLog(TestHAAdmin.class);
  
  private HAAdmin tool;
  private ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
  private String errOutput;
  private HAServiceProtocol mockProtocol;
  
  @Before
  public void setup() {
    mockProtocol = Mockito.mock(HAServiceProtocol.class);
    tool = new HAAdmin() {
      @Override
      protected HAServiceProtocol getProtocol(String target) throws IOException {
        return mockProtocol;
      }
    };
    tool.setConf(new Configuration());
    tool.errOut = new PrintStream(errOutBytes);
  }
  
  private void assertOutputContains(String string) {
    if (!errOutput.contains(string)) {
      fail("Expected output to contain '" + string + "' but was:\n" +
          errOutput);
    }
  }
  
  @Test
  public void testAdminUsage() throws Exception {
    assertEquals(-1, runTool());
    assertOutputContains("Usage:");
    assertOutputContains("-transitionToActive");
    
    assertEquals(-1, runTool("badCommand"));
    assertOutputContains("Bad command 'badCommand'");
    
    assertEquals(-1, runTool("-badCommand"));
    assertOutputContains("badCommand: Unknown");    

    // valid command but not enough arguments
    assertEquals(-1, runTool("-transitionToActive"));
    assertOutputContains("transitionToActive: incorrect number of arguments");
    assertEquals(-1, runTool("-transitionToActive", "x", "y"));
    assertOutputContains("transitionToActive: incorrect number of arguments");
    assertEquals(-1, runTool("-failover"));
    assertOutputContains("failover: incorrect arguments");
    assertOutputContains("failover: incorrect arguments");    
    assertEquals(-1, runTool("-failover", "foo:1234"));
    assertOutputContains("failover: incorrect arguments");
  }
  
  @Test
  public void testHelp() throws Exception {
    assertEquals(-1, runTool("-help"));
    assertEquals(0, runTool("-help", "transitionToActive"));
    assertOutputContains("Transitions the daemon into Active");
  }
  
  @Test
  public void testTransitionToActive() throws Exception {
    assertEquals(0, runTool("-transitionToActive", "foo:1234"));
    Mockito.verify(mockProtocol).transitionToActive();
  }

  @Test
  public void testTransitionToStandby() throws Exception {
    assertEquals(0, runTool("-transitionToStandby", "foo:1234"));
    Mockito.verify(mockProtocol).transitionToStandby();
  }

  @Test
  public void testFailoverWithNoFencerConfigured() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    assertEquals(-1, runTool("-failover", "foo:1234", "bar:5678"));
  }

  @Test
  public void testFailoverWithFencerConfigured() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(0, runTool("-failover", "foo:1234", "bar:5678"));
  }

  @Test
  public void testFailoverWithFencerConfiguredAndForce() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(0, runTool("-failover", "foo:1234", "bar:5678", "--forcefence"));
  }

  @Test
  public void testFailoverWithInvalidFenceArg() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "foo:1234", "bar:5678", "notforcefence"));
  }

  @Test
  public void testFailoverWithFenceButNoFencer() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    assertEquals(-1, runTool("-failover", "foo:1234", "bar:5678", "--forcefence"));
  }

  @Test
  public void testFailoverWithFenceAndBadFencer() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, "foobar!");
    tool.setConf(conf);
    assertEquals(-1, runTool("-failover", "foo:1234", "bar:5678", "--forcefence"));
  }

  @Test
  public void testForceFenceOptionListedBeforeArgs() throws Exception {
    Mockito.doReturn(HAServiceState.STANDBY).when(mockProtocol).getServiceState();
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, "shell(true)");
    tool.setConf(conf);
    assertEquals(0, runTool("-failover", "--forcefence", "foo:1234", "bar:5678"));
  }

  @Test
  public void testGetServiceState() throws Exception {
    assertEquals(0, runTool("-getServiceState", "foo:1234"));
    Mockito.verify(mockProtocol).getServiceState();
  }

  @Test
  public void testCheckHealth() throws Exception {
    assertEquals(0, runTool("-checkHealth", "foo:1234"));
    Mockito.verify(mockProtocol).monitorHealth();
    
    Mockito.doThrow(new HealthCheckFailedException("fake health check failure"))
      .when(mockProtocol).monitorHealth();
    assertEquals(-1, runTool("-checkHealth", "foo:1234"));
    assertOutputContains("Health check failed: fake health check failure");
  }

  private Object runTool(String ... args) throws Exception {
    errOutBytes.reset();
    LOG.info("Running: HAAdmin " + Joiner.on(" ").join(args));
    int ret = tool.run(args);
    errOutput = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
    LOG.info("Output:\n" + errOutput);
    return ret;
  }
  
}
