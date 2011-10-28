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
}
  
  @Test
  public void testHelp() throws Exception {
    assertEquals(-1, runTool("-help"));
    assertEquals(1, runTool("-help", "transitionToActive"));
    assertOutputContains("Transitions the daemon into Active");
  }
  
  @Test
  public void testTransitionToActive() throws Exception {
    assertEquals(0, runTool("-transitionToActive", "xxx"));
    Mockito.verify(mockProtocol).transitionToActive();
  }

  @Test
  public void testTransitionToStandby() throws Exception {
    assertEquals(0, runTool("-transitionToStandby", "xxx"));
    Mockito.verify(mockProtocol).transitionToStandby();
  }
  
  @Test
  public void testCheckHealth() throws Exception {
    assertEquals(0, runTool("-checkHealth", "xxx"));
    Mockito.verify(mockProtocol).monitorHealth();
    
    Mockito.doThrow(new HealthCheckFailedException("fake health check failure"))
      .when(mockProtocol).monitorHealth();
    assertEquals(1, runTool("-checkHealth", "xxx"));
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
