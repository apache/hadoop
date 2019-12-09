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
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHAAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(TestHAAdmin.class);
  
  private HAAdmin tool;
  private ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
  private ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
  private String errOutput;
  private String output;

  @Before
  public void setup() throws IOException {
    tool = new HAAdmin() {
      @Override
      protected HAServiceTarget resolveTarget(String target) {
        return new DummyHAService(HAServiceState.STANDBY,
            new InetSocketAddress("dummy", 12345));
      }
    };
    tool.setConf(new Configuration());
    tool.errOut = new PrintStream(errOutBytes);
    tool.out = new PrintStream(outBytes);
  }
  
  private void assertOutputContains(String string) {
    if (!errOutput.contains(string) && !output.contains(string)) {
      fail("Expected output to contain '" + string + 
          "' but err_output was:\n" + errOutput + 
          "\n and output was: \n" + output);
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
    assertEquals(0, runTool("-help"));
    assertEquals(0, runTool("-help", "transitionToActive"));
    assertOutputContains("Transitions the service into Active");
  }

  private Object runTool(String ... args) throws Exception {
    errOutBytes.reset();
    outBytes.reset();
    LOG.info("Running: HAAdmin " + Joiner.on(" ").join(args));
    int ret = tool.run(args);
    errOutput = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
    output = new String(outBytes.toByteArray(), Charsets.UTF_8);
    LOG.info("Err_output:\n" + errOutput + "\nOutput:\n" + output);
    return ret;
  }
}
