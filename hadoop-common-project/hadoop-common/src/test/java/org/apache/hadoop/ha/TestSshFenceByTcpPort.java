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

import java.net.InetSocketAddress;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.SshFenceByTcpPort.Args;
import org.apache.log4j.Level;
import org.junit.Assume;
import org.junit.Test;

public class TestSshFenceByTcpPort {

  static {
    ((Log4JLogger)SshFenceByTcpPort.LOG).getLogger().setLevel(Level.ALL);
  }

  private static String TEST_FENCING_HOST = System.getProperty(
      "test.TestSshFenceByTcpPort.host", "localhost");
  private static final String TEST_FENCING_PORT = System.getProperty(
      "test.TestSshFenceByTcpPort.port", "9820");
  private static final String TEST_KEYFILE = System.getProperty(
      "test.TestSshFenceByTcpPort.key");

  private static final InetSocketAddress TEST_ADDR =
    new InetSocketAddress(TEST_FENCING_HOST,
      Integer.parseInt(TEST_FENCING_PORT));
  private static final HAServiceTarget TEST_TARGET =
    new DummyHAService(HAServiceState.ACTIVE, TEST_ADDR);

  /**
   *  Connect to Google's DNS server - not running ssh!
   */
  private static final HAServiceTarget UNFENCEABLE_TARGET =
    new DummyHAService(HAServiceState.ACTIVE,
        new InetSocketAddress("8.8.8.8", 1234));

  @Test(timeout=20000)
  public void testFence() throws BadFencingConfigurationException {
    Assume.assumeTrue(isConfigured());
    Configuration conf = new Configuration();
    conf.set(SshFenceByTcpPort.CONF_IDENTITIES_KEY, TEST_KEYFILE);
    SshFenceByTcpPort fence = new SshFenceByTcpPort();
    fence.setConf(conf);
    assertTrue(fence.tryFence(
        TEST_TARGET,
        null));
  }

  /**
   * Test connecting to a host which definitely won't respond.
   * Make sure that it times out and returns false, but doesn't throw
   * any exception
   */
  @Test(timeout=20000)
  public void testConnectTimeout() throws BadFencingConfigurationException {
    Configuration conf = new Configuration();
    conf.setInt(SshFenceByTcpPort.CONF_CONNECT_TIMEOUT_KEY, 3000);
    SshFenceByTcpPort fence = new SshFenceByTcpPort();
    fence.setConf(conf);
    assertFalse(fence.tryFence(UNFENCEABLE_TARGET, ""));
  }
  
  @Test
  public void testArgsParsing() throws BadFencingConfigurationException {
    Args args = new SshFenceByTcpPort.Args(null);
    assertEquals(System.getProperty("user.name"), args.user);
    assertEquals(22, args.sshPort);
    
    args = new SshFenceByTcpPort.Args("");
    assertEquals(System.getProperty("user.name"), args.user);
    assertEquals(22, args.sshPort);

    args = new SshFenceByTcpPort.Args("12345");
    assertEquals("12345", args.user);
    assertEquals(22, args.sshPort);

    args = new SshFenceByTcpPort.Args(":12345");
    assertEquals(System.getProperty("user.name"), args.user);
    assertEquals(12345, args.sshPort);

    args = new SshFenceByTcpPort.Args("foo:2222");
    assertEquals("foo", args.user);
    assertEquals(2222, args.sshPort);
  }
  
  @Test
  public void testBadArgsParsing() throws BadFencingConfigurationException {
    assertBadArgs(":");          // No port specified
    assertBadArgs("bar.com:");   // "
    assertBadArgs(":xx");        // Port does not parse
    assertBadArgs("bar.com:xx"); // "
  }
  
  private void assertBadArgs(String argStr) {
    try {
      new Args(argStr);
      fail("Did not fail on bad args: " + argStr);
    } catch (BadFencingConfigurationException e) {
      // Expected
    }
  }

  private boolean isConfigured() {
    return (TEST_FENCING_HOST != null && !TEST_FENCING_HOST.isEmpty()) &&
           (TEST_FENCING_PORT != null && !TEST_FENCING_PORT.isEmpty()) &&
           (TEST_KEYFILE != null && !TEST_KEYFILE.isEmpty());
  }
}
