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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestNodeFencer {

  @Before
  public void clearMockState() {
    AlwaysSucceedFencer.fenceCalled = 0;
    AlwaysSucceedFencer.callArgs.clear();
    AlwaysFailFencer.fenceCalled = 0;
    AlwaysFailFencer.callArgs.clear();
  }

  @Test
  public void testSingleFencer() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer(
        AlwaysSucceedFencer.class.getName() + "(foo)");
    assertTrue(fencer.fence(new InetSocketAddress("host", 1234)));
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("host:1234", AlwaysSucceedFencer.fencedSvc);
    assertEquals("foo", AlwaysSucceedFencer.callArgs.get(0));
  }
  
  @Test
  public void testMultipleFencers() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer(
        AlwaysSucceedFencer.class.getName() + "(foo)\n" +
        AlwaysSucceedFencer.class.getName() + "(bar)\n");
    assertTrue(fencer.fence(new InetSocketAddress("host", 1234)));
    // Only one call, since the first fencer succeeds
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("foo", AlwaysSucceedFencer.callArgs.get(0));
  }
  
  @Test
  public void testWhitespaceAndCommentsInConfig()
      throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer(
        "\n" +
        " # the next one will always fail\n" +
        " " + AlwaysFailFencer.class.getName() + "(foo) # <- fails\n" +
        AlwaysSucceedFencer.class.getName() + "(bar) \n");
    assertTrue(fencer.fence(new InetSocketAddress("host", 1234)));
    // One call to each, since top fencer fails
    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertEquals("host:1234", AlwaysFailFencer.fencedSvc);
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("host:1234", AlwaysSucceedFencer.fencedSvc);
    assertEquals("foo", AlwaysFailFencer.callArgs.get(0));
    assertEquals("bar", AlwaysSucceedFencer.callArgs.get(0));
  }
 
  @Test
  public void testArglessFencer() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer(
        AlwaysSucceedFencer.class.getName());
    assertTrue(fencer.fence(new InetSocketAddress("host", 1234)));
    // One call to each, since top fencer fails
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("host:1234", AlwaysSucceedFencer.fencedSvc);
    assertEquals(null, AlwaysSucceedFencer.callArgs.get(0));
  }

  @Test
  public void testShortNameShell() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer("shell(true)");
    assertTrue(fencer.fence(new InetSocketAddress("host", 1234)));
  }

  @Test
  public void testShortNameSsh() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer("sshfence");
    assertFalse(fencer.fence(new InetSocketAddress("host", 1234)));
  }

  @Test
  public void testShortNameSshWithUser() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer("sshfence(user)");
    assertFalse(fencer.fence(new InetSocketAddress("host", 1234)));
  }

  @Test
  public void testShortNameSshWithPort() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer("sshfence(:123)");
    assertFalse(fencer.fence(new InetSocketAddress("host", 1234)));
  }

  @Test
  public void testShortNameSshWithUserPort() throws BadFencingConfigurationException {
    NodeFencer fencer = setupFencer("sshfence(user:123)");
    assertFalse(fencer.fence(new InetSocketAddress("host", 1234)));
  }

  public static NodeFencer setupFencer(String confStr)
      throws BadFencingConfigurationException {
    System.err.println("Testing configuration:\n" + confStr);
    Configuration conf = new Configuration();
    conf.set(NodeFencer.CONF_METHODS_KEY, confStr);
    return new NodeFencer(conf);
  }
  
  /**
   * Mock fencing method that always returns true
   */
  public static class AlwaysSucceedFencer extends Configured
      implements FenceMethod {
    static int fenceCalled = 0;
    static String fencedSvc;
    static List<String> callArgs = Lists.newArrayList();

    @Override
    public boolean tryFence(InetSocketAddress serviceAddr, String args) {
      fencedSvc = serviceAddr.getHostName() + ":" + serviceAddr.getPort();
      callArgs.add(args);
      fenceCalled++;
      return true;
    }

    @Override
    public void checkArgs(String args) {
    }
  }
  
  /**
   * Identical mock to above, except always returns false
   */
  public static class AlwaysFailFencer extends Configured
      implements FenceMethod {
    static int fenceCalled = 0;
    static String fencedSvc;
    static List<String> callArgs = Lists.newArrayList();

    @Override
    public boolean tryFence(InetSocketAddress serviceAddr, String args) {
      fencedSvc = serviceAddr.getHostName() + ":" + serviceAddr.getPort();
      callArgs.add(args);
      fenceCalled++;
      return false;
    }

    @Override
    public void checkArgs(String args) {
    }
  }
}
