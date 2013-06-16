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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.spy;

public class TestShellCommandFencer {
  private ShellCommandFencer fencer = createFencer();
  private static final HAServiceTarget TEST_TARGET =
      new DummyHAService(HAServiceState.ACTIVE,
          new InetSocketAddress("dummyhost", 1234));
  
  @BeforeClass
  public static void setupLogSpy() {
    ShellCommandFencer.LOG = spy(ShellCommandFencer.LOG);
  }
  
  @Before
  public void resetLogSpy() {
    Mockito.reset(ShellCommandFencer.LOG);
  }
  
  private static ShellCommandFencer createFencer() {
    Configuration conf = new Configuration();
    conf.set("in.fencing.tests", "yessir");
    ShellCommandFencer fencer = new ShellCommandFencer();
    fencer.setConf(conf);
    return fencer;
  }
  
  /**
   * Test that the exit code of the script determines
   * whether the fencer succeeded or failed
   */
  @Test
  public void testBasicSuccessFailure() {
    assertTrue(fencer.tryFence(TEST_TARGET, "echo"));
    assertFalse(fencer.tryFence(TEST_TARGET, "exit 1"));
    // bad path should also fail
    assertFalse(fencer.tryFence(TEST_TARGET, "xxxxxxxxxxxx"));
  }
  
  @Test
  public void testCheckNoArgs() {
    try {
      Configuration conf = new Configuration();
      new NodeFencer(conf, "shell");
      fail("Didn't throw when passing no args to shell");
    } catch (BadFencingConfigurationException confe) {
      assertTrue(
        "Unexpected exception:" + StringUtils.stringifyException(confe),
        confe.getMessage().contains("No argument passed"));    
    }
  }

  @Test
  public void testCheckParensNoArgs() {
    try {
      Configuration conf = new Configuration();
      new NodeFencer(conf, "shell()");
      fail("Didn't throw when passing no args to shell");
    } catch (BadFencingConfigurationException confe) {
      assertTrue(
        "Unexpected exception:" + StringUtils.stringifyException(confe),
        confe.getMessage().contains("Unable to parse line: 'shell()'"));
    }
  }

  /**
   * Test that lines on stdout get passed as INFO
   * level messages
   */
  @Test
  public void testStdoutLogging() {
    assertTrue(fencer.tryFence(TEST_TARGET, "echo hello"));
    Mockito.verify(ShellCommandFencer.LOG).info(
        Mockito.endsWith("echo hello: hello"));
  }
   
  /**
   * Test that lines on stderr get passed as
   * WARN level log messages
   */
  @Test
  public void testStderrLogging() {
    assertTrue(fencer.tryFence(TEST_TARGET, "echo hello>&2"));
    Mockito.verify(ShellCommandFencer.LOG).warn(
        Mockito.endsWith("echo hello>&2: hello"));
  }

  /**
   * Verify that the Configuration gets passed as
   * environment variables to the fencer.
   */
  @Test
  public void testConfAsEnvironment() {
    if (!Shell.WINDOWS) {
      fencer.tryFence(TEST_TARGET, "echo $in_fencing_tests");
      Mockito.verify(ShellCommandFencer.LOG).info(
          Mockito.endsWith("echo $in...ing_tests: yessir"));
    } else {
      fencer.tryFence(TEST_TARGET, "echo %in_fencing_tests%");
      Mockito.verify(ShellCommandFencer.LOG).info(
          Mockito.endsWith("echo %in...ng_tests%: yessir"));
    }
  }
  
  /**
   * Verify that information about the fencing target gets passed as
   * environment variables to the fencer.
   */
  @Test
  public void testTargetAsEnvironment() {
    if (!Shell.WINDOWS) {
      fencer.tryFence(TEST_TARGET, "echo $target_host $target_port");
      Mockito.verify(ShellCommandFencer.LOG).info(
          Mockito.endsWith("echo $ta...rget_port: dummyhost 1234"));
    } else {
      fencer.tryFence(TEST_TARGET, "echo %target_host% %target_port%");
      Mockito.verify(ShellCommandFencer.LOG).info(
          Mockito.endsWith("echo %ta...get_port%: dummyhost 1234"));
    }
  }


  /**
   * Test that we properly close off our input to the subprocess
   * such that it knows there's no tty connected. This is important
   * so that, if we use 'ssh', it won't try to prompt for a password
   * and block forever, for example.
   */
  @Test(timeout=10000)
  public void testSubprocessInputIsClosed() {
    assertFalse(fencer.tryFence(TEST_TARGET, "read"));
  }
  
  @Test
  public void testCommandAbbreviation() {
    assertEquals("a...f", ShellCommandFencer.abbreviate("abcdef", 5));
    assertEquals("abcdef", ShellCommandFencer.abbreviate("abcdef", 6));
    assertEquals("abcdef", ShellCommandFencer.abbreviate("abcdef", 7));

    assertEquals("a...g", ShellCommandFencer.abbreviate("abcdefg", 5));
    assertEquals("a...h", ShellCommandFencer.abbreviate("abcdefgh", 5));
    assertEquals("a...gh", ShellCommandFencer.abbreviate("abcdefgh", 6));
    assertEquals("ab...gh", ShellCommandFencer.abbreviate("abcdefgh", 7));
  }
}
