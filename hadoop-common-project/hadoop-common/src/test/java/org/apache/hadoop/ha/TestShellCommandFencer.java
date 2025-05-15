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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.endsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.reset;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

public class TestShellCommandFencer {
  private ShellCommandFencer fencer = createFencer();
  private static final HAServiceTarget TEST_TARGET =
      new DummyHAService(HAServiceState.ACTIVE,
          new InetSocketAddress("dummyhost", 1234));
  private static final Logger LOG = ShellCommandFencer.LOG;

  @BeforeAll
  public static void setupLogMock() {
    ShellCommandFencer.LOG = mock(Logger.class, new LogAnswer());
  }

  @AfterAll
  public static void tearDownLogMock() throws Exception {
    ShellCommandFencer.LOG = LOG;
  }

  @BeforeEach
  public void resetLogSpy() {
    reset(ShellCommandFencer.LOG);
  }
  
  private static ShellCommandFencer createFencer() {
    Configuration conf = new Configuration();
    conf.set("in.fencing-tests", "yessir");
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
      assertTrue(confe.getMessage().contains("No argument passed"),
          "Unexpected exception:" + StringUtils.stringifyException(confe));
    }
  }

  @Test
  public void testCheckParensNoArgs() {
    try {
      Configuration conf = new Configuration();
      new NodeFencer(conf, "shell()");
      fail("Didn't throw when passing no args to shell");
    } catch (BadFencingConfigurationException confe) {
      assertTrue(confe.getMessage().contains("Unable to parse line: 'shell()'"),
          "Unexpected exception:" + StringUtils.stringifyException(confe));
    }
  }

  /**
   * Test that lines on stdout get passed as INFO
   * level messages
   */
  @Test
  public void testStdoutLogging() {
    assertTrue(fencer.tryFence(TEST_TARGET, "echo hello"));
    verify(ShellCommandFencer.LOG).info(
        endsWith("echo hello: hello"));
  }
   
  /**
   * Test that lines on stderr get passed as
   * WARN level log messages
   */
  @Test
  public void testStderrLogging() {
    assertTrue(fencer.tryFence(TEST_TARGET, "echo hello>&2"));
    verify(ShellCommandFencer.LOG).warn(
        endsWith("echo hello>&2: hello"));
  }

  /**
   * Verify that the Configuration gets passed as
   * environment variables to the fencer.
   */
  @Test
  public void testConfAsEnvironment() {
    if (!Shell.WINDOWS) {
      fencer.tryFence(TEST_TARGET, "echo $in_fencing_tests");
      verify(ShellCommandFencer.LOG).info(
          endsWith("echo $in...ing_tests: yessir"));
    } else {
      fencer.tryFence(TEST_TARGET, "echo %in_fencing_tests%");
      verify(ShellCommandFencer.LOG).info(
          endsWith("echo %in...ng_tests%: yessir"));
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
      verify(ShellCommandFencer.LOG).info(
          endsWith("echo $ta...rget_port: dummyhost 1234"));
    } else {
      fencer.tryFence(TEST_TARGET, "echo %target_host% %target_port%");
      verify(ShellCommandFencer.LOG).info(
          endsWith("echo %ta...get_port%: dummyhost 1234"));
    }
  }

  /**
   * Test if fencing target has peer set, the failover can trigger different
   * commands on source and destination respectively.
   */
  @Test
  public void testEnvironmentWithPeer() {
    HAServiceTarget target = new DummyHAService(HAServiceState.ACTIVE,
        new InetSocketAddress("dummytarget", 1111));
    HAServiceTarget source = new DummyHAService(HAServiceState.STANDBY,
        new InetSocketAddress("dummysource", 2222));
    target.setTransitionTargetHAStatus(HAServiceState.ACTIVE);
    source.setTransitionTargetHAStatus(HAServiceState.STANDBY);
    String cmd = "echo $target_host $target_port,"
        + "echo $source_host $source_port";
    if (!Shell.WINDOWS) {
      fencer.tryFence(target, cmd);
      verify(ShellCommandFencer.LOG).info(
          contains("echo $ta...rget_port: dummytarget 1111"));
      fencer.tryFence(source, cmd);
      verify(ShellCommandFencer.LOG).info(
          contains("echo $so...urce_port: dummysource 2222"));
    } else {
      fencer.tryFence(target, cmd);
      verify(ShellCommandFencer.LOG).info(
          contains("echo %ta...get_port%: dummytarget 1111"));
      fencer.tryFence(source, cmd);
      verify(ShellCommandFencer.LOG).info(
          contains("echo %so...urce_port%: dummysource 2222"));
    }
  }


  /**
   * Test that we properly close off our input to the subprocess
   * such that it knows there's no tty connected. This is important
   * so that, if we use 'ssh', it won't try to prompt for a password
   * and block forever, for example.
   */
  @Test
  @Timeout(value = 10)
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

  /**
   * An answer simply delegate some basic log methods to real LOG.
   */
  private static class LogAnswer implements Answer {

    private static final List<String> DELEGATE_METHODS = Arrays.asList(
        "error", "warn", "info", "debug", "trace");

    @Override
    public Object answer(InvocationOnMock invocation) {

      String methodName = invocation.getMethod().getName();

      if (!DELEGATE_METHODS.contains(methodName)) {
        return null;
      }

      try {
        String msg = invocation.getArguments()[0].toString();
        Method delegateMethod = LOG.getClass().getMethod(methodName,
            msg.getClass());
        delegateMethod.invoke(LOG, msg);
      } catch (Throwable e) {
        throw new IllegalStateException(
            "Unsupported delegate method: " + methodName);
      }

      return null;
    }
  }

}
