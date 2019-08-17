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
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestShellBasedUnixGroupsMapping {
  private static final Logger TESTLOG =
      LoggerFactory.getLogger(TestShellBasedUnixGroupsMapping.class);

  private final GenericTestUtils.LogCapturer shellMappingLog =
      GenericTestUtils.LogCapturer.captureLogs(
          ShellBasedUnixGroupsMapping.LOG);

  private class TestGroupUserNotExist
      extends ShellBasedUnixGroupsMapping {
    /**
     * Create a ShellCommandExecutor object which returns exit code 1,
     * emulating the case that the user does not exist.
     *
     * @param userName not used
     * @return a mock ShellCommandExecutor object
     */
    @Override
    protected ShellCommandExecutor createGroupExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        doThrow(new ExitCodeException(1,
            "id: foobarusernotexist: No such user")).
            when(executor).execute();

        when(executor.getOutput()).thenReturn("");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }
  }

  @Test
  public void testGetGroupsNonexistentUser() throws Exception {
    TestGroupUserNotExist mapping = new TestGroupUserNotExist();

    List<String> groups = mapping.getGroups("foobarusernotexist");
    assertTrue(groups.isEmpty());
  }

  private class TestGroupNotResolvable
      extends ShellBasedUnixGroupsMapping {
    /**
     * Create a ShellCommandExecutor object which returns partially resolved
     * group names for a user.
     *
     * @param userName not used
     * @return a mock ShellCommandExecutor object
     */
    @Override
    protected ShellCommandExecutor createGroupExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        // There is both a group name 9999 and a group ID 9999.
        // This is treated as unresolvable group.
        doThrow(new ExitCodeException(1, "cannot find name for group ID 9999")).
            when(executor).execute();

        when(executor.getOutput()).thenReturn("9999\n9999 abc def");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }

    @Override
    protected ShellCommandExecutor createGroupIDExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      when(executor.getOutput()).thenReturn("9999\n9999 1 2");
      return executor;
    }
  }

  @Test
  public void testGetGroupsNotResolvable() throws Exception {
    TestGroupNotResolvable mapping = new TestGroupNotResolvable();

    List<String> groups = mapping.getGroups("user");
    assertTrue(groups.size() == 2);
    assertTrue(groups.contains("abc"));
    assertTrue(groups.contains("def"));
  }

  private class TestNumericGroupResolvable
      extends ShellBasedUnixGroupsMapping {
    /**
     * Create a ShellCommandExecutor object which returns numerical group
     * names of a user.
     *
     * @param userName not used
     * @return a mock ShellCommandExecutor object
     */
    @Override
    protected ShellCommandExecutor createGroupExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        // There is a numerical group 23, but no group name 23.
        // Thus 23 is treated as a resolvable group name.
        doNothing().when(executor).execute();
        when(executor.getOutput()).thenReturn("23\n23 groupname zzz");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }

    @Override
    protected ShellCommandExecutor createGroupIDExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        doNothing().when(executor).execute();
        when(executor.getOutput()).thenReturn("111\n111 112 113");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }
  }

  @Test
  public void testGetNumericGroupsResolvable() throws Exception {
    TestNumericGroupResolvable mapping = new TestNumericGroupResolvable();

    List<String> groups = mapping.getGroups("user");
    assertTrue(groups.size() == 3);
    assertTrue(groups.contains("23"));
    assertTrue(groups.contains("groupname"));
    assertTrue(groups.contains("zzz"));
  }

  public long getTimeoutInterval(String timeout) {
    Configuration conf = new Configuration();
    String userName = "foobarnonexistinguser";
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY,
        timeout);
    TestDelayedGroupCommand mapping = ReflectionUtils
        .newInstance(TestDelayedGroupCommand.class, conf);
    ShellCommandExecutor executor = mapping.createGroupExecutor(userName);
    return executor.getTimeoutInterval();
  }

  @Test
  public void testShellTimeOutConf() {

    // Test a 1 second max-runtime timeout
    assertEquals(
        "Expected the group names executor to carry the configured timeout",
        1000L, getTimeoutInterval("1s"));

    // Test a 1 minute max-runtime timeout
    assertEquals(
        "Expected the group names executor to carry the configured timeout",
        60000L, getTimeoutInterval("1m"));

    // Test a 1 millisecond max-runtime timeout
    assertEquals(
        "Expected the group names executor to carry the configured timeout",
        1L, getTimeoutInterval("1"));
  }

  private class TestGroupResolvable
      extends ShellBasedUnixGroupsMapping {
    /**
     * Create a ShellCommandExecutor object to return the group names of a user.
     *
     * @param userName not used
     * @return a mock ShellCommandExecutor object
     */
    @Override
    protected ShellCommandExecutor createGroupExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        doNothing().when(executor).execute();
        when(executor.getOutput()).thenReturn("abc\ndef abc hij");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }

    @Override
    protected ShellCommandExecutor createGroupIDExecutor(String userName) {
      ShellCommandExecutor executor = mock(ShellCommandExecutor.class);

      try {
        doNothing().when(executor).execute();
        when(executor.getOutput()).thenReturn("1\n1 2 3");
      } catch (IOException e) {
        TESTLOG.warn(e.getMessage());
      }
      return executor;
    }
  }

  @Test
  public void testGetGroupsResolvable() throws Exception {
    TestGroupResolvable mapping = new TestGroupResolvable();

    List<String> groups = mapping.getGroups("user");
    assertTrue(groups.size() == 3);
    assertTrue(groups.contains("abc"));
    assertTrue(groups.contains("def"));
    assertTrue(groups.contains("hij"));
  }

  private static class TestDelayedGroupCommand
      extends ShellBasedUnixGroupsMapping {

    private Long timeoutSecs = 1L;

    TestDelayedGroupCommand() {
      super();
    }

    @Override
    protected String[] getGroupsForUserCommand(String userName) {
      // Sleeps 2 seconds when executed and writes no output
      if (Shell.WINDOWS) {
        return new String[]{"timeout", timeoutSecs.toString()};
      }
      return new String[]{"sleep", timeoutSecs.toString()};
    }

    @Override
    protected String[] getGroupsIDForUserCommand(String userName) {
      return getGroupsForUserCommand(userName);
    }
  }

  @Test(timeout=4000)
  public void testFiniteGroupResolutionTime() throws Exception {
    Configuration conf = new Configuration();
    String userName = "foobarnonexistinguser";
    String commandTimeoutMessage =
        "ran longer than the configured timeout limit";
    long testTimeout = 500L;

    // Test a 1 second max-runtime timeout
    conf.setLong(
        CommonConfigurationKeys.
            HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY,
        testTimeout);

    TestDelayedGroupCommand mapping =
        ReflectionUtils.newInstance(TestDelayedGroupCommand.class, conf);

    ShellCommandExecutor executor = mapping.createGroupExecutor(userName);
    assertEquals(
        "Expected the group names executor to carry the configured timeout",
        testTimeout,
        executor.getTimeoutInterval());

    executor = mapping.createGroupIDExecutor(userName);
    assertEquals(
        "Expected the group ID executor to carry the configured timeout",
        testTimeout,
        executor.getTimeoutInterval());

    assertEquals(
        "Expected no groups to be returned given a shell command timeout",
        0,
        mapping.getGroups(userName).size());
    assertTrue(
        "Expected the logs to carry " +
            "a message about command timeout but was: " +
            shellMappingLog.getOutput(),
        shellMappingLog.getOutput().contains(commandTimeoutMessage));
    shellMappingLog.clearOutput();

    // Test also the parent Groups framework for expected behaviour
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        TestDelayedGroupCommand.class,
        GroupMappingServiceProvider.class);
    Groups groups = new Groups(conf);
    try {
      groups.getGroups(userName);
      fail(
          "The groups framework call should " +
              "have failed with a command timeout");
    } catch (IOException e) {
      assertTrue(
          "Expected the logs to carry " +
              "a message about command timeout but was: " +
              shellMappingLog.getOutput(),
          shellMappingLog.getOutput().contains(commandTimeoutMessage));
    }
    shellMappingLog.clearOutput();

    // Test the no-timeout (default) configuration
    conf = new Configuration();
    long defaultTimeout =
        CommonConfigurationKeys.
            HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT;

    mapping =
        ReflectionUtils.newInstance(TestDelayedGroupCommand.class, conf);

    executor = mapping.createGroupExecutor(userName);
    assertEquals(
        "Expected the group names executor to carry the default timeout",
        defaultTimeout,
        executor.getTimeoutInterval());

    executor = mapping.createGroupIDExecutor(userName);
    assertEquals(
        "Expected the group ID executor to carry the default timeout",
        defaultTimeout,
        executor.getTimeoutInterval());

    mapping.getGroups(userName);
    assertFalse(
        "Didn't expect a timeout of command in execution but logs carry it: " +
            shellMappingLog.getOutput(),
        shellMappingLog.getOutput().contains(commandTimeoutMessage));
  }
}


