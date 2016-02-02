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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestShellBasedUnixGroupsMapping {
  private static final Log LOG =
      LogFactory.getLog(TestShellBasedUnixGroupsMapping.class);

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
        LOG.warn(e.getMessage());
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
        LOG.warn(e.getMessage());
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
        LOG.warn(e.getMessage());
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
        LOG.warn(e.getMessage());
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
        LOG.warn(e.getMessage());
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
        LOG.warn(e.getMessage());
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
}


