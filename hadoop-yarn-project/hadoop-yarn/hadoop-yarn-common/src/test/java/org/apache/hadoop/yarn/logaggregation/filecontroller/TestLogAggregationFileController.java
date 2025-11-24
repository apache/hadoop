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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController.TLDIR_PERMISSIONS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for the abstract {@link LogAggregationFileController} class,
 * checking its core functionality.
 */
public class TestLogAggregationFileController {

  @Test
  void testRemoteDirCreationDefault() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    doReturn(new URI("")).when(fs).getUri();
    doThrow(FileNotFoundException.class).when(fs)
        .getFileStatus(any(Path.class));

    Configuration conf = new Configuration();
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    doReturn(fs).when(controller).getFileSystem(any(Configuration.class));

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "yarn_user", new String[]{"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);

    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();

    verify(fs).setOwner(any(), eq("yarn_user"), eq("yarn_group"));
  }

  @Test
  void testRemoteDirCreationWithCustomGroup() throws Exception {
    String testGroupName = "testGroup";

    FileSystem fs = mock(FileSystem.class);
    doReturn(new URI("")).when(fs).getUri();
    doThrow(FileNotFoundException.class).when(fs)
        .getFileStatus(any(Path.class));

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_GROUPNAME, testGroupName);
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    doReturn(fs).when(controller).getFileSystem(any(Configuration.class));

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "yarn_user", new String[]{"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);

    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();

    verify(fs).setOwner(any(), eq("yarn_user"), eq(testGroupName));
  }

  private static class PathContainsString implements ArgumentMatcher<Path> {
    private final String expected;

    PathContainsString(String expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Path path) {
      return path.getName().contains(expected);
    }

    @Override
    public String toString() {
      return "Path with name=" + expected;
    }
  }

  @Test
  void testRemoteDirCreationWithCustomUser() throws Exception {
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    FileSystem fs = mock(FileSystem.class);
    setupCustomUserMocks(controller, fs, "/tmp/logs");

    controller.initialize(new Configuration(), "TFile");
    controller.fsSupportsChmod = false;

    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs);
    assertTrue(controller.fsSupportsChmod);

    doThrow(UnsupportedOperationException.class).when(fs).setPermission(any(), any());
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    assertTrue(controller.fsSupportsChmod);

    controller.fsSupportsChmod = false;
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    assertTrue(controller.fsSupportsChmod);
  }

  @Test
  void testRemoteDirCreationWithCustomUserFsChmodNotSupported() throws Exception {
    LogAggregationFileController controller = mock(
        LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    FileSystem fs = mock(FileSystem.class);
    setupCustomUserMocks(controller, fs, "/tmp/logs2");
    doThrow(UnsupportedOperationException.class).when(fs).setPermission(any(), any());

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, "/tmp/logs2");
    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs);
    assertFalse(controller.fsSupportsChmod);

    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    assertFalse(controller.fsSupportsChmod);

    controller.fsSupportsChmod = true;
    controller.verifyAndCreateRemoteLogDir();
    assertPermissionFileWasUsedOneTime(fs); // still once -> cached
    assertFalse(controller.fsSupportsChmod);
  }

  private static void setupCustomUserMocks(LogAggregationFileController controller,
                                           FileSystem fs, String path)
      throws URISyntaxException, IOException {
    doReturn(new URI("")).when(fs).getUri();
    doReturn(new FileStatus(128, false, 0, 64, System.currentTimeMillis(),
        System.currentTimeMillis(), new FsPermission(TLDIR_PERMISSIONS),
        "not_yarn_user", "yarn_group", new Path(path))).when(fs)
        .getFileStatus(any(Path.class));
    doReturn(fs).when(controller).getFileSystem(any(Configuration.class));

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "yarn_user", new String[]{"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);
  }

  private static void assertPermissionFileWasUsedOneTime(FileSystem fs) throws IOException {
    verify(fs, times(1))
        .createNewFile(argThat(new PathContainsString(".permission_check")));
    verify(fs, times(1))
        .setPermission(argThat(new PathContainsString(".permission_check")),
        eq(new FsPermission(TLDIR_PERMISSIONS)));
    verify(fs, times(1))
        .delete(argThat(new PathContainsString(".permission_check")), eq(false));
  }
}
