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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.net.URI;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test for the abstract {@link LogAggregationFileController} class,
 * checking its core functionality.
 */
public class TestLogAggregationFileController {

  @Test
  public void testRemoteDirCreationDefault() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    doReturn(new URI("")).when(fs).getUri();
    doThrow(FileNotFoundException.class).when(fs)
            .getFileStatus(any(Path.class));

    Configuration conf = new Configuration();
    LogAggregationFileController controller = mock(
            LogAggregationFileController.class, Mockito.CALLS_REAL_METHODS);
    doReturn(fs).when(controller).getFileSystem(any(Configuration.class));

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "yarn_user", new String[] {"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);

    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();

    verify(fs).setOwner(any(), eq("yarn_user"), eq("yarn_group"));
  }

  @Test
  public void testRemoteDirCreationWithCustomGroup() throws Exception {
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
        "yarn_user", new String[] {"yarn_group", "other_group"});
    UserGroupInformation.setLoginUser(ugi);

    controller.initialize(conf, "TFile");
    controller.verifyAndCreateRemoteLogDir();

    verify(fs).setOwner(any(), eq("yarn_user"), eq(testGroupName));
  }
}
