/*
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** Unit tests for DockerClient. */
public class TestDockerClient {
  private static final File TEST_ROOT_DIR = GenericTestUtils.getTestDir(
      TestDockerClient.class.getName());

  @Before
  public void setup() {
    TEST_ROOT_DIR.mkdirs();
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(TEST_ROOT_DIR);
  }

  @Test
  public void testWriteCommandToTempFile() throws Exception {
    String absRoot = TEST_ROOT_DIR.getAbsolutePath();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    DockerCommand dockerCmd = new DockerInspectCommand(cid.toString());
    Configuration conf = new Configuration();
    conf.set("hadoop.tmp.dir", absRoot);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absRoot);
    conf.set(YarnConfiguration.NM_LOG_DIRS, absRoot);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    Context mockContext = mock(Context.class);
    doReturn(conf).when(mockContext).getConf();
    doReturn(dirsHandler).when(mockContext).getLocalDirsHandler();

    DockerClient dockerClient = new DockerClient();
    dirsHandler.init(conf);
    dirsHandler.start();
    String tmpPath = dockerClient.writeCommandToTempFile(dockerCmd, cid,
        mockContext);
    dirsHandler.stop();
    File tmpFile = new File(tmpPath);
    assertTrue(tmpFile + " was not created", tmpFile.exists());
  }

  @Test
  public void testCommandValidation() throws Exception {
    String absRoot = TEST_ROOT_DIR.getAbsolutePath();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Configuration conf = new Configuration();
    conf.set("hadoop.tmp.dir", absRoot);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absRoot);
    conf.set(YarnConfiguration.NM_LOG_DIRS, absRoot);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    Context mockContext = mock(Context.class);
    doReturn(conf).when(mockContext).getConf();
    doReturn(dirsHandler).when(mockContext).getLocalDirsHandler();

    DockerClient dockerClient = new DockerClient();
    dirsHandler.init(conf);
    dirsHandler.start();
    try {
      DockerRunCommand dockerCmd = new DockerRunCommand(cid.toString(), "user",
          "image");
      dockerCmd.addCommandArguments("prop=bad", "val");
      dockerClient.writeCommandToTempFile(dockerCmd, cid,
          mockContext);
      fail("Expected exception writing command file");
    } catch (ContainerExecutionException e) {
      assertTrue("Expected key validation error", e.getMessage()
          .contains("'=' found in entry for docker command file"));
    }

    try {
      DockerRunCommand dockerCmd = new DockerRunCommand(cid.toString(), "user",
          "image");
      dockerCmd.setOverrideCommandWithArgs(Arrays.asList("sleep", "1000\n"));
      dockerClient.writeCommandToTempFile(dockerCmd, cid, mockContext);
      fail("Expected exception writing command file");
    } catch (ContainerExecutionException e) {
      assertTrue("Expected value validation error", e.getMessage()
          .contains("'\\n' found in entry for docker command file"));
    }
    dirsHandler.stop();
  }
}
