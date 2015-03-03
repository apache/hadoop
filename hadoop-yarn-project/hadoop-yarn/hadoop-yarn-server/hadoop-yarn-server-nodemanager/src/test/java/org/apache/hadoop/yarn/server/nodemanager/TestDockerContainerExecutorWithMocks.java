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

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

/**
 * Mock tests for docker container executor
 */
public class TestDockerContainerExecutorWithMocks {

  private static final Log LOG = LogFactory
      .getLog(TestDockerContainerExecutorWithMocks.class);
  public static final String DOCKER_LAUNCH_COMMAND = "/bin/true";
  private DockerContainerExecutor dockerContainerExecutor = null;
  private LocalDirsHandlerService dirsHandler;
  private Path workDir;
  private FileContext lfs;
  private String yarnImage;

  @Before
  public void setup() {
    assumeTrue(Shell.LINUX);
    File f = new File("./src/test/resources/mock-container-executor");
    if(!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    yarnImage = "yarnImage";
    long time = System.currentTimeMillis();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "/tmp/nm-local-dir" + time);
    conf.set(YarnConfiguration.NM_LOG_DIRS, "/tmp/userlogs" + time);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, yarnImage);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME , DOCKER_LAUNCH_COMMAND);
    dockerContainerExecutor = new DockerContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    dockerContainerExecutor.setConf(conf);
    lfs = null;
    try {
      lfs = FileContext.getLocalFSFileContext();
      workDir = new Path("/tmp/temp-"+ System.currentTimeMillis());
      lfs.mkdir(workDir, FsPermission.getDirDefault(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @After
  public void tearDown() {
    try {
      if (lfs != null) {
        lfs.delete(workDir, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testContainerInitSecure() throws IOException {
    dockerContainerExecutor.getConf().set(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    dockerContainerExecutor.init();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerLaunchNullImage() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    dockerContainerExecutor.getConf()
        .set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerLaunchInvalidImage() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "testrepo.com/test-image rm -rf $HADOOP_PREFIX/*";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    dockerContainerExecutor.getConf()
      .set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid.txt");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    dockerContainerExecutor.launchContainer(container, scriptPath, tokensPath,
      appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
      dirsHandler.getLogDirs());
  }

  @Test
  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    String testImage = "\"sequenceiq/hadoop-docker:2.4.1\"";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid");

    dockerContainerExecutor.activateContainer(cId, pidFile);
    int ret = dockerContainerExecutor.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    assertEquals(0, ret);
    //get the script
    Path sessionScriptPath = new Path(workDir,
        Shell.appendScriptExtension(
            DockerContainerExecutor.DOCKER_CONTAINER_EXECUTOR_SESSION_SCRIPT));
    LineNumberReader lnr = new LineNumberReader(new FileReader(sessionScriptPath.toString()));
    boolean cmdFound = false;
    List<String> localDirs = dirsToMount(dirsHandler.getLocalDirs());
    List<String> logDirs = dirsToMount(dirsHandler.getLogDirs());
    List<String> workDirMount = dirsToMount(Collections.singletonList(workDir.toUri().getPath()));
    List<String> expectedCommands =  new ArrayList<String>(
        Arrays.asList(DOCKER_LAUNCH_COMMAND, "run", "--rm", "--net=host",  "--name", containerId));
    expectedCommands.addAll(localDirs);
    expectedCommands.addAll(logDirs);
    expectedCommands.addAll(workDirMount);
    String shellScript =  workDir + "/launch_container.sh";

    expectedCommands.addAll(Arrays.asList(testImage.replaceAll("['\"]", ""), "bash","\"" + shellScript + "\""));

    String expectedPidString = "echo `/bin/true inspect --format {{.State.Pid}} " + containerId+"` > "+ pidFile.toString() + ".tmp";
    boolean pidSetterFound = false;
    while(lnr.ready()){
      String line = lnr.readLine();
      LOG.debug("line: " + line);
      if (line.startsWith(DOCKER_LAUNCH_COMMAND)){
        List<String> command = new ArrayList<String>();
        for( String s :line.split("\\s+")){
          command.add(s.trim());
        }

        assertEquals(expectedCommands, command);
        cmdFound = true;
      } else if (line.startsWith("echo")) {
        assertEquals(expectedPidString, line);
        pidSetterFound = true;
      }

    }
    assertTrue(cmdFound);
    assertTrue(pidSetterFound);
  }

  private List<String> dirsToMount(List<String> dirs) {
    List<String> localDirs = new ArrayList<String>();
    for(String dir: dirs){
      localDirs.add("-v");
      localDirs.add(dir + ":" + dir);
    }
    return localDirs;
  }
}
