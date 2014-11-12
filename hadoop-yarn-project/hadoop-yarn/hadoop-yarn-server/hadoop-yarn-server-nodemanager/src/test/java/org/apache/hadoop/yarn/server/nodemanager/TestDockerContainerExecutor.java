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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is intended to test the DockerContainerExecutor code, but it requires docker
 * to be installed.
 * <br><ol>
 * <li>Install docker, and Compile the code with docker-service-url set to the host and port
 * where docker service is running.
 * <br><pre><code>
 * > mvn clean install -Ddocker-service-url=tcp://0.0.0.0:4243
 *                          -DskipTests
 * </code></pre>
 */
public class TestDockerContainerExecutor {
  private static final Log LOG = LogFactory
      .getLog(TestDockerContainerExecutor.class);
  private static File workSpace = null;
  private DockerContainerExecutor exec = null;
  private LocalDirsHandlerService dirsHandler;
  private Path workDir;
  private FileContext lfs;
  private String yarnImage;

  private int id = 0;
  private String appSubmitter;
  private String dockerUrl;
  private String testImage = "centos";
  private String dockerExec;
  private String containerIdStr;


  private ContainerId getNextContainerId() {
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    String id = "CONTAINER_" + System.currentTimeMillis();
    when(cId.toString()).thenReturn(id);
    return cId;
  }

  @Before
  public void setup() {
    try {
      lfs = FileContext.getLocalFSFileContext();
      workDir = new Path("/tmp/temp-" + System.currentTimeMillis());
      workSpace = new File(workDir.toUri().getPath());
      lfs.mkdir(workDir, FsPermission.getDirDefault(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Configuration conf = new Configuration();
    yarnImage = "yarnImage";
    long time = System.currentTimeMillis();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "/tmp/nm-local-dir" + time);
    conf.set(YarnConfiguration.NM_LOG_DIRS, "/tmp/userlogs" + time);

    dockerUrl = System.getProperty("docker-service-url");
    LOG.info("dockerUrl: " + dockerUrl);
    if (Strings.isNullOrEmpty(dockerUrl)) {
      return;
    }
    dockerUrl = " -H " + dockerUrl;
    dockerExec = "docker " + dockerUrl;
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, yarnImage);
    conf.set(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME, dockerExec);
    exec = new DockerContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    exec.setConf(conf);
    appSubmitter = System.getProperty("application.submitter");
    if (appSubmitter == null || appSubmitter.isEmpty()) {
      appSubmitter = "nobody";
    }
    shellExec(dockerExec + " pull " + testImage);

  }

  private Shell.ShellCommandExecutor shellExec(String command) {
    try {

      Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(
          command.split("\\s+"),
          new File(workDir.toUri().getPath()),
          System.getenv());
      shExec.execute();
      return shExec;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean shouldRun() {
    return exec != null;
  }

  private int runAndBlock(ContainerId cId, Map<String, String> launchCtxEnv, String... cmd) throws IOException {
    String appId = "APP_" + System.currentTimeMillis();
    Container container = mock(Container.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(context.getEnvironment()).thenReturn(launchCtxEnv);

    String script = writeScriptFile(launchCtxEnv, cmd);

    Path scriptPath = new Path(script);
    Path tokensPath = new Path("/dev/null");
    Path workDir = new Path(workSpace.getAbsolutePath());
    Path pidFile = new Path(workDir, "pid.txt");

    exec.activateContainer(cId, pidFile);
    return exec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
  }

  private String writeScriptFile(Map<String, String> launchCtxEnv, String... cmd) throws IOException {
    File f = File.createTempFile("TestDockerContainerExecutor", ".sh");
    f.deleteOnExit();
    PrintWriter p = new PrintWriter(new FileOutputStream(f));
    for(Map.Entry<String, String> entry: launchCtxEnv.entrySet()) {
      p.println("export " + entry.getKey() + "=\"" + entry.getValue() + "\"");
    }
    for (String part : cmd) {
      p.print(part.replace("\\", "\\\\").replace("'", "\\'"));
      p.print(" ");
    }
    p.println();
    p.close();
    return f.getAbsolutePath();
  }

  @After
  public void tearDown() {
    try {
      lfs.delete(workDir, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLaunchContainer() throws IOException {
    if (!shouldRun()) {
      LOG.warn("Docker not installed, aborting test.");
      return;
    }

    Map<String, String> env = new HashMap<String, String>();
    env.put(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME, testImage);
    String touchFileName = "touch-file-" + System.currentTimeMillis();
    File touchFile = new File(dirsHandler.getLocalDirs().get(0), touchFileName);
    ContainerId cId = getNextContainerId();
    int ret = runAndBlock(
        cId, env, "touch", touchFile.getAbsolutePath(), "&&", "cp", touchFile.getAbsolutePath(), "/");

    assertEquals(0, ret);
  }
}
