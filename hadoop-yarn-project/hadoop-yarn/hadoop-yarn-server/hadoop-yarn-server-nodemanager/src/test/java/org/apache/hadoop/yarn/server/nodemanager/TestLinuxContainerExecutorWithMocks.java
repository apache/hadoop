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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestLinuxContainerExecutorWithMocks {

  private static final Log LOG = LogFactory
      .getLog(TestLinuxContainerExecutorWithMocks.class);

  private LinuxContainerExecutor mockExec = null;
  private final File mockParamFile = new File("./params.txt");
  private LocalDirsHandlerService dirsHandler;
  

  private void deleteMockParamFile() {
    if(mockParamFile.exists()) {
      mockParamFile.delete();
    }
  }
  
  private List<String> readMockParams() throws IOException {
    LinkedList<String> ret = new LinkedList<String>();
    LineNumberReader reader = new LineNumberReader(new FileReader(
        mockParamFile));
    String line;
    while((line = reader.readLine()) != null) {
      ret.add(line);
    }
    reader.close();
    return ret;
  }
  
  @Before
  public void setup() {
    assumeTrue(!Path.WINDOWS);
    File f = new File("./src/test/resources/mock-container-executor");
    if(!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    mockExec = new LinuxContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    mockExec.setConf(conf);
  }

  @After
  public void tearDown() {
    deleteMockParamFile();
  }
  
  @Test
  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.LAUNCH_CONTAINER.getValue());
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();
    
    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    
    when(cId.toString()).thenReturn(containerId);
    
    when(context.getEnvironment()).thenReturn(env);
    
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");
    Path workDir = new Path("/tmp");
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    assertEquals(0, ret);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(",", dirsHandler.getLocalDirs()),
        StringUtils.join(",", dirsHandler.getLogDirs()), "cgroups=none"),
        readMockParams());
    
  }

  @Test (timeout = 5000)
  public void testContainerLaunchWithPriority() throws IOException {

    // set the scheduler priority to make sure still works with nice -n prio
    File f = new File("./src/test/resources/mock-container-executor");
    if (!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.setInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 2);

    mockExec.setConf(conf);
    List<String> command = new ArrayList<String>();
    mockExec.addSchedPriorityCommand(command);
    assertEquals("first should be nice", "nice", command.get(0));
    assertEquals("second should be -n", "-n", command.get(1));
    assertEquals("third should be the priority", Integer.toString(2),
                 command.get(2));

    testContainerLaunch();
  }

  @Test (timeout = 5000)
  public void testLaunchCommandWithoutPriority() throws IOException {
    // make sure the command doesn't contain the nice -n since priority
    // not specified
   List<String> command = new ArrayList<String>();
    mockExec.addSchedPriorityCommand(command);
    assertEquals("addSchedPriority should be empty", 0, command.size());
  }
  
  @Test (timeout = 5000)
  public void testStartLocalizer() throws IOException {

  
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 8040);
    Path nmPrivateCTokensPath= new Path("file:///bin/nmPrivateCTokensPath");
 
    try {
      mockExec.startLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345", dirsHandler.getLocalDirs(), dirsHandler.getLogDirs());
      List<String> result=readMockParams();
      Assert.assertEquals(result.size(), 17);
      Assert.assertEquals(result.get(0), YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
      Assert.assertEquals(result.get(1), "test");
      Assert.assertEquals(result.get(2), "0" );
      Assert.assertEquals(result.get(3),"application_0" );
      Assert.assertEquals(result.get(4), "/bin/nmPrivateCTokensPath");
      Assert.assertEquals(result.get(8), "-classpath" );
      Assert.assertEquals(result.get(11),"org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer" );
      Assert.assertEquals(result.get(12), "test");
      Assert.assertEquals(result.get(13), "application_0");
      Assert.assertEquals(result.get(14),"12345" );
      Assert.assertEquals(result.get(15),"localhost" );
      Assert.assertEquals(result.get(16),"8040" );

    } catch (InterruptedException e) {
      LOG.error("Error:"+e.getMessage(),e);
      Assert.fail();
    }
  }
  
  
  @Test
  public void testContainerLaunchError() throws IOException {

    // reinitialize executer
    File f = new File("./src/test/resources/mock-container-executer-with-error");
    if (!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "file:///bin/echo");
    conf.set(YarnConfiguration.NM_LOG_DIRS, "file:///dev/null");

    mockExec = spy(new LinuxContainerExecutor());
    doAnswer(
        new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock)
              throws Throwable {
             String diagnostics = (String) invocationOnMock.getArguments()[0];
            assertTrue("Invalid Diagnostics message: " + diagnostics,
                diagnostics.contains("badcommand"));
            return null;
          }
        }
    ).when(mockExec).logOutput(any(String.class));
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    mockExec.setConf(conf);

    String appSubmitter = "nobody";
    String cmd = String
        .valueOf(LinuxContainerExecutor.Commands.LAUNCH_CONTAINER.getValue());
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String, String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    doAnswer(
        new Answer() {
          @Override
          public Object answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            ContainerDiagnosticsUpdateEvent event =
                (ContainerDiagnosticsUpdateEvent) invocationOnMock
                    .getArguments()[0];
            assertTrue("Invalid Diagnostics message: " +
                event.getDiagnosticsUpdate(),
                event.getDiagnosticsUpdate().contains("badcommand"));
            return null;
          }
        }
    ).when(container).handle(any(ContainerDiagnosticsUpdateEvent.class));
    
    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);

    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");
    Path workDir = new Path("/tmp");
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    Assert.assertNotSame(0, ret);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(",", dirsHandler.getLocalDirs()),
        StringUtils.join(",", dirsHandler.getLogDirs()),
        "cgroups=none"), readMockParams());

  }
  
  @Test
  public void testInit() throws Exception {

    mockExec.init();
    assertEquals(Arrays.asList("--checksetup"), readMockParams());
    
  }

  
  @Test
  public void testContainerKill() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.SIGNAL_CONTAINER.getValue());
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());
    
    mockExec.signalContainer(appSubmitter, "1000", signal);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "1000", sigVal),
        readMockParams());
  }
  
  @Test
  public void testDeleteAsUser() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.DELETE_AS_USER.getValue());
    Path dir = new Path("/tmp/testdir");
    
    mockExec.deleteAsUser(appSubmitter, dir);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());
  }
}
