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

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DefaultLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TestLinuxContainerExecutorWithMocks {

  private static final Logger LOG =
       LoggerFactory.getLogger(TestLinuxContainerExecutorWithMocks.class);

  private static final String MOCK_EXECUTOR =
      "./src/test/resources/mock-container-executor";
  private static final String MOCK_EXECUTOR_WITH_ERROR =
      "./src/test/resources/mock-container-executer-with-error";
  private static final String MOCK_EXECUTOR_WITH_CONFIG_ERROR =
      "./src/test/resources/mock-container-executer-with-configuration-error";

  private String tmpMockExecutor;
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

  private void setupMockExecutor(String executorPath, Configuration conf)
      throws IOException {
    //we'll always use the tmpMockExecutor - since
    // PrivilegedOperationExecutor can only be initialized once.

    Files.copy(Paths.get(executorPath), Paths.get(tmpMockExecutor),
        REPLACE_EXISTING);

    File executor = new File(tmpMockExecutor);

    if (!FileUtil.canExecute(executor)) {
      FileUtil.setExecutable(executor, true);
    }
    String executorAbsolutePath = executor.getAbsolutePath();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        executorAbsolutePath);
  }

  @Before
  public void setup() throws IOException, ContainerExecutionException {
    assumeNotWindows();

    tmpMockExecutor = System.getProperty("test.build.data") +
        "/tmp-mock-container-executor";

    Configuration conf = new YarnConfiguration();
    LinuxContainerRuntime linuxContainerRuntime;

    setupMockExecutor(MOCK_EXECUTOR, conf);
    linuxContainerRuntime = new DefaultLinuxContainerRuntime(
        PrivilegedOperationExecutor.getInstance(conf));
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    linuxContainerRuntime.initialize(conf);
    mockExec = new LinuxContainerExecutor(linuxContainerRuntime);
    mockExec.setConf(conf);
  }

  @After
  public void tearDown() {
    deleteMockParamFile();
  }
  
  @Test
  public void testContainerLaunch()
      throws IOException, ConfigurationException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        PrivilegedOperation.RunAsUserCommand.LAUNCH_CONTAINER.getValue());
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
    int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(scriptPath)
        .setNmPrivateTokensPath(tokensPath)
        .setUser(appSubmitter)
        .setAppId(appId)
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .setFilecacheDirs(new ArrayList<>())
        .setUserLocalDirs(new ArrayList<>())
        .setContainerLocalDirs(new ArrayList<>())
        .setContainerLogDirs(new ArrayList<>())
        .build());
    assertEquals(0, ret);
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLocalDirs()),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            dirsHandler.getLogDirs()), "cgroups=none"),
        readMockParams());
    
  }

  @Test (timeout = 5000)
  public void testContainerLaunchWithPriority()
      throws IOException, ConfigurationException {

    // set the scheduler priority to make sure still works with nice -n prio
    Configuration conf = new Configuration();
    setupMockExecutor(MOCK_EXECUTOR, conf);
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
    Configuration conf = mockExec.getConf();
    conf.unset(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    mockExec.setConf(conf);
    mockExec.addSchedPriorityCommand(command);
    assertEquals("addSchedPriority should be empty", 0, command.size());
  }
  
  @Test (timeout = 5000)
  public void testStartLocalizer() throws IOException {

  
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 8040);
    Path nmPrivateCTokensPath= new Path("file:///bin/nmPrivateCTokensPath");
 
    try {
      mockExec.startLocalizer(new LocalizerStartContext.Builder()
          .setNmPrivateContainerTokens(nmPrivateCTokensPath)
          .setNmAddr(address)
          .setUser("test")
          .setAppId("application_0")
          .setLocId("12345")
          .setDirsHandler(dirsHandler)
          .build());

      List<String> result=readMockParams();
      Assert.assertEquals(result.size(), 19);
      Assert.assertEquals(result.get(0), YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
      Assert.assertEquals(result.get(1), "test");
      Assert.assertEquals(result.get(2), "0" );
      Assert.assertEquals(result.get(3),"application_0" );
      Assert.assertEquals(result.get(4), "/bin/nmPrivateCTokensPath");
      Assert.assertEquals(result.get(8), "-classpath" );
      Assert.assertEquals(result.get(11), "-Xmx256m" );
      Assert.assertEquals(result.get(12),"org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer" );
      Assert.assertEquals(result.get(13), "test");
      Assert.assertEquals(result.get(14), "application_0");
      Assert.assertEquals(result.get(15),"12345" );
      Assert.assertEquals(result.get(16),"localhost" );
      Assert.assertEquals(result.get(17),"8040" );

    } catch (InterruptedException e) {
      LOG.error("Error:"+e.getMessage(),e);
      Assert.fail();
    }
  }
  
  
  @Test
  public void testContainerLaunchError()
      throws IOException, ContainerExecutionException {

    final String[] expecetedMessage = {"badcommand", "Exit code: 24"};
    final String[] executor = {
        MOCK_EXECUTOR_WITH_ERROR,
        MOCK_EXECUTOR_WITH_CONFIG_ERROR
    };

    for (int i = 0; i < expecetedMessage.length; ++i) {
      final int j = i;
      // reinitialize executer
      Configuration conf = new Configuration();
      setupMockExecutor(executor[j], conf);
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, "file:///bin/echo");
      conf.set(YarnConfiguration.NM_LOG_DIRS, "file:///dev/null");


      LinuxContainerExecutor exec;
      LinuxContainerRuntime linuxContainerRuntime = new
          DefaultLinuxContainerRuntime(PrivilegedOperationExecutor.getInstance(
              conf));

      linuxContainerRuntime.initialize(conf);
      exec = new LinuxContainerExecutor(linuxContainerRuntime);

      mockExec = spy(exec);
      doAnswer(
          new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock)
                throws Throwable {
              String diagnostics = (String) invocationOnMock.getArguments()[0];
              assertTrue("Invalid Diagnostics message: " + diagnostics,
                  diagnostics.contains(expecetedMessage[j]));
              return null;
            }
          }
      ).when(mockExec).logOutput(any(String.class));
      dirsHandler = new LocalDirsHandlerService();
      dirsHandler.init(conf);
      mockExec.setConf(conf);

      String appSubmitter = "nobody";
      String cmd = String
          .valueOf(PrivilegedOperation.RunAsUserCommand.LAUNCH_CONTAINER.
              getValue());
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
                  event.getDiagnosticsUpdate().contains(expecetedMessage[j]));
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

      try {
        int ret = mockExec.launchContainer(new ContainerStartContext.Builder()
            .setContainer(container)
            .setNmPrivateContainerScriptPath(scriptPath)
            .setNmPrivateTokensPath(tokensPath)
            .setUser(appSubmitter)
            .setAppId(appId)
            .setContainerWorkDir(workDir)
            .setLocalDirs(dirsHandler.getLocalDirs())
            .setLogDirs(dirsHandler.getLogDirs())
            .setFilecacheDirs(new ArrayList<>())
            .setUserLocalDirs(new ArrayList<>())
            .setContainerLocalDirs(new ArrayList<>())
            .setContainerLogDirs(new ArrayList<>())
            .build());

        Assert.assertNotSame(0, ret);
        assertEquals(Arrays.asList(YarnConfiguration.
                DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, appId, containerId,
            workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                dirsHandler.getLocalDirs()),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                dirsHandler.getLogDirs()),
            "cgroups=none"), readMockParams());

        assertNotEquals("Expected YarnRuntimeException",
            MOCK_EXECUTOR_WITH_CONFIG_ERROR, executor[i]);
      } catch (ConfigurationException ex) {
        assertEquals(MOCK_EXECUTOR_WITH_CONFIG_ERROR, executor[i]);
        Assert.assertEquals("Linux Container Executor reached unrecoverable " +
            "exception", ex.getMessage());
      }
    }
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
        PrivilegedOperation.RunAsUserCommand.SIGNAL_CONTAINER.getValue());
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());

    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);

    mockExec.signalContainer(new ContainerSignalContext.Builder()
        .setContainer(container)
        .setUser(appSubmitter)
        .setPid("1000")
        .setSignal(signal)
        .build());
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "1000", sigVal),
        readMockParams());
  }
  
  @Test
  public void testDeleteAsUser() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        PrivilegedOperation.RunAsUserCommand.DELETE_AS_USER.getValue());
    Path dir = new Path("/tmp/testdir");
    Path testFile = new Path("testfile");
    Path baseDir0 = new Path("/grid/0/BaseDir");
    Path baseDir1 = new Path("/grid/1/BaseDir");

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(dir)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, ""),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(testFile)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, testFile.toString(), baseDir0.toString(),
            baseDir1.toString()),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "", baseDir0.toString(), baseDir1.toString()),
        readMockParams());
    ;
    Configuration conf = new Configuration();
    setupMockExecutor(MOCK_EXECUTOR, conf);
    mockExec.setConf(conf);

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(dir)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, "/tmp/testdir"),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, ""),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(testFile)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(
        Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
            appSubmitter, cmd, testFile.toString(), baseDir0.toString(),
            baseDir1.toString()),
        readMockParams());

    mockExec.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setBasedirs(baseDir0, baseDir1)
        .build());
    assertEquals(Arrays.asList(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
        appSubmitter, cmd, "", baseDir0.toString(), baseDir1.toString()),
        readMockParams());
  }

  @Test
  public void testNoExitCodeFromPrivilegedOperation() throws Exception {
    Configuration conf = new Configuration();
    final PrivilegedOperationExecutor spyPrivilegedExecutor =
        spy(PrivilegedOperationExecutor.getInstance(conf));
    doThrow(new PrivilegedOperationException("interrupted"))
        .when(spyPrivilegedExecutor).executePrivilegedOperation(
            any(List.class), any(PrivilegedOperation.class),
            any(File.class), any(Map.class), anyBoolean(), anyBoolean());
    LinuxContainerRuntime runtime = new DefaultLinuxContainerRuntime(
        spyPrivilegedExecutor);
    runtime.initialize(conf);
    mockExec = new LinuxContainerExecutor(runtime);
    mockExec.setConf(conf);
    LinuxContainerExecutor lce = new LinuxContainerExecutor(runtime) {
      @Override
      protected PrivilegedOperationExecutor getPrivilegedOperationExecutor() {
        return spyPrivilegedExecutor;
      }
    };
    lce.setConf(conf);
    InetSocketAddress address = InetSocketAddress.createUnresolved(
        "localhost", 8040);
    Path nmPrivateCTokensPath= new Path("file:///bin/nmPrivateCTokensPath");
    LocalDirsHandlerService dirService = new LocalDirsHandlerService();
    dirService.init(conf);

    String appSubmitter = "nobody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    HashMap<String, String> env = new HashMap<>();
    Container container = mock(Container.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    when(container.getContainerId()).thenReturn(cid);
    when(container.getLaunchContext()).thenReturn(context);
    when(context.getEnvironment()).thenReturn(env);
    Path workDir = new Path("/tmp");

    try {
      lce.startLocalizer(new LocalizerStartContext.Builder()
          .setNmPrivateContainerTokens(nmPrivateCTokensPath)
          .setNmAddr(address)
          .setUser(appSubmitter)
          .setAppId(appId.toString())
          .setLocId("12345")
          .setDirsHandler(dirService)
          .build());
      Assert.fail("startLocalizer should have thrown an exception");
    } catch (IOException e) {
      assertTrue("Unexpected exception " + e,
          e.getMessage().contains("exitCode"));
    }

    lce.activateContainer(cid, new Path(workDir, "pid.txt"));
    lce.launchContainer(new ContainerStartContext.Builder()
        .setContainer(container)
        .setNmPrivateContainerScriptPath(new Path("file:///bin/echo"))
        .setNmPrivateTokensPath(new Path("file:///dev/null"))
        .setUser(appSubmitter)
        .setAppId(appId.toString())
        .setContainerWorkDir(workDir)
        .setLocalDirs(dirsHandler.getLocalDirs())
        .setLogDirs(dirsHandler.getLogDirs())
        .setFilecacheDirs(new ArrayList<>())
        .setUserLocalDirs(new ArrayList<>())
        .setContainerLocalDirs(new ArrayList<>())
        .setContainerLogDirs(new ArrayList<>())
        .build());
    lce.deleteAsUser(new DeletionAsUserContext.Builder()
        .setUser(appSubmitter)
        .setSubDir(new Path("/tmp/testdir"))
        .build());

    try {
      lce.mountCgroups(new ArrayList<String>(), "hierarchy");
      Assert.fail("mountCgroups should have thrown an exception");
    } catch (IOException e) {
      assertTrue("Unexpected exception " + e,
          e.getMessage().contains("exit code"));
    }
  }
}
