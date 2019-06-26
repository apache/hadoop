package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests to verify all the Recovered Container's Launcher Events in
 * {@link RecoveredContainerLaunch} are handled as expected.
 */
public class TestRecoveredContainerLaunch extends BaseContainerManagerTest {

  public TestRecoveredContainerLaunch() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    conf.setClass(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    super.setup();
    context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), dirsHandler,
        new ApplicationACLsManager(conf), new NMNullStateStoreService(), false,
        conf) {
      public int getHttpPort() {
        return HTTP_PORT;
      }
      @Override
      public ContainerExecutor getContainerExecutor() {
        return exec;
      }
    };
  }

  @SuppressWarnings("rawtypes")
  @Test(timeout = 10000)
  public void testRecoveryContainerExitCode() throws IOException, YarnException {
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            System.currentTimeMillis(), 1), 1), 1));
    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    when(clc.getCommands()).thenReturn(Collections.<String>emptyList());
    when(container.getLaunchContext()).thenReturn(clc);
    when(container.getLocalizedResources())
        .thenReturn(Collections.<Path, List<String>> emptyMap());
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler<Event> eventHandler = new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        if(event instanceof ContainerExitEvent){
          ContainerExitEvent exitEvent = (ContainerExitEvent) event;
        }
      }
    };
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    RecoveredContainerLaunch launch = new RecoveredContainerLaunch(context, new Configuration(),
        dispatcher, exec, null, container, dirsHandler, containerManager);
    String pidFileSubpath = launch
        .getPidFileSubpath(container.getContainerId().getApplicationAttemptId().getApplicationId().toString(),
            container.getContainerId().toString());
    File pidFile = new File(this.dirsHandler.getLocalDirsForRead().get(0) + "/" + pidFileSubpath);
    File exitCodeFile = new File(this.dirsHandler.getLocalDirsForRead().get(0) + "/" + pidFileSubpath+".exitcode");
    new File(pidFile.getParent()).mkdirs();
    PrintWriter fileWriter = new PrintWriter(pidFile);
    PrintWriter exitCodeFileWriter = new PrintWriter(exitCodeFile);
    fileWriter.println("2");
    exitCodeFileWriter.println(ContainerExitStatus.KILLED_EXCEEDED_PMEM);
    fileWriter.close();
    exitCodeFileWriter.close();
    Integer exitCode = launch.call();
    Assert.assertEquals(exitCode,Integer.valueOf(ContainerExitStatus.KILLED_EXCEEDED_PMEM));
  }
}
