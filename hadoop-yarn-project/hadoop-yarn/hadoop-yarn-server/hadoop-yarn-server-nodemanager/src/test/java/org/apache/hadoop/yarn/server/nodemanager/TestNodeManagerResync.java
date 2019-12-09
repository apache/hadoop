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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeManagerResync {
  static final File basedir =
      new File("target", TestNodeManagerResync.class.getName());
  static final File tmpDir = new File(basedir, "tmpDir");
  static final File logsDir = new File(basedir, "logs");
  static final File remoteLogsDir = new File(basedir, "remotelogs");
  static final File nmLocalDir = new File(basedir, "nm0");
  static final File processStartFile = new File(tmpDir, "start_file.txt")
    .getAbsoluteFile();

  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  static final String user = "nobody";
  private FileContext localFS;
  private CyclicBarrier syncBarrier;
  private CyclicBarrier updateBarrier;
  private AtomicInteger resyncThreadCount;
  private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);
  private AtomicBoolean isNMShutdownCalled = new AtomicBoolean(false);
  private final NodeManagerEvent resyncEvent =
      new NodeManagerEvent(NodeManagerEventType.RESYNC);
  private final long DUMMY_RM_IDENTIFIER = 1234;

  protected static final Logger LOG =
       LoggerFactory.getLogger(TestNodeManagerResync.class);

  @Before
  public void setup() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
    tmpDir.mkdirs();
    logsDir.mkdirs();
    remoteLogsDir.mkdirs();
    nmLocalDir.mkdirs();
    syncBarrier = new CyclicBarrier(2);
    updateBarrier = new CyclicBarrier(2);
    resyncThreadCount = new AtomicInteger(0);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    localFS.delete(new Path(basedir.getPath()), true);
    assertionFailedInThread.set(false);
  }

  @Test
  public void testKillContainersOnResync() throws IOException,
      InterruptedException, YarnException {
    TestNodeManager1 nm = new TestNodeManager1(false);

    testContainerPreservationOnResyncImpl(nm, false);
  }

  @Test
  public void testPreserveContainersOnResyncKeepingContainers() throws
      IOException,
      InterruptedException, YarnException {
    TestNodeManager1 nm = new TestNodeManager1(true);

    testContainerPreservationOnResyncImpl(nm, true);
  }

  protected void testContainerPreservationOnResyncImpl(TestNodeManager1 nm,
      boolean isWorkPreservingRestartEnabled)
      throws IOException, YarnException, InterruptedException {
    int port = ServerSocketUtil.getPort(49153, 10);
    YarnConfiguration conf = createNMConfig(port);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        isWorkPreservingRestartEnabled);

    try {
      nm.init(conf);
      nm.start();
      ContainerId cId = TestNodeManagerShutdown.createContainerId();
      TestNodeManagerShutdown.startContainer(nm, cId, localFS, tmpDir,
          processStartFile, port);

      nm.setExistingContainerId(cId);
      Assert.assertEquals(1, ((TestNodeManager1) nm).getNMRegistrationCount());
      nm.getNMDispatcher().getEventHandler().handle(resyncEvent);
      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
      }
      Assert.assertEquals(2, ((TestNodeManager1) nm).getNMRegistrationCount());
      // Only containers should be killed on resync, apps should lie around.
      // That way local resources for apps can be used beyond resync without
      // relocalization
      Assert.assertTrue(nm.getNMContext().getApplications()
          .containsKey(cId.getApplicationAttemptId().getApplicationId()));
      Assert.assertFalse(assertionFailedInThread.get());
    }
    finally {
      nm.stop();
    }
  }

  @SuppressWarnings("resource")
  @Test(timeout = 30000)
  public void testNMMultipleResyncEvent()
      throws IOException, InterruptedException {
    TestNodeManager1 nm = new TestNodeManager1(false);
    YarnConfiguration conf = createNMConfig();

    int resyncEventCount = 4;
    try {
      nm.init(conf);
      nm.start();
      Assert.assertEquals(1, nm.getNMRegistrationCount());
      for (int i = 0; i < resyncEventCount; i++) {
        nm.getNMDispatcher().getEventHandler().handle(resyncEvent);
      }

      DrainDispatcher dispatcher = (DrainDispatcher) nm.getNMDispatcher();
      dispatcher.await();
      LOG.info("NM dispatcher drained");

      // Wait for the resync thread to finish
      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
      }
      LOG.info("Barrier wait done for the resync thread");

      // Resync should only happen once
      Assert.assertEquals(2, nm.getNMRegistrationCount());
      Assert.assertFalse("NM shutdown called.", isNMShutdownCalled.get());
    } finally {
      nm.stop();
    }
  }

  @SuppressWarnings("resource")
  @Test(timeout=10000)
  public void testNMshutdownWhenResyncThrowException() throws IOException,
      InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager3();
    YarnConfiguration conf = createNMConfig();
    try {
      nm.init(conf);
      nm.start();
      Assert.assertEquals(1, ((TestNodeManager3) nm).getNMRegistrationCount());
      nm.getNMDispatcher().getEventHandler()
          .handle(new NodeManagerEvent(NodeManagerEventType.RESYNC));

      synchronized (isNMShutdownCalled) {
        while (!isNMShutdownCalled.get()) {
          try {
            isNMShutdownCalled.wait();
          } catch (InterruptedException e) {
          }
        }
      }

      Assert.assertTrue("NM shutdown not called.", isNMShutdownCalled.get());
    } finally {
      nm.stop();
    }
  }

  @SuppressWarnings("resource")
  @Test(timeout=60000)
  public void testContainerResourceIncreaseIsSynchronizedWithRMResync()
      throws IOException, InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager4();
    YarnConfiguration conf = createNMConfig();
    conf.setBoolean(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    try {
      nm.init(conf);
      nm.start();
      // Start a container and make sure it is in RUNNING state
      ((TestNodeManager4) nm).startContainer();
      // Simulate a container resource increase in a separate thread
      ((TestNodeManager4) nm).updateContainerResource();
      // Simulate RM restart by sending a RESYNC event
      LOG.info("Sending out RESYNC event");
      nm.getNMDispatcher().getEventHandler()
          .handle(new NodeManagerEvent(NodeManagerEventType.RESYNC));
      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
        e.printStackTrace();
      }
      Assert.assertFalse(assertionFailedInThread.get());
    } finally {
      nm.stop();
    }
  }

  // This is to test when NM gets the resync response from last heart beat, it
  // should be able to send the already-sent-via-last-heart-beat container
  // statuses again when it re-register with RM.
  @SuppressWarnings("resource")
  @Test
  public void testNMSentContainerStatusOnResync() throws Exception {
    final ContainerStatus testCompleteContainer =
        TestNodeStatusUpdater.createContainerStatus(2, ContainerState.COMPLETE);
    final Container container =
        TestNodeStatusUpdater.getMockContainer(testCompleteContainer);
    NMContainerStatus report =
        createNMContainerStatus(2, ContainerState.COMPLETE);
    when(container.getNMContainerStatus()).thenReturn(report);
    NodeManager nm = new NodeManager() {
      int registerCount = 0;

      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        return new TestNodeStatusUpdaterResync(context, dispatcher,
          healthChecker, metrics) {
          @Override
          protected ResourceTracker createResourceTracker() {
            return new MockResourceTracker() {
              @Override
              public RegisterNodeManagerResponse registerNodeManager(
                  RegisterNodeManagerRequest request) throws YarnException,
                  IOException {
                if (registerCount == 0) {
                  // first register, no containers info.
                  try {
                    Assert.assertEquals(0, request.getNMContainerStatuses()
                      .size());
                  } catch (AssertionError error) {
                    error.printStackTrace();
                    assertionFailedInThread.set(true);
                  }
                  // put the completed container into the context
                  getNMContext().getContainers().put(
                    testCompleteContainer.getContainerId(), container);
                  getNMContext().getApplications().put(
                      testCompleteContainer.getContainerId()
                          .getApplicationAttemptId().getApplicationId(),
                      mock(Application.class));
                } else {
                  // second register contains the completed container info.
                  List<NMContainerStatus> statuses =
                      request.getNMContainerStatuses();
                  try {
                    Assert.assertEquals(1, statuses.size());
                    Assert.assertEquals(testCompleteContainer.getContainerId(),
                      statuses.get(0).getContainerId());
                  } catch (AssertionError error) {
                    error.printStackTrace();
                    assertionFailedInThread.set(true);
                  }
                }
                registerCount++;
                return super.registerNodeManager(request);
              }

              @Override
              public NodeHeartbeatResponse nodeHeartbeat(
                  NodeHeartbeatRequest request) {
                // first heartBeat contains the completed container info
                List<ContainerStatus> statuses =
                    request.getNodeStatus().getContainersStatuses();
                try {
                  Assert.assertEquals(1, statuses.size());
                  Assert.assertEquals(testCompleteContainer.getContainerId(),
                    statuses.get(0).getContainerId());
                } catch (AssertionError error) {
                  error.printStackTrace();
                  assertionFailedInThread.set(true);
                }

                // notify RESYNC on first heartbeat.
                return YarnServerBuilderUtils.newNodeHeartbeatResponse(1,
                  NodeAction.RESYNC, null, null, null, null, 1000L);
              }
            };
          }
        };
      }
    };
    YarnConfiguration conf = createNMConfig();
    try {
      nm.init(conf);
      nm.start();

      try {
        syncBarrier.await();
      } catch (BrokenBarrierException e) {
      }
      Assert.assertFalse(assertionFailedInThread.get());
    } finally {
      nm.stop();
    }
  }

  // This can be used as a common base class for testing NM resync behavior.
  class TestNodeStatusUpdaterResync extends MockNodeStatusUpdater {
    public TestNodeStatusUpdaterResync(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
    }
    @Override
    protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
      try {
        // Wait here so as to sync with the main test thread.
        super.rebootNodeStatusUpdaterAndRegisterWithRM();
        syncBarrier.await();
      } catch (InterruptedException e) {
      } catch (BrokenBarrierException e) {
      } catch (AssertionError ae) {
        ae.printStackTrace();
        assertionFailedInThread.set(true);
      }
    }
  }

  private YarnConfiguration createNMConfig(int port) throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5*1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:" + port);
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:"
        + ServerSocketUtil.getPort(49155, 10));
    conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS,
        "127.0.0.1:" + ServerSocketUtil
            .getPort(YarnConfiguration.DEFAULT_NM_WEBAPP_PORT, 10));
    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      remoteLogsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    return conf;
  }

  private YarnConfiguration createNMConfig() throws IOException {
    return createNMConfig(ServerSocketUtil.getPort(49156, 10));
  }

  class TestNodeManager1 extends NodeManager {

    private int registrationCount = 0;
    private boolean containersShouldBePreserved;
    private ContainerId existingCid;

    public TestNodeManager1(boolean containersShouldBePreserved) {
      this.containersShouldBePreserved = containersShouldBePreserved;
    }

    public void setExistingContainerId(ContainerId cId) {
      existingCid = cId;
    }

    @Override
    protected AsyncDispatcher createNMDispatcher() {
      return new DrainDispatcher();
    }

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new TestNodeStatusUpdaterImpl1(context, dispatcher,
          healthChecker, metrics);
    }

    public int getNMRegistrationCount() {
      return registrationCount;
    }

    @Override
    protected void shutDown(int exitCode) {
      synchronized (isNMShutdownCalled) {
        isNMShutdownCalled.set(true);
        isNMShutdownCalled.notify();
      }
    }

    class TestNodeStatusUpdaterImpl1 extends MockNodeStatusUpdater {

      public TestNodeStatusUpdaterImpl1(Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
        super(context, dispatcher, healthChecker, metrics);
      }

      @Override
      protected void registerWithRM() throws YarnException, IOException {
        super.registerWithRM();
        registrationCount++;
      }

      @Override
      protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
        ConcurrentMap<ContainerId, org.apache.hadoop.yarn.server.nodemanager
        .containermanager.container.Container> containers =
            getNMContext().getContainers();
        if (resyncThreadCount.incrementAndGet() > 1) {
          throw new YarnRuntimeException("Multiple resync thread created!");
        }
        try {
          try {
            if (containersShouldBePreserved) {
              Assert.assertFalse(containers.isEmpty());
              Assert.assertTrue(containers.containsKey(existingCid));
              ContainerState state = containers.get(existingCid)
                  .cloneAndGetContainerStatus().getState();
              // Wait till RUNNING state...
              int counter = 50;
              while (state != ContainerState.RUNNING && counter > 0) {
                Thread.sleep(100);
                counter--;
              }
              Assert.assertEquals(ContainerState.RUNNING,
                  containers.get(existingCid)
                  .cloneAndGetContainerStatus().getState());
            } else {
              // ensure that containers are empty or are completed before
              // restart nodeStatusUpdater
              if (!containers.isEmpty()) {
                Assert.assertEquals(ContainerState.COMPLETE,
                    containers.get(existingCid)
                        .cloneAndGetContainerStatus().getState());
              }
            }
            super.rebootNodeStatusUpdaterAndRegisterWithRM();
          }
          catch (AssertionError ae) {
            ae.printStackTrace();
            assertionFailedInThread.set(true);
          }
          finally {
            syncBarrier.await();
          }
        } catch (InterruptedException e) {
        } catch (BrokenBarrierException e) {
        } catch (AssertionError ae) {
          ae.printStackTrace();
          assertionFailedInThread.set(true);
        }
      }
    }
  }

  class TestNodeManager3 extends NodeManager {

    private int registrationCount = 0;

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new TestNodeStatusUpdaterImpl3(context, dispatcher, healthChecker,
          metrics);
    }

    public int getNMRegistrationCount() {
      return registrationCount;
    }

    @Override
    protected void shutDown(int exitCode) {
      synchronized (isNMShutdownCalled) {
        isNMShutdownCalled.set(true);
        isNMShutdownCalled.notify();
      }
    }

    class TestNodeStatusUpdaterImpl3 extends MockNodeStatusUpdater {

      public TestNodeStatusUpdaterImpl3(Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
        super(context, dispatcher, healthChecker, metrics);
      }

      @Override
      protected void registerWithRM() throws YarnException, IOException {
        super.registerWithRM();
        registrationCount++;
        if (registrationCount > 1) {
          throw new YarnRuntimeException("Registration with RM failed.");
        }
      }
    }}

  class TestNodeManager4 extends NodeManager {

    private Thread containerUpdateResourceThread = null;

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new TestNodeStatusUpdaterImpl4(context, dispatcher,
          healthChecker, metrics);
    }

    @Override
    protected ContainerManagerImpl createContainerManager(Context context,
        ContainerExecutor exec, DeletionService del,
        NodeStatusUpdater nodeStatusUpdater,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
          metrics, dirsHandler){

        @Override
        protected void authorizeGetAndStopContainerRequest(
            ContainerId containerId, Container container,
            boolean stopRequest, NMTokenIdentifier identifier)
            throws YarnException {
          // do nothing
        }
        @Override
        protected void authorizeUser(UserGroupInformation remoteUgi,
            NMTokenIdentifier nmTokenIdentifier) {
          // do nothing
        }
        @Override
        protected void authorizeStartAndResourceIncreaseRequest(
            NMTokenIdentifier nmTokenIdentifier,
            ContainerTokenIdentifier containerTokenIdentifier,
            boolean startRequest) throws YarnException {
          try {
            // Sleep 2 seconds to simulate a pro-longed increase action.
            // If during this time a RESYNC event is sent by RM, the
            // resync action should block until the increase action is
            // completed.
            // See testContainerResourceIncreaseIsSynchronizedWithRMResync()
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        @Override
        protected void updateNMTokenIdentifier(
            NMTokenIdentifier nmTokenIdentifier)
                throws SecretManager.InvalidToken {
          // Do nothing
        }
        @Override
        public Map<String, ByteBuffer> getAuxServiceMetaData() {
          return new HashMap<>();
        }
        @Override
        protected NMTokenIdentifier selectNMTokenIdentifier(
            UserGroupInformation remoteUgi) {
          return new NMTokenIdentifier();
        }
      };
    }

    // Start a container in NM
    public void startContainer()
        throws IOException, InterruptedException, YarnException {
      LOG.info("Start a container and wait until it is in RUNNING state");
      File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
      PrintWriter fileWriter = new PrintWriter(scriptFile);
      if (Shell.WINDOWS) {
        fileWriter.println("@ping -n 100 127.0.0.1 >nul");
      } else {
        fileWriter.write("\numask 0");
        fileWriter.write("\nexec sleep 100");
      }
      fileWriter.close();
      ContainerLaunchContext containerLaunchContext =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      URL resource_alpha =
          URL.fromPath(localFS
              .makeQualified(new Path(scriptFile.getAbsolutePath())));
      LocalResource rsrc_alpha =
          recordFactory.newRecordInstance(LocalResource.class);
      rsrc_alpha.setResource(resource_alpha);
      rsrc_alpha.setSize(-1);
      rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
      rsrc_alpha.setType(LocalResourceType.FILE);
      rsrc_alpha.setTimestamp(scriptFile.lastModified());
      String destinationFile = "dest_file";
      Map<String, LocalResource> localResources =
          new HashMap<String, LocalResource>();
      localResources.put(destinationFile, rsrc_alpha);
      containerLaunchContext.setLocalResources(localResources);
      List<String> commands =
          Arrays.asList(Shell.getRunScriptCommand(scriptFile));
      containerLaunchContext.setCommands(commands);
      Resource resource = Resource.newInstance(1024, 1);
      StartContainerRequest scRequest =
          StartContainerRequest.newInstance(
              containerLaunchContext,
              getContainerToken(resource));
      List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
      list.add(scRequest);
      StartContainersRequest allRequests =
          StartContainersRequest.newInstance(list);
      getContainerManager().startContainers(allRequests);
      // Make sure the container reaches RUNNING state
      ContainerId cId = TestContainerManager.createContainerId(0);
      BaseContainerManagerTest.waitForNMContainerState(
          getContainerManager(), cId,
          org.apache.hadoop.yarn.server.nodemanager.
              containermanager.container.ContainerState.RUNNING);
    }

    // Increase container resource in a thread
    public void updateContainerResource()
        throws InterruptedException {
      LOG.info("Increase a container resource in a separate thread");
      containerUpdateResourceThread = new ContainerUpdateResourceThread();
      containerUpdateResourceThread.start();
    }

    class TestNodeStatusUpdaterImpl4 extends MockNodeStatusUpdater {

      public TestNodeStatusUpdaterImpl4(Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
        super(context, dispatcher, healthChecker, metrics);
      }

      @Override
      protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
        try {
          try {
            // Check status before registerWithRM
            List<ContainerId> containerIds = new ArrayList<>();
            ContainerId cId = TestContainerManager.createContainerId(0);
            containerIds.add(cId);
            GetContainerStatusesRequest gcsRequest =
                GetContainerStatusesRequest.newInstance(containerIds);
            ContainerStatus containerStatus = getContainerManager()
                .getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
            assertEquals(Resource.newInstance(1024, 1),
                containerStatus.getCapability());
            updateBarrier.await();
            // Call the actual rebootNodeStatusUpdaterAndRegisterWithRM().
            // This function should be synchronized with
            // updateContainer().
            updateBarrier.await();
            super.rebootNodeStatusUpdaterAndRegisterWithRM();
            // Check status after registerWithRM
            containerStatus = getContainerManager()
                .getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
            assertEquals(Resource.newInstance(4096, 2),
                containerStatus.getCapability());
          } catch (AssertionError ae) {
            ae.printStackTrace();
            assertionFailedInThread.set(true);
          }   finally {
            syncBarrier.await();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    class ContainerUpdateResourceThread extends Thread {
      @Override
      public void run() {
        // Construct container resource increase request
        List<Token> increaseTokens = new ArrayList<Token>();
        // Add increase request.
        Resource targetResource = Resource.newInstance(4096, 2);
        try{
          try {
            updateBarrier.await();
            increaseTokens.add(getContainerToken(targetResource, 1));
            ContainerUpdateRequest updateRequest =
                ContainerUpdateRequest.newInstance(increaseTokens);
            ContainerUpdateResponse updateResponse =
                getContainerManager()
                    .updateContainer(updateRequest);
            Assert.assertEquals(
                1, updateResponse.getSuccessfullyUpdatedContainers()
                    .size());
            Assert.assertTrue(updateResponse.getFailedRequests().isEmpty());
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            updateBarrier.await();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private Token getContainerToken(Resource resource) throws IOException {
      ContainerId cId = TestContainerManager.createContainerId(0);
      return TestContainerManager.createContainerToken(
          cId, DUMMY_RM_IDENTIFIER,
          getNMContext().getNodeId(), user, resource,
          getNMContext().getContainerTokenSecretManager(), null);
    }

    private Token getContainerToken(Resource resource, int version)
        throws IOException {
      ContainerId cId = TestContainerManager.createContainerId(0);
      return TestContainerManager.createContainerToken(
          cId, version, DUMMY_RM_IDENTIFIER,
          getNMContext().getNodeId(), user, resource,
          getNMContext().getContainerTokenSecretManager(), null);
    }
  }

  public static NMContainerStatus createNMContainerStatus(int id,
      ContainerState containerState) {
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(applicationAttemptId, id);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, 0, containerState,
          Resource.newInstance(1024, 1), "recover container", 0,
          Priority.newInstance(10), 0);
    return containerReport;
  }
}
