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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
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
  private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);
  private AtomicBoolean isNMShutdownCalled = new AtomicBoolean(false);
  private final NodeManagerEvent resyncEvent =
      new NodeManagerEvent(NodeManagerEventType.RESYNC);


  @Before
  public void setup() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
    tmpDir.mkdirs();
    logsDir.mkdirs();
    remoteLogsDir.mkdirs();
    nmLocalDir.mkdirs();
    syncBarrier = new CyclicBarrier(2);
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

  @SuppressWarnings("unchecked")
  protected void testContainerPreservationOnResyncImpl(TestNodeManager1 nm,
      boolean isWorkPreservingRestartEnabled)
      throws IOException, YarnException, InterruptedException {
    YarnConfiguration conf = createNMConfig();
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        isWorkPreservingRestartEnabled);

    try {
      nm.init(conf);
      nm.start();
      ContainerId cId = TestNodeManagerShutdown.createContainerId();
      TestNodeManagerShutdown.startContainer(nm, cId, localFS, tmpDir,
          processStartFile);

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

  // This test tests new container requests are blocked when NM starts from
  // scratch until it register with RM AND while NM is resyncing with RM
  @SuppressWarnings("unchecked")
  @Test(timeout=60000)
  public void testBlockNewContainerRequestsOnStartAndResync()
      throws IOException, InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager2();
    YarnConfiguration conf = createNMConfig();
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);
    nm.init(conf);
    nm.start();

    // Start the container in running state
    ContainerId cId = TestNodeManagerShutdown.createContainerId();
    TestNodeManagerShutdown.startContainer(nm, cId, localFS, tmpDir,
      processStartFile);

    nm.getNMDispatcher().getEventHandler()
      .handle(new NodeManagerEvent(NodeManagerEventType.RESYNC));
    try {
      syncBarrier.await();
    } catch (BrokenBarrierException e) {
    }
    Assert.assertFalse(assertionFailedInThread.get());
    nm.stop();
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=10000)
  public void testNMshutdownWhenResyncThrowException() throws IOException,
      InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager3();
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();
    Assert.assertEquals(1, ((TestNodeManager3) nm).getNMRegistrationCount());
    nm.getNMDispatcher().getEventHandler()
        .handle(new NodeManagerEvent(NodeManagerEventType.RESYNC));

    synchronized (isNMShutdownCalled) {
      while (isNMShutdownCalled.get() == false) {
        try {
          isNMShutdownCalled.wait();
        } catch (InterruptedException e) {
        }
      }
    }

    Assert.assertTrue("NM shutdown not called.",isNMShutdownCalled.get());
    nm.stop();
  }


  // This is to test when NM gets the resync response from last heart beat, it
  // should be able to send the already-sent-via-last-heart-beat container
  // statuses again when it re-register with RM.
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
    nm.init(conf);
    nm.start();

    try {
      syncBarrier.await();
    } catch (BrokenBarrierException e) {
    }
    Assert.assertFalse(assertionFailedInThread.get());
    nm.stop();
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

  private YarnConfiguration createNMConfig() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5*1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:12345");
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:12346");
    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      remoteLogsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    return conf;
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
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new TestNodeStatusUpdaterImpl1(context, dispatcher,
          healthChecker, metrics);
    }

    public int getNMRegistrationCount() {
      return registrationCount;
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
        try {
          try {
            if (containersShouldBePreserved) {
              Assert.assertFalse(containers.isEmpty());
              Assert.assertTrue(containers.containsKey(existingCid));
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

  class TestNodeManager2 extends NodeManager {

    Thread launchContainersThread = null;
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new TestNodeStatusUpdaterImpl2(context, dispatcher,
        healthChecker, metrics);
    }

    @Override
    protected ContainerManagerImpl createContainerManager(Context context,
        ContainerExecutor exec, DeletionService del,
        NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
        metrics, aclsManager, dirsHandler){
        @Override
        public void setBlockNewContainerRequests(
            boolean blockNewContainerRequests) {
          if (blockNewContainerRequests) {
            // start test thread right after blockNewContainerRequests is set
            // true
            super.setBlockNewContainerRequests(blockNewContainerRequests);
            launchContainersThread = new RejectedContainersLauncherThread();
            launchContainersThread.start();
          } else {
            // join the test thread right before blockNewContainerRequests is
            // reset
            try {
              // stop the test thread
              ((RejectedContainersLauncherThread) launchContainersThread)
                .setStopThreadFlag(true);
              launchContainersThread.join();
              ((RejectedContainersLauncherThread) launchContainersThread)
              .setStopThreadFlag(false);
              super.setBlockNewContainerRequests(blockNewContainerRequests);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      };
    }

    class TestNodeStatusUpdaterImpl2 extends MockNodeStatusUpdater {

      public TestNodeStatusUpdaterImpl2(Context context, Dispatcher dispatcher,
          NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
        super(context, dispatcher, healthChecker, metrics);
      }

      @Override
      protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
        ConcurrentMap<ContainerId, org.apache.hadoop.yarn.server.nodemanager
        .containermanager.container.Container> containers =
            getNMContext().getContainers();

        try {
          // ensure that containers are empty before restart nodeStatusUpdater
          if (!containers.isEmpty()) {
            for (Container container: containers.values()) {
              Assert.assertEquals(ContainerState.COMPLETE,
                  container.cloneAndGetContainerStatus().getState());
            }
          }
          super.rebootNodeStatusUpdaterAndRegisterWithRM();
          // After this point new containers are free to be launched, except
          // containers from previous RM
          // Wait here so as to sync with the main test thread.
          syncBarrier.await();
        } catch (InterruptedException e) {
        } catch (BrokenBarrierException e) {
        } catch (AssertionError ae) {
          ae.printStackTrace();
          assertionFailedInThread.set(true);
        }
      }
    }

    class RejectedContainersLauncherThread extends Thread {

      boolean isStopped = false;
      public void setStopThreadFlag(boolean isStopped) {
        this.isStopped = isStopped;
      }

      @Override
      public void run() {
        int numContainers = 0;
        int numContainersRejected = 0;
        ContainerLaunchContext containerLaunchContext =
            recordFactory.newRecordInstance(ContainerLaunchContext.class);
        try {
          while (!isStopped && numContainers < 10) {
            StartContainerRequest scRequest =
                StartContainerRequest.newInstance(containerLaunchContext,
                  null);
            List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
            list.add(scRequest);
            StartContainersRequest allRequests =
                StartContainersRequest.newInstance(list);
            System.out.println("no. of containers to be launched: "
                + numContainers);
            numContainers++;
            try {
              getContainerManager().startContainers(allRequests);
            } catch (YarnException e) {
              numContainersRejected++;
              Assert.assertTrue(e.getMessage().contains(
                "Rejecting new containers as NodeManager has not" +
                " yet connected with ResourceManager"));
              Assert.assertEquals(NMNotYetReadyException.class.getName(), e
                .getClass().getName());
            } catch (IOException e) {
              e.printStackTrace();
              assertionFailedInThread.set(true);
            }
          }
          // no. of containers to be launched should equal to no. of
          // containers rejected
          Assert.assertEquals(numContainers, numContainersRejected);
        } catch (AssertionError ae) {
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
    protected void shutDown() {
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

  public static NMContainerStatus createNMContainerStatus(int id,
      ContainerState containerState) {
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(applicationAttemptId, id);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, containerState,
          Resource.newInstance(1024, 1), "recover container", 0,
          Priority.newInstance(10), 0);
    return containerReport;
  }
}
