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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.After;
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

  @SuppressWarnings("unchecked")
  @Test
  public void testKillContainersOnResync() throws IOException,
      InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager1();
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();
    ContainerId cId = TestNodeManagerShutdown.createContainerId();
    TestNodeManagerShutdown.startContainer(nm, cId, localFS, tmpDir,
      processStartFile);

    Assert.assertEquals(1, ((TestNodeManager1) nm).getNMRegistrationCount());
    nm.getNMDispatcher().getEventHandler().
        handle( new NodeManagerEvent(NodeManagerEventType.RESYNC));
    try {
      syncBarrier.await();
    } catch (BrokenBarrierException e) {
    }
    Assert.assertEquals(2, ((TestNodeManager1) nm).getNMRegistrationCount());
    // Only containers should be killed on resync, apps should lie around. That
    // way local resources for apps can be used beyond resync without
    // relocalization
    Assert.assertTrue(nm.getNMContext().getApplications()
      .containsKey(cId.getApplicationAttemptId().getApplicationId()));
    Assert.assertFalse(assertionFailedInThread.get());

    nm.stop();
  }

  // This test tests new container requests are blocked when NM starts from
  // scratch until it register with RM AND while NM is resyncing with RM
  @SuppressWarnings("unchecked")
  @Test
  public void testBlockNewContainerRequestsOnStartAndResync()
      throws IOException, InterruptedException, YarnException {
    NodeManager nm = new TestNodeManager2();
    YarnConfiguration conf = createNMConfig();
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
      protected void rebootNodeStatusUpdater() {
        ConcurrentMap<ContainerId, org.apache.hadoop.yarn.server.nodemanager
        .containermanager.container.Container> containers =
            getNMContext().getContainers();
        try {
          // ensure that containers are empty before restart nodeStatusUpdater
          Assert.assertTrue(containers.isEmpty());
          super.rebootNodeStatusUpdater();
          syncBarrier.await();
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
      protected void rebootNodeStatusUpdater() {
        ConcurrentMap<ContainerId, org.apache.hadoop.yarn.server.nodemanager
        .containermanager.container.Container> containers =
            getNMContext().getContainers();

        try {
          // ensure that containers are empty before restart nodeStatusUpdater
          Assert.assertTrue(containers.isEmpty());
          super.rebootNodeStatusUpdater();
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
}
