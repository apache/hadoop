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

package org.apache.hadoop.mapreduce.v2.app.launcher;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestContainerLauncher {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  Configuration conf;
  Server server;

  static final Log LOG = LogFactory.getLog(TestContainerLauncher.class);

  @Test (timeout = 10000)
  public void testPoolSize() throws InterruptedException {

    ApplicationId appId = ApplicationId.newInstance(12345, 67);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
      appId, 3);
    JobId jobId = MRBuilderUtils.newJobId(appId, 8);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 9, TaskType.MAP);

    AppContext context = mock(AppContext.class);
    CustomContainerLauncher containerLauncher = new CustomContainerLauncher(
      context);
    containerLauncher.init(new Configuration());
    containerLauncher.start();

    ThreadPoolExecutor threadPool = containerLauncher.getThreadPool();

    // No events yet
    Assert.assertEquals(containerLauncher.initialPoolSize,
        MRJobConfig.DEFAULT_MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE);
    Assert.assertEquals(0, threadPool.getPoolSize());
    Assert.assertEquals(containerLauncher.initialPoolSize,
      threadPool.getCorePoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    containerLauncher.expectedCorePoolSize = containerLauncher.initialPoolSize;
    for (int i = 0; i < 10; i++) {
      ContainerId containerId = ContainerId.newContainerId(appAttemptId, i);
      TaskAttemptId taskAttemptId = MRBuilderUtils.newTaskAttemptId(taskId, i);
      containerLauncher.handle(new ContainerLauncherEvent(taskAttemptId,
        containerId, "host" + i + ":1234", null,
        ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH));
    }
    waitForEvents(containerLauncher, 10);
    Assert.assertEquals(10, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    // Same set of hosts, so no change
    containerLauncher.finishEventHandling = true;
    int timeOut = 0;
    while (containerLauncher.numEventsProcessed.get() < 10 && timeOut++ < 200) {
      LOG.info("Waiting for number of events processed to become " + 10
          + ". It is now " + containerLauncher.numEventsProcessed.get()
          + ". Timeout is " + timeOut);
      Thread.sleep(1000);
    }
    Assert.assertEquals(10, containerLauncher.numEventsProcessed.get());
    containerLauncher.finishEventHandling = false;
    for (int i = 0; i < 10; i++) {
      ContainerId containerId = ContainerId.newContainerId(appAttemptId,
          i + 10);
      TaskAttemptId taskAttemptId = MRBuilderUtils.newTaskAttemptId(taskId,
          i + 10);
      containerLauncher.handle(new ContainerLauncherEvent(taskAttemptId,
        containerId, "host" + i + ":1234", null,
        ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH));
    }
    waitForEvents(containerLauncher, 20);
    Assert.assertEquals(10, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    // Different hosts, there should be an increase in core-thread-pool size to
    // 21(11hosts+10buffer)
    // Core pool size should be 21 but the live pool size should be only 11.
    containerLauncher.expectedCorePoolSize = 11 + containerLauncher.initialPoolSize;
    containerLauncher.finishEventHandling = false;
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 21);
    TaskAttemptId taskAttemptId = MRBuilderUtils.newTaskAttemptId(taskId, 21);
    containerLauncher.handle(new ContainerLauncherEvent(taskAttemptId,
      containerId, "host11:1234", null,
      ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH));
    waitForEvents(containerLauncher, 21);
    Assert.assertEquals(11, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    containerLauncher.stop();

    // change configuration MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE
    // and verify initialPoolSize value.
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE,
        20);
    containerLauncher = new CustomContainerLauncher(context);
    containerLauncher.init(conf);
    Assert.assertEquals(containerLauncher.initialPoolSize, 20);
  }

  @Test(timeout = 5000)
  public void testPoolLimits() throws InterruptedException {
    ApplicationId appId = ApplicationId.newInstance(12345, 67);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
      appId, 3);
    JobId jobId = MRBuilderUtils.newJobId(appId, 8);
    TaskId taskId = MRBuilderUtils.newTaskId(jobId, 9, TaskType.MAP);
    TaskAttemptId taskAttemptId = MRBuilderUtils.newTaskAttemptId(taskId, 0);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 10);

    AppContext context = mock(AppContext.class);
    CustomContainerLauncher containerLauncher = new CustomContainerLauncher(
      context);
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT, 12);
    containerLauncher.init(conf);
    containerLauncher.start();

    ThreadPoolExecutor threadPool = containerLauncher.getThreadPool();

    // 10 different hosts
    containerLauncher.expectedCorePoolSize = containerLauncher.initialPoolSize;
    for (int i = 0; i < 10; i++) {
      containerLauncher.handle(new ContainerLauncherEvent(taskAttemptId,
        containerId, "host" + i + ":1234", null,
        ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH));
    }
    waitForEvents(containerLauncher, 10);
    Assert.assertEquals(10, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    // 4 more different hosts, but thread pool size should be capped at 12
    containerLauncher.expectedCorePoolSize = 12 ;
    for (int i = 1; i <= 4; i++) {
      containerLauncher.handle(new ContainerLauncherEvent(taskAttemptId,
        containerId, "host1" + i + ":1234", null,
        ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH));
    }
    waitForEvents(containerLauncher, 12);
    Assert.assertEquals(12, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    // Make some threads ideal so that remaining events are also done.
    containerLauncher.finishEventHandling = true;
    waitForEvents(containerLauncher, 14);
    Assert.assertEquals(12, threadPool.getPoolSize());
    Assert.assertNull(containerLauncher.foundErrors);

    containerLauncher.stop();
  }

  private void waitForEvents(CustomContainerLauncher containerLauncher,
      int expectedNumEvents) throws InterruptedException {
    int timeOut = 0;
    while (containerLauncher.numEventsProcessing.get() < expectedNumEvents
        && timeOut++ < 20) {
      LOG.info("Waiting for number of events to become " + expectedNumEvents
          + ". It is now " + containerLauncher.numEventsProcessing.get());
      Thread.sleep(1000);
    }
    Assert.assertEquals(expectedNumEvents,
      containerLauncher.numEventsProcessing.get());
  }

  @Test(timeout = 15000)
  public void testSlowNM() throws Exception {

    conf = new Configuration();
    int maxAttempts = 1;
    conf.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, maxAttempts);
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);
    // set timeout low for the test
    conf.setInt("yarn.rpc.nm-command-timeout", 3000);
    conf.set(YarnConfiguration.IPC_RPC_IMPL, HadoopYarnProtoRPC.class.getName());
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    NMTokenSecretManagerInNM tokenSecretManager =
        new NMTokenSecretManagerInNM();
    MasterKey masterKey = Records.newRecord(MasterKey.class);
    masterKey.setBytes(ByteBuffer.wrap("key".getBytes()));
    tokenSecretManager.setMasterKey(masterKey);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "token");
    server =
        rpc.getServer(ContainerManagementProtocol.class,
          new DummyContainerManager(), addr, conf, tokenSecretManager, 1);
    server.start();

    MRApp app = new MRAppWithSlowNM(tokenSecretManager);

    try {
    Job job = app.submit(conf);
    app.waitForState(job, JobState.RUNNING);

    Map<TaskId, Task> tasks = job.getTasks();
    Assert.assertEquals("Num tasks is not correct", 1, tasks.size());

    Task task = tasks.values().iterator().next();
    app.waitForState(task, TaskState.SCHEDULED);

    Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator()
        .next().getAttempts();
      Assert.assertEquals("Num attempts is not correct", maxAttempts,
          attempts.size());

    TaskAttempt attempt = attempts.values().iterator().next();
      app.waitForInternalState((TaskAttemptImpl) attempt,
          TaskAttemptStateInternal.ASSIGNED);

    app.waitForState(job, JobState.FAILED);

    String diagnostics = attempt.getDiagnostics().toString();
    LOG.info("attempt.getDiagnostics: " + diagnostics);

      Assert.assertTrue(diagnostics.contains("Container launch failed for "
          + "container_0_0000_01_000000 : "));
      Assert
          .assertTrue(diagnostics
              .contains("java.net.SocketTimeoutException: 3000 millis timeout while waiting for channel"));

    } finally {
      server.stop();
    app.stop();
  }
  }

  private final class CustomContainerLauncher extends ContainerLauncherImpl {

    private volatile int expectedCorePoolSize = 0;
    private AtomicInteger numEventsProcessing = new AtomicInteger(0);
    private AtomicInteger numEventsProcessed = new AtomicInteger(0);
    private volatile String foundErrors = null;
    private volatile boolean finishEventHandling;

    private CustomContainerLauncher(AppContext context) {
      super(context);
    }

    public ThreadPoolExecutor getThreadPool() {
      return super.launcherPool;
    }

    private final class CustomEventProcessor extends
        ContainerLauncherImpl.EventProcessor {
      private final ContainerLauncherEvent event;

      private CustomEventProcessor(ContainerLauncherEvent event) {
        super(event);
        this.event = event;
      }

      @Override
      public void run() {
        // do nothing substantial

        LOG.info("Processing the event " + event.toString());

        numEventsProcessing.incrementAndGet();
        // Stall
        while (!finishEventHandling) {
          synchronized (this) {
            try {
              wait(1000);
            } catch (InterruptedException e) {
              ;
            }
          }
        }
        numEventsProcessed.incrementAndGet();
      }
    }

    protected ContainerLauncherImpl.EventProcessor createEventProcessor(
        final ContainerLauncherEvent event) {
      // At this point of time, the EventProcessor is being created and so no
      // additional threads would have been created.

      // Core-pool-size should have increased by now.
      if (expectedCorePoolSize != launcherPool.getCorePoolSize()) {
        foundErrors = "Expected " + expectedCorePoolSize + " but found "
            + launcherPool.getCorePoolSize();
      }

      return new CustomEventProcessor(event);
    }
  }

  private class MRAppWithSlowNM extends MRApp {

    private NMTokenSecretManagerInNM tokenSecretManager;
    public MRAppWithSlowNM(NMTokenSecretManagerInNM tokenSecretManager) {
      super(1, 0, false, "TestContainerLauncher", true);
      this.tokenSecretManager = tokenSecretManager;
    }

    @Override
    protected ContainerLauncher
        createContainerLauncher(final AppContext context) {
      return new ContainerLauncherImpl(context) {

        @Override
        public ContainerManagementProtocolProxyData getCMProxy(
            String containerMgrBindAddr, ContainerId containerId)
            throws IOException {
          InetSocketAddress addr = NetUtils.getConnectAddress(server);
          String containerManagerBindAddr =
              addr.getHostName() + ":" + addr.getPort();
          Token token =
              tokenSecretManager.createNMToken(
                containerId.getApplicationAttemptId(),
                NodeId.newInstance(addr.getHostName(), addr.getPort()), "user");
          ContainerManagementProtocolProxy cmProxy =
              new ContainerManagementProtocolProxy(conf);
          ContainerManagementProtocolProxyData proxy =
              cmProxy.new ContainerManagementProtocolProxyData(
                YarnRPC.create(conf), containerManagerBindAddr, containerId,
                token);
          return proxy;
        }
      };

    };
  }

  public class DummyContainerManager implements ContainerManagementProtocol {

    private ContainerStatus status = null;

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws IOException {
      List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
      statuses.add(status);
      return GetContainerStatusesResponse.newInstance(statuses, null);
    }

    @Override
    public StartContainersResponse startContainers(StartContainersRequest requests)
        throws IOException {

      StartContainerRequest request = requests.getStartContainerRequests().get(0);
      ContainerTokenIdentifier containerTokenIdentifier =
          MRApp.newContainerTokenIdentifier(request.getContainerToken());

      // Validate that the container is what RM is giving.
      Assert.assertEquals(MRApp.NM_HOST + ":" + MRApp.NM_PORT,
        containerTokenIdentifier.getNmHostAddress());

      StartContainersResponse response = recordFactory
          .newRecordInstance(StartContainersResponse.class);
      status = recordFactory.newRecordInstance(ContainerStatus.class);
      try {
        // make the thread sleep to look like its not going to respond
        Thread.sleep(15000);
      } catch (Exception e) {
        LOG.error(e);
        throw new UndeclaredThrowableException(e);
      }
      status.setState(ContainerState.RUNNING);
      status.setContainerId(containerTokenIdentifier.getContainerID());
      status.setExitStatus(0);
      return response;
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
        throws IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new IOException(e);
    }

    @Override
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request) throws IOException,
        IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new IOException(e);
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      return null;
    }
  }
}
