/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container;

import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler
    .CloseContainerStatus;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler
    .CloseContainerRetryableReq;
import org.apache.hadoop.hdds.scm.container.CloseContainerWatcher;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.EventWatcher;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link CloseContainerWatcher}.
 * */
public class TestCloseContainerWatcher implements EventHandler<ContainerID> {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCloseContainerWatcher.class);
  private static EventWatcher<CloseContainerRetryableReq, CloseContainerStatus>
      watcher;
  private static LeaseManager<Long> leaseManager;
  private static ContainerMapping containerMapping = Mockito
      .mock(ContainerMapping.class);
  private static EventQueue queue;
  @Rule
  public Timeout timeout = new Timeout(1000*15);

  @After
  public void stop() {
    leaseManager.shutdown();
    queue.close();
  }

  /*
   * This test will test watcher for Failure status event.
   * */
  @Test
  public void testWatcherForFailureStatusEvent() throws
      InterruptedException, IOException {
    setupWatcher(90000L);
    long id1 = HddsIdFactory.getLongId();
    long id2 = HddsIdFactory.getLongId();
    queue.addHandler(SCMEvents.CLOSE_CONTAINER, this);
    setupMock(id1, id2, true);
    GenericTestUtils.LogCapturer testLogger = GenericTestUtils.LogCapturer
        .captureLogs(LOG);
    GenericTestUtils.LogCapturer watcherLogger = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerWatcher.LOG);
    GenericTestUtils.setLogLevel(CloseContainerWatcher.LOG, Level.TRACE);
    testLogger.clearOutput();
    watcherLogger.clearOutput();

    CommandStatus cmdStatus1 = CommandStatus.newBuilder()
        .setCmdId(id1)
        .setStatus(CommandStatus.Status.FAILED)
        .setType(Type.closeContainerCommand).build();
    CommandStatus cmdStatus2 = CommandStatus.newBuilder()
        .setCmdId(id2)
        .setStatus(CommandStatus.Status.FAILED)
        .setType(Type.closeContainerCommand).build();

    // File events to watcher
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id1)));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id2)));
    Thread.sleep(10L);
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS, new
        CloseContainerStatus(cmdStatus1));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS, new
        CloseContainerStatus(cmdStatus2));

    Thread.sleep(1000*4L);
    // validation
    assertTrue(watcherLogger.getOutput().contains("CloseContainerCommand for " +
        "containerId: " + id1 + " executed"));
    assertTrue(watcherLogger.getOutput().contains("CloseContainerCommand for " +
        "containerId: " + id2 + " executed"));
    assertTrue(
        testLogger.getOutput().contains("Handling closeContainerEvent " +
            "for containerId: id=" + id1));
    assertTrue(testLogger.getOutput().contains("Handling closeContainerEvent " +
        "for containerId: id=" + id2));

  }

  @Test
  public void testWatcherForPendingStatusEvent() throws
      InterruptedException, IOException {
    setupWatcher(90000L);
    long id1 = HddsIdFactory.getLongId();
    long id2 = HddsIdFactory.getLongId();
    queue.addHandler(SCMEvents.CLOSE_CONTAINER, this);
    setupMock(id1, id2, true);
    GenericTestUtils.LogCapturer testLogger = GenericTestUtils.LogCapturer
        .captureLogs(LOG);
    GenericTestUtils.LogCapturer watcherLogger = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerWatcher.LOG);
    GenericTestUtils.setLogLevel(CloseContainerWatcher.LOG, Level.TRACE);
    testLogger.clearOutput();
    watcherLogger.clearOutput();

    CommandStatus cmdStatus1 = CommandStatus.newBuilder()
        .setCmdId(id1)
        .setStatus(CommandStatus.Status.PENDING)
        .setType(Type.closeContainerCommand).build();
    CommandStatus cmdStatus2 = CommandStatus.newBuilder()
        .setCmdId(id2)
        .setStatus(CommandStatus.Status.PENDING)
        .setType(Type.closeContainerCommand).build();

    // File events to watcher
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id1)));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id2)));
    Thread.sleep(10L);
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS, new
        CloseContainerStatus(cmdStatus1));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS, new
        CloseContainerStatus(cmdStatus2));

    Thread.sleep(1000*2L);
    // validation
    assertFalse(watcherLogger.getOutput().contains("CloseContainerCommand "
        + "for containerId: " + id1 + " executed"));
    assertFalse(watcherLogger.getOutput().contains("CloseContainerCommand "
        + "for containerId: " + id2 + " executed"));
    assertFalse(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id1));
    assertFalse(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id2));

  }

  @Test
  public void testWatcherForExecutedStatusEvent()
      throws IOException, InterruptedException {
    setupWatcher(90000L);
    long id1 = HddsIdFactory.getLongId();
    long id2 = HddsIdFactory.getLongId();
    queue.addHandler(SCMEvents.CLOSE_CONTAINER, this);
    setupMock(id1, id2, true);
    GenericTestUtils.LogCapturer testLogger = GenericTestUtils.LogCapturer
        .captureLogs(LOG);
    GenericTestUtils.LogCapturer watcherLogger = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerWatcher.LOG);
    GenericTestUtils.setLogLevel(CloseContainerWatcher.LOG, Level.TRACE);
    testLogger.clearOutput();
    watcherLogger.clearOutput();

    // When both of the pending event are executed successfully by DataNode
    CommandStatus cmdStatus1 = CommandStatus.newBuilder()
        .setCmdId(id1)
        .setStatus(CommandStatus.Status.EXECUTED)
        .setType(Type.closeContainerCommand).build();
    CommandStatus cmdStatus2 = CommandStatus.newBuilder()
        .setCmdId(id2)
        .setStatus(CommandStatus.Status.EXECUTED)
        .setType(Type.closeContainerCommand).build();
    // File events to watcher
    testLogger.clearOutput();
    watcherLogger.clearOutput();
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id1)));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id2)));
    Thread.sleep(10L);
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS,
        new CloseContainerStatus(cmdStatus1));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_STATUS,
        new CloseContainerStatus(cmdStatus2));

    Thread.sleep(1000*3L);
    // validation
    assertTrue(watcherLogger.getOutput().contains("CloseContainerCommand "
        + "for containerId: " + id1 + " executed"));
    assertTrue(watcherLogger.getOutput().contains("CloseContainerCommand "
        + "for containerId: " + id2 + " executed"));
    assertFalse(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id1));
    assertFalse(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id2));
  }

  private void setupWatcher(long time) {
    leaseManager = new LeaseManager<>("TestCloseContainerWatcher#LeaseManager",
        time);
    leaseManager.start();
    watcher = new CloseContainerWatcher(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        SCMEvents.CLOSE_CONTAINER_STATUS, leaseManager, containerMapping);
    queue = new EventQueue();
    watcher.start(queue);
  }

  /*
   * This test will fire two retryable closeContainer events. Both will timeout.
   * First event container will be open at time of handling so it should be
   * sent back to appropriate handler. Second event container will be closed,
   * so it should not be retried.
   * */
  @Test
  public void testWatcherRetryableTimeoutHandling() throws InterruptedException,
      IOException {

    long id1 = HddsIdFactory.getLongId();
    long id2 = HddsIdFactory.getLongId();
    setupWatcher(1000L);
    queue.addHandler(SCMEvents.CLOSE_CONTAINER, this);
    setupMock(id1, id2, false);
    GenericTestUtils.LogCapturer testLogger = GenericTestUtils.LogCapturer
        .captureLogs(LOG);
    testLogger.clearOutput();

    // File events to watcher
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id1)));
    queue.fireEvent(SCMEvents.CLOSE_CONTAINER_RETRYABLE_REQ,
        new CloseContainerRetryableReq(ContainerID.valueof(id2)));

    Thread.sleep(1000L + 10);

    // validation
    assertTrue(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id1));
    assertFalse(testLogger.getOutput().contains("Handling "
        + "closeContainerEvent for containerId: id=" + id2));
  }


  private void setupMock(long id1, long id2, boolean isOpen)
      throws IOException {
    ContainerInfo containerInfo = Mockito.mock(ContainerInfo.class);
    ContainerInfo containerInfo2 = Mockito.mock(ContainerInfo.class);
    when(containerMapping.getContainer(id1)).thenReturn(containerInfo);
    when(containerMapping.getContainer(id2)).thenReturn(containerInfo2);
    when(containerInfo.isContainerOpen()).thenReturn(true);
    when(containerInfo2.isContainerOpen()).thenReturn(isOpen);
  }

  @Override
  public void onMessage(ContainerID containerID, EventPublisher publisher) {
    LOG.info("Handling closeContainerEvent for containerId: {}", containerID);
  }
}
