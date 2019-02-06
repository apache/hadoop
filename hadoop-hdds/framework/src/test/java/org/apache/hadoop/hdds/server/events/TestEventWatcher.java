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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.server.events;

import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

/**
 * Test the basic functionality of event watcher.
 */
public class TestEventWatcher {

  private static final TypedEvent<UnderreplicatedEvent> WATCH_UNDER_REPLICATED =
      new TypedEvent<>(UnderreplicatedEvent.class);

  private static final TypedEvent<UnderreplicatedEvent> UNDER_REPLICATED =
      new TypedEvent<>(UnderreplicatedEvent.class);

  private static final TypedEvent<ReplicationCompletedEvent>
      REPLICATION_COMPLETED = new TypedEvent<>(ReplicationCompletedEvent.class);

  private LeaseManager<Long> leaseManager;

  @Before
  public void startLeaseManager() {
    DefaultMetricsSystem.instance();
    leaseManager = new LeaseManager<>("Test", 2000L);
    leaseManager.start();
  }

  @After
  public void stopLeaseManager() {
    leaseManager.shutdown();
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testEventHandling() throws InterruptedException {
    EventQueue queue = new EventQueue();

    EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>
        replicationWatcher = createEventWatcher();

    EventHandlerStub<UnderreplicatedEvent> underReplicatedEvents =
        new EventHandlerStub<>();

    queue.addHandler(UNDER_REPLICATED, underReplicatedEvents);

    replicationWatcher.start(queue);

    long id1 = HddsIdFactory.getLongId();
    long id2 = HddsIdFactory.getLongId();

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(id1, "C1"));

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(id2, "C2"));

    Assert.assertEquals(0, underReplicatedEvents.getReceivedEvents().size());

    Thread.sleep(1000);

    queue.fireEvent(REPLICATION_COMPLETED,
        new ReplicationCompletedEvent(id1, "C2", "D1"));

    Assert.assertEquals(0, underReplicatedEvents.getReceivedEvents().size());

    Thread.sleep(1500);

    queue.processAll(1000L);

    Assert.assertEquals(1, underReplicatedEvents.getReceivedEvents().size());
    Assert.assertEquals(id2,
        underReplicatedEvents.getReceivedEvents().get(0).id);

  }

  @Test
  public void testInprogressFilter() throws InterruptedException {

    EventQueue queue = new EventQueue();

    EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>
        replicationWatcher = createEventWatcher();

    EventHandlerStub<UnderreplicatedEvent> underReplicatedEvents =
        new EventHandlerStub<>();

    queue.addHandler(UNDER_REPLICATED, underReplicatedEvents);

    replicationWatcher.start(queue);

    UnderreplicatedEvent event1 =
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C1");

    queue.fireEvent(WATCH_UNDER_REPLICATED, event1);

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C2"));

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C1"));

    queue.processAll(1000L);
    Thread.sleep(1000L);
    List<UnderreplicatedEvent> c1todo = replicationWatcher
        .getTimeoutEvents(e -> e.containerId.equalsIgnoreCase("C1"));

    Assert.assertEquals(2, c1todo.size());
    Assert.assertTrue(replicationWatcher.contains(event1));
    Thread.sleep(1500L);

    c1todo = replicationWatcher
        .getTimeoutEvents(e -> e.containerId.equalsIgnoreCase("C1"));
    Assert.assertEquals(0, c1todo.size());
    Assert.assertFalse(replicationWatcher.contains(event1));

  }

  @Test
  public void testMetrics() throws InterruptedException {

    DefaultMetricsSystem.initialize("test");

    EventQueue queue = new EventQueue();

    EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>
        replicationWatcher = createEventWatcher();

    EventHandlerStub<UnderreplicatedEvent> underReplicatedEvents =
        new EventHandlerStub<>();

    queue.addHandler(UNDER_REPLICATED, underReplicatedEvents);

    replicationWatcher.start(queue);

    //send 3 event to track 3 in-progress activity
    UnderreplicatedEvent event1 =
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C1");

    UnderreplicatedEvent event2 =
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C2");

    UnderreplicatedEvent event3 =
        new UnderreplicatedEvent(HddsIdFactory.getLongId(), "C1");

    queue.fireEvent(WATCH_UNDER_REPLICATED, event1);

    queue.fireEvent(WATCH_UNDER_REPLICATED, event2);

    queue.fireEvent(WATCH_UNDER_REPLICATED, event3);

    //1st event is completed, don't need to track any more
    ReplicationCompletedEvent event1Completed =
        new ReplicationCompletedEvent(event1.id, "C1", "D1");

    queue.fireEvent(REPLICATION_COMPLETED, event1Completed);

    Thread.sleep(2200L);

    //until now: 3 in-progress activities are tracked with three
    // UnderreplicatedEvents. The first one is completed, the remaining two
    // are timed out (as the timeout -- defined in the leasmanager -- is 2000ms.

    EventWatcherMetrics metrics = replicationWatcher.getMetrics();

    //3 events are received
    Assert.assertEquals(3, metrics.getTrackedEvents().value());

    //one is finished. doesn't need to be resent
    Assert.assertEquals(1, metrics.getCompletedEvents().value());

    //Other two are timed out and resent
    Assert.assertEquals(2, metrics.getTimedOutEvents().value());

    DefaultMetricsSystem.shutdown();
  }

  private EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>
      createEventWatcher() {
    return new CommandWatcherExample(WATCH_UNDER_REPLICATED,
        REPLICATION_COMPLETED, leaseManager);
  }

  private static class CommandWatcherExample
      extends EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent> {

    CommandWatcherExample(Event<UnderreplicatedEvent> startEvent,
        Event<ReplicationCompletedEvent> completionEvent,
        LeaseManager<Long> leaseManager) {
      super("TestCommandWatcher", startEvent, completionEvent, leaseManager);
    }

    @Override
    protected void onTimeout(EventPublisher publisher,
        UnderreplicatedEvent payload) {
      publisher.fireEvent(UNDER_REPLICATED, payload);
    }

    @Override
    protected void onFinished(EventPublisher publisher,
        UnderreplicatedEvent payload) {
      //Good job. We did it.
    }

    @Override
    public EventWatcherMetrics getMetrics() {
      return super.getMetrics();
    }
  }

  private static class ReplicationCompletedEvent
      implements IdentifiableEventPayload {

    private final long id;

    private final String containerId;

    private final String datanodeId;

    ReplicationCompletedEvent(long id, String containerId,
        String datanodeId) {
      this.id = id;
      this.containerId = containerId;
      this.datanodeId = datanodeId;
    }

    @Override
    public long getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReplicationCompletedEvent that = (ReplicationCompletedEvent) o;
      return Objects.equals(containerId, that.containerId) && Objects
          .equals(datanodeId, that.datanodeId);
    }

    @Override
    public int hashCode() {

      return Objects.hash(containerId, datanodeId);
    }
  }

  private static class UnderreplicatedEvent

      implements IdentifiableEventPayload {

    private final long id;

    private final String containerId;

    UnderreplicatedEvent(long id, String containerId) {
      this.containerId = containerId;
      this.id = id;
    }

    @Override
    public long getId() {
      return id;
    }
  }

}
