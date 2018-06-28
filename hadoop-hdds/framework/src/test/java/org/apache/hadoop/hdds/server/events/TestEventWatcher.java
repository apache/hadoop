/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.events;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.ozone.lease.LeaseManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  LeaseManager<UUID> leaseManager;

  @Before
  public void startLeaseManager() {
    leaseManager = new LeaseManager<>(2000l);
    leaseManager.start();
  }

  @After
  public void stopLeaseManager() {
    leaseManager.shutdown();
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

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(uuid1, "C1"));

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(uuid2, "C2"));

    Assert.assertEquals(0, underReplicatedEvents.getReceivedEvents().size());

    Thread.sleep(1000);

    queue.fireEvent(REPLICATION_COMPLETED,
        new ReplicationCompletedEvent(uuid1, "C2", "D1"));

    Assert.assertEquals(0, underReplicatedEvents.getReceivedEvents().size());

    Thread.sleep(1500);

    queue.processAll(1000L);

    Assert.assertEquals(1, underReplicatedEvents.getReceivedEvents().size());
    Assert.assertEquals(uuid2,
        underReplicatedEvents.getReceivedEvents().get(0).UUID);

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
        new UnderreplicatedEvent(UUID.randomUUID(), "C1");

    queue.fireEvent(WATCH_UNDER_REPLICATED, event1);

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(UUID.randomUUID(), "C2"));

    queue.fireEvent(WATCH_UNDER_REPLICATED,
        new UnderreplicatedEvent(UUID.randomUUID(), "C1"));

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

  private EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>
  createEventWatcher() {
    return new EventWatcher<UnderreplicatedEvent, ReplicationCompletedEvent>(
        WATCH_UNDER_REPLICATED, REPLICATION_COMPLETED, leaseManager) {

      @Override
      void onTimeout(EventPublisher publisher, UnderreplicatedEvent payload) {
        publisher.fireEvent(UNDER_REPLICATED, payload);
      }

      @Override
      void onFinished(EventPublisher publisher, UnderreplicatedEvent payload) {
        //Good job. We did it.
      }
    };
  }

  private static class ReplicationCompletedEvent
      implements IdentifiableEventPayload {

    private final UUID UUID;

    private final String containerId;

    private final String datanodeId;

    public ReplicationCompletedEvent(UUID UUID, String containerId,
        String datanodeId) {
      this.UUID = UUID;
      this.containerId = containerId;
      this.datanodeId = datanodeId;
    }

    public UUID getUUID() {
      return UUID;
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

    private final UUID UUID;

    private final String containerId;

    public UnderreplicatedEvent(UUID UUID, String containerId) {
      this.containerId = containerId;
      this.UUID = UUID;
    }

    public UUID getUUID() {
      return UUID;
    }
  }

}