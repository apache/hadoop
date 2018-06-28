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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseAlreadyExistException;
import org.apache.hadoop.ozone.lease.LeaseExpiredException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.lease.LeaseNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event watcher the (re)send a message after timeout.
 * <p>
 * Event watcher will send the tracked payload/event after a timeout period
 * unless a confirmation from the original event (completion event) is arrived.
 *
 * @param <TIMEOUT_PAYLOAD>    The type of the events which are tracked.
 * @param <COMPLETION_PAYLOAD> The type of event which could cancel the
 *                             tracking.
 */
@SuppressWarnings("CheckStyle")
public abstract class EventWatcher<TIMEOUT_PAYLOAD extends
    IdentifiableEventPayload,
    COMPLETION_PAYLOAD extends IdentifiableEventPayload> {

  private static final Logger LOG = LoggerFactory.getLogger(EventWatcher.class);

  private final Event<TIMEOUT_PAYLOAD> startEvent;

  private final Event<COMPLETION_PAYLOAD> completionEvent;

  private final LeaseManager<UUID> leaseManager;

  protected final Map<UUID, TIMEOUT_PAYLOAD> trackedEventsByUUID =
      new ConcurrentHashMap<>();

  protected final Set<TIMEOUT_PAYLOAD> trackedEvents = new HashSet<>();

  public EventWatcher(Event<TIMEOUT_PAYLOAD> startEvent,
      Event<COMPLETION_PAYLOAD> completionEvent,
      LeaseManager<UUID> leaseManager) {
    this.startEvent = startEvent;
    this.completionEvent = completionEvent;
    this.leaseManager = leaseManager;

  }

  public void start(EventQueue queue) {

    queue.addHandler(startEvent, this::handleStartEvent);

    queue.addHandler(completionEvent, (completionPayload, publisher) -> {
      UUID uuid = completionPayload.getUUID();
      try {
        handleCompletion(uuid, publisher);
      } catch (LeaseNotFoundException e) {
        //It's already done. Too late, we already retried it.
        //Not a real problem.
        LOG.warn("Completion event without active lease. UUID={}", uuid);
      }
    });

  }

  private synchronized void handleStartEvent(TIMEOUT_PAYLOAD payload,
      EventPublisher publisher) {
    UUID identifier = payload.getUUID();
    trackedEventsByUUID.put(identifier, payload);
    trackedEvents.add(payload);
    try {
      Lease<UUID> lease = leaseManager.acquire(identifier);
      try {
        lease.registerCallBack(() -> {
          handleTimeout(publisher, identifier);
          return null;
        });

      } catch (LeaseExpiredException e) {
        handleTimeout(publisher, identifier);
      }
    } catch (LeaseAlreadyExistException e) {
      //No problem at all. But timer is not reset.
    }
  }

  private synchronized void handleCompletion(UUID uuid,
      EventPublisher publisher) throws LeaseNotFoundException {
    leaseManager.release(uuid);
    TIMEOUT_PAYLOAD payload = trackedEventsByUUID.remove(uuid);
    trackedEvents.remove(payload);
    onFinished(publisher, payload);
  }

  private synchronized void handleTimeout(EventPublisher publisher,
      UUID identifier) {
    TIMEOUT_PAYLOAD payload = trackedEventsByUUID.remove(identifier);
    trackedEvents.remove(payload);
    onTimeout(publisher, payload);
  }


  /**
   * Check if a specific payload is in-progress.
   */
  public synchronized boolean contains(TIMEOUT_PAYLOAD payload) {
    return trackedEvents.contains(payload);
  }

  public synchronized boolean remove(TIMEOUT_PAYLOAD payload) {
    try {
      leaseManager.release(payload.getUUID());
    } catch (LeaseNotFoundException e) {
      LOG.warn("Completion event without active lease. UUID={}",
          payload.getUUID());
    }
    trackedEventsByUUID.remove(payload.getUUID());
    return trackedEvents.remove(payload);

  }

  abstract void onTimeout(EventPublisher publisher, TIMEOUT_PAYLOAD payload);

  abstract void onFinished(EventPublisher publisher, TIMEOUT_PAYLOAD payload);

  public List<TIMEOUT_PAYLOAD> getTimeoutEvents(
      Predicate<? super TIMEOUT_PAYLOAD> predicate) {
    return trackedEventsByUUID.values().stream().filter(predicate)
        .collect(Collectors.toList());
  }
}
