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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.lease.Lease;
import org.apache.hadoop.ozone.lease.LeaseAlreadyExistException;
import org.apache.hadoop.ozone.lease.LeaseExpiredException;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.lease.LeaseNotFoundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.map.HashedMap;
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

  private final LeaseManager<Long> leaseManager;

  private final EventWatcherMetrics metrics;

  private final String name;

  private final Map<Long, TIMEOUT_PAYLOAD> trackedEventsByID =
      new ConcurrentHashMap<>();

  private final Set<TIMEOUT_PAYLOAD> trackedEvents = new HashSet<>();

  private final Map<Long, Long> startTrackingTimes = new HashedMap();

  public EventWatcher(String name, Event<TIMEOUT_PAYLOAD> startEvent,
      Event<COMPLETION_PAYLOAD> completionEvent,
      LeaseManager<Long> leaseManager) {
    this.startEvent = startEvent;
    this.completionEvent = completionEvent;
    this.leaseManager = leaseManager;
    this.metrics = new EventWatcherMetrics();
    Preconditions.checkNotNull(name);
    if (name.equals("")) {
      name = getClass().getSimpleName();
    }
    if (name.equals("")) {
      //for anonymous inner classes
      name = getClass().getName();
    }
    this.name = name;
  }

  public EventWatcher(Event<TIMEOUT_PAYLOAD> startEvent,
      Event<COMPLETION_PAYLOAD> completionEvent,
      LeaseManager<Long> leaseManager) {
    this("", startEvent, completionEvent, leaseManager);
  }

  public void start(EventQueue queue) {

    queue.addHandler(startEvent, this::handleStartEvent);

    queue.addHandler(completionEvent, (completionPayload, publisher) -> {
      try {
        handleCompletion(completionPayload, publisher);
      } catch (LeaseNotFoundException e) {
        //It's already done. Too late, we already retried it.
        //Not a real problem.
        LOG.warn("Completion event without active lease. Id={}",
            completionPayload.getId());
      }
    });

    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(name, "EventWatcher metrics", metrics);
  }

  private synchronized void handleStartEvent(TIMEOUT_PAYLOAD payload,
      EventPublisher publisher) {
    metrics.incrementTrackedEvents();
    long identifier = payload.getId();
    startTrackingTimes.put(identifier, System.currentTimeMillis());

    trackedEventsByID.put(identifier, payload);
    trackedEvents.add(payload);
    try {
      Lease<Long> lease = leaseManager.acquire(identifier);
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

  protected synchronized void handleCompletion(COMPLETION_PAYLOAD
      completionPayload, EventPublisher publisher) throws
      LeaseNotFoundException {
    metrics.incrementCompletedEvents();
    long id = completionPayload.getId();
    leaseManager.release(id);
    TIMEOUT_PAYLOAD payload = trackedEventsByID.remove(id);
    trackedEvents.remove(payload);
    long originalTime = startTrackingTimes.remove(id);
    metrics.updateFinishingTime(System.currentTimeMillis() - originalTime);
    onFinished(publisher, payload);
  }

  private synchronized void handleTimeout(EventPublisher publisher,
      long identifier) {
    metrics.incrementTimedOutEvents();
    TIMEOUT_PAYLOAD payload = trackedEventsByID.remove(identifier);
    trackedEvents.remove(payload);
    startTrackingTimes.remove(payload.getId());
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
      leaseManager.release(payload.getId());
    } catch (LeaseNotFoundException e) {
      LOG.warn("Completion event without active lease. Id={}",
          payload.getId());
    }
    trackedEventsByID.remove(payload.getId());
    return trackedEvents.remove(payload);

  }

  protected abstract void onTimeout(
      EventPublisher publisher, TIMEOUT_PAYLOAD payload);

  protected abstract void onFinished(
      EventPublisher publisher, TIMEOUT_PAYLOAD payload);

  public List<TIMEOUT_PAYLOAD> getTimeoutEvents(
      Predicate<? super TIMEOUT_PAYLOAD> predicate) {
    return trackedEventsByID.values().stream().filter(predicate)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  protected EventWatcherMetrics getMetrics() {
    return metrics;
  }

  /**
   * Returns a tracked event to which the specified id is
   * mapped, or {@code null} if there is no mapping for the id.
   */
  public TIMEOUT_PAYLOAD getTrackedEventbyId(long id) {
    return trackedEventsByID.get(id);
  }

  public Map<Long, TIMEOUT_PAYLOAD> getTrackedEventsByID() {
    return trackedEventsByID;
  }

  public Set<TIMEOUT_PAYLOAD> getTrackedEvents() {
    return trackedEvents;
  }
}
