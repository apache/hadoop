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

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple async event processing utility.
 * <p>
 * Event queue handles a collection of event handlers and routes the incoming
 * events to one (or more) event handler.
 */
public class EventQueue implements EventPublisher, AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(EventQueue.class);

  private static final String EXECUTOR_NAME_SEPARATOR = "For";

  private final Map<Event, Map<EventExecutor, List<EventHandler>>> executors =
      new HashMap<>();

  private final AtomicLong queuedCount = new AtomicLong(0);

  private final AtomicLong eventCount = new AtomicLong(0);

  private boolean isRunning = true;

  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventHandler<PAYLOAD> handler) {
    this.addHandler(event, handler, generateHandlerName(handler));
  }

  /**
   * Add new handler to the event queue.
   * <p>
   * By default a separated single thread executor will be dedicated to
   * deliver the events to the registered event handler.
   *
   * @param event        Triggering event.
   * @param handler      Handler of event (will be called from a separated
   *                     thread)
   * @param handlerName  The name of handler (should be unique together with
   *                     the event name)
   * @param <PAYLOAD>    The type of the event payload.
   * @param <EVENT_TYPE> The type of the event identifier.
   */
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventHandler<PAYLOAD> handler, String handlerName) {
    validateEvent(event);
    Preconditions.checkNotNull(handler, "Handler name should not be null.");
    String executorName =
        StringUtils.camelize(event.getName()) + EXECUTOR_NAME_SEPARATOR
            + handlerName;
    this.addHandler(event, new SingleThreadExecutor<>(executorName), handler);
  }

  private <EVENT_TYPE extends Event<?>> void validateEvent(EVENT_TYPE event) {
    Preconditions
        .checkArgument(!event.getName().contains(EXECUTOR_NAME_SEPARATOR),
            "Event name should not contain " + EXECUTOR_NAME_SEPARATOR
                + " string.");

  }

  private <PAYLOAD> String generateHandlerName(EventHandler<PAYLOAD> handler) {
    if (!"".equals(handler.getClass().getSimpleName())) {
      return handler.getClass().getSimpleName();
    } else {
      return handler.getClass().getName();
    }
  }

  /**
   * Add event handler with custom executor.
   *
   * @param event        Triggering event.
   * @param executor     The executor imlementation to deliver events from a
   *                     separated threads. Please keep in your mind that
   *                     registering metrics is the responsibility of the
   *                     caller.
   * @param handler      Handler of event (will be called from a separated
   *                     thread)
   * @param <PAYLOAD>    The type of the event payload.
   * @param <EVENT_TYPE> The type of the event identifier.
   */
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventExecutor<PAYLOAD> executor,
      EventHandler<PAYLOAD> handler) {
    if (!isRunning) {
      LOG.warn("Not adding handler for {}, EventQueue is not running", event);
      return;
    }
    validateEvent(event);
    executors.putIfAbsent(event, new HashMap<>());
    executors.get(event).putIfAbsent(executor, new ArrayList<>());

    executors.get(event).get(executor).add(handler);
  }



  /**
   * Route an event with payload to the right listener(s).
   *
   * @param event   The event identifier
   * @param payload The payload of the event.
   * @throws IllegalArgumentException If there is no EventHandler for
   *                                  the specific event.
   */
  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {

    if (!isRunning) {
      LOG.warn("Processing of {} is skipped, EventQueue is not running", event);
      return;
    }

    Map<EventExecutor, List<EventHandler>> eventExecutorListMap =
        this.executors.get(event);

    eventCount.incrementAndGet();
    if (eventExecutorListMap != null) {

      for (Map.Entry<EventExecutor, List<EventHandler>> executorAndHandlers :
          eventExecutorListMap.entrySet()) {

        for (EventHandler handler : executorAndHandlers.getValue()) {
          queuedCount.incrementAndGet();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Delivering event {} to executor/handler {}: {}",
                event.getName(),
                executorAndHandlers.getKey().getName(),
                payload);
          }
          executorAndHandlers.getKey()
              .onMessage(handler, payload, this);

        }
      }

    } else {
      LOG.warn("No event handler registered for event " + event);
    }

  }

  /**
   * This is just for unit testing, don't use it for production code.
   * <p>
   * It waits for all messages to be processed. If one event handler invokes an
   * other one, the later one also should be finished.
   * <p>
   * Long counter overflow is not handled, therefore it's safe only for unit
   * testing.
   * <p>
   * This method is just eventually consistent. In some cases it could return
   * even if there are new messages in some of the handler. But in a simple
   * case (one message) it will return only if the message is processed and
   * all the dependent messages (messages which are sent by current handlers)
   * are processed.
   *
   * @param timeout Timeout in seconds to wait for the processing.
   */
  @VisibleForTesting
  public void processAll(long timeout) {
    long currentTime = Time.now();
    while (true) {

      if (!isRunning) {
        LOG.warn("Processing of event skipped. EventQueue is not running");
        return;
      }

      long processed = 0;

      Stream<EventExecutor> allExecutor = this.executors.values().stream()
          .flatMap(handlerMap -> handlerMap.keySet().stream());

      boolean allIdle =
          allExecutor.allMatch(executor -> executor.queuedEvents() == executor
              .successfulEvents() + executor.failedEvents());

      if (allIdle) {
        return;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted exception while sleeping.", e);
        // We ignore this exception for time being. Review? should we
        // propogate it back to caller?
      }

      if (Time.now() > currentTime + timeout) {
        throw new AssertionError(
            "Messages are not processed in the given timeframe. Queued: "
                + queuedCount.get() + " Processed: " + processed);
      }
    }
  }
  @Override
  public void close() {

    isRunning = false;

    Set<EventExecutor> allExecutors = this.executors.values().stream()
        .flatMap(handlerMap -> handlerMap.keySet().stream())
        .collect(Collectors.toSet());

    allExecutors.forEach(executor -> {
      try {
        executor.close();
      } catch (Exception ex) {
        LOG.error("Can't close the executor " + executor.getName(), ex);
      }
    });
  }


}
