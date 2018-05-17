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
import org.apache.hadoop.util.Time;
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

  private final Map<Event, Map<EventExecutor, List<EventHandler>>> executors =
      new HashMap<>();

  private final AtomicLong queuedCount = new AtomicLong(0);

  private final AtomicLong eventCount = new AtomicLong(0);

  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event, EventHandler<PAYLOAD> handler) {

    this.addHandler(event, new SingleThreadExecutor<>(
        event.getName()), handler);
  }

  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void addHandler(
      EVENT_TYPE event,
      EventExecutor<PAYLOAD> executor,
      EventHandler<PAYLOAD> handler) {

    executors.putIfAbsent(event, new HashMap<>());
    executors.get(event).putIfAbsent(executor, new ArrayList<>());

    executors.get(event)
        .get(executor)
        .add(handler);
  }

  /**
   * Creates one executor with multiple event handlers.
   */
  public void addHandlerGroup(String name, HandlerForEvent<?>...
      eventsAndHandlers) {
    SingleThreadExecutor sharedExecutor =
        new SingleThreadExecutor(name);
    for (HandlerForEvent handlerForEvent : eventsAndHandlers) {
      addHandler(handlerForEvent.event, sharedExecutor,
          handlerForEvent.handler);
    }

  }

  /**
   * Route an event with payload to the right listener(s).
   *
   * @param event   The event identifier
   * @param payload The payload of the event.
   * @throws IllegalArgumentException If there is no EventHandler for
   *                                  the specific event.
   */
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {

    Map<EventExecutor, List<EventHandler>> eventExecutorListMap =
        this.executors.get(event);

    eventCount.incrementAndGet();
    if (eventExecutorListMap != null) {

      for (Map.Entry<EventExecutor, List<EventHandler>> executorAndHandlers :
          eventExecutorListMap.entrySet()) {

        for (EventHandler handler : executorAndHandlers.getValue()) {
          queuedCount.incrementAndGet();

          executorAndHandlers.getKey()
              .onMessage(handler, payload, this);

        }
      }

    } else {
      throw new IllegalArgumentException(
          "No event handler registered for event " + event);
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
        e.printStackTrace();
      }

      if (Time.now() > currentTime + timeout) {
        throw new AssertionError(
            "Messages are not processed in the given timeframe. Queued: "
                + queuedCount.get() + " Processed: " + processed);
      }
    }
  }

  public void close() {

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

  /**
   * Event identifier together with the handler.
   *
   * @param <PAYLOAD>
   */
  public static class HandlerForEvent<PAYLOAD> {

    private final Event<PAYLOAD> event;

    private final EventHandler<PAYLOAD> handler;

    public HandlerForEvent(
        Event<PAYLOAD> event,
        EventHandler<PAYLOAD> handler) {
      this.event = event;
      this.handler = handler;
    }

    public Event<PAYLOAD> getEvent() {
      return event;
    }

    public EventHandler<PAYLOAD> getHandler() {
      return handler;
    }
  }

}
