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

package org.apache.hadoop.yarn.event;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.metrics.EventTypeMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This is a specialized EventHandler to be used by Services that are expected
 * handle a large number of events efficiently by ensuring that the caller
 * thread is not blocked. Events are immediately stored in a BlockingQueue and
 * a separate dedicated Thread consumes events from the queue and handles
 * appropriately
 * @param <T> Type of Event
 */
public class EventDispatcher<T extends Event> extends
    AbstractService implements EventHandler<T> {

  private final EventHandler<T> handler;
  private final BlockingQueue<T> eventQueue =
      new LinkedBlockingDeque<>();
  private final Thread eventProcessor;
  private volatile boolean stopped = false;
  private boolean shouldExitOnError = true;
  private EventTypeMetrics metrics;

  private static final Logger LOG =
      LoggerFactory.getLogger(EventDispatcher.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");

  private Clock clock = new MonotonicClock();

  private final class EventProcessor implements Runnable {
    @Override
    public void run() {

      T event;

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {
          LOG.error("Returning, interrupted : " + e);
          return; // TODO: Kill RM.
        }

        try {
          if (metrics != null) {
            long startTime = clock.getTime();
            handler.handle(event);
            metrics.increment(event.getType(),
                clock.getTime() - startTime);
          } else {
            handler.handle(event);
          }
        } catch (Throwable t) {
          // An error occurred, but we are shutting down anyway.
          // If it was an InterruptedException, the very act of
          // shutdown could have caused it and is probably harmless.
          if (stopped) {
            LOG.warn("Exception during shutdown: ", t);
            break;
          }
          LOG.error(FATAL, "Error in handling event type " + event.getType()
              + " to the Event Dispatcher", t);
          if (shouldExitOnError
              && !ShutdownHookManager.get().isShutdownInProgress()) {
            LOG.info("Exiting, bbye..");
            System.exit(-1);
          }
        }
      }
    }
  }

  public EventDispatcher(EventHandler<T> handler, String name) {
    super(name);
    this.handler = handler;
    this.eventProcessor = new Thread(new EventProcessor());
    this.eventProcessor.setName(getName() + ":Event Processor");
  }

  @Override
  protected void serviceStart() throws Exception {
    this.eventProcessor.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    this.stopped = true;
    this.eventProcessor.interrupt();
    try {
      this.eventProcessor.join();
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStop();
  }

  @Override
  public void handle(T event) {
    try {
      int qSize = eventQueue.size();
      if (qSize !=0 && qSize %1000 == 0) {
        LOG.info("Size of " + getName() + " event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.info("Very low remaining capacity on " + getName() + "" +
            "event queue: " + remCapacity);
      }
      this.eventQueue.put(event);
    } catch (InterruptedException e) {
      LOG.info("Interrupted. Trying to exit gracefully.");
    }
  }

  @VisibleForTesting
  public void disableExitOnError() {
    shouldExitOnError = false;
  }

  protected long getEventProcessorId() {
    return this.eventProcessor.getId();
  }

  protected boolean isStopped() {
    return this.stopped;
  }

  public void setMetrics(EventTypeMetrics metrics) {
    this.metrics = metrics;
  }

}
