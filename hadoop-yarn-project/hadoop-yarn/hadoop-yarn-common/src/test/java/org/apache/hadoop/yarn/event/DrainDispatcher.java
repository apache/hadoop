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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("rawtypes")
public class DrainDispatcher extends AsyncDispatcher {
  private volatile boolean drained = false;
  private final BlockingQueue<Event> queue;
  private final Object mutex;

  public DrainDispatcher() {
    this(new LinkedBlockingQueue<Event>());
  }

  public DrainDispatcher(BlockingQueue<Event> eventQueue) {
    super(eventQueue);
    this.queue = eventQueue;
    this.mutex = this;
    // Disable system exit since this class is only for unit tests.
    disableExitOnDispatchException();
  }

  /**
   *  Wait till event thread enters WAITING state (i.e. waiting for new events).
   */
  public void waitForEventThreadToWait() {
    while (!isEventThreadWaiting()) {
      Thread.yield();
    }
  }

  /**
   * Busy loop waiting for all queued events to drain.
   */
  public void await() {
    while (!isDrained()) {
      Thread.yield();
    }
  }

  @Override
  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!isStopped() && !Thread.currentThread().isInterrupted()) {
          synchronized (mutex) {
            // !drained if dispatch queued new events on this dispatcher
            drained = queue.isEmpty();
          }
          Event event;
          try {
            event = queue.take();
          } catch (InterruptedException ie) {
            return;
          }
          if (event != null) {
            dispatch(event);
          }
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public EventHandler<Event> getEventHandler() {
    final EventHandler<Event> actual = super.getEventHandler();
    return new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        synchronized (mutex) {
          actual.handle(event);
          drained = false;
        }
      }
    };
  }

  @Override
  protected boolean isDrained() {
    synchronized (mutex) {
      return drained;
    }
  }
}
