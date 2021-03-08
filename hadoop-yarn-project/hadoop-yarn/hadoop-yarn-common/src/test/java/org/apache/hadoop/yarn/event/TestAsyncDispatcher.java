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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestAsyncDispatcher {

  /* This test checks whether dispatcher hangs on close if following two things
   * happen :
   * 1. A thread which was putting event to event queue is interrupted.
   * 2. Event queue is empty on close.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout=10000)
  public void testDispatcherOnCloseIfQueueEmpty() throws Exception {
    BlockingQueue<Event> eventQueue = spy(new LinkedBlockingQueue<Event>());
    Event event = mock(Event.class);
    doThrow(new InterruptedException()).when(eventQueue).put(event);
    DrainDispatcher disp = new DrainDispatcher(eventQueue);
    disp.init(new Configuration());
    disp.setDrainEventsOnStop();
    disp.start();
    // Wait for event handler thread to start and begin waiting for events.
    disp.waitForEventThreadToWait();
    try {
      disp.getEventHandler().handle(event);
      Assert.fail("Expected YarnRuntimeException");
    } catch (YarnRuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof InterruptedException);
    }
    // Queue should be empty and dispatcher should not hang on close
    Assert.assertTrue("Event Queue should have been empty",
        eventQueue.isEmpty());
    disp.close();
  }

  // Test dispatcher should timeout on draining events.
  @Test(timeout=10000)
  public void testDispatchStopOnTimeout() throws Exception {
    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>();
    eventQueue = spy(eventQueue);
    // simulate dispatcher is not drained.
    when(eventQueue.isEmpty()).thenReturn(false);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT, 2000);
    DrainDispatcher disp = new DrainDispatcher(eventQueue);
    disp.init(conf);
    disp.setDrainEventsOnStop();
    disp.start();
    disp.waitForEventThreadToWait();
    disp.close();
  }

  @SuppressWarnings("rawtypes")
  private static class DummyHandler implements EventHandler<Event> {
    @Override
    public void handle(Event event) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {}
    }
  }

  private enum DummyType {
    DUMMY
  }

  private static class TestHandler implements EventHandler<Event> {

    private long sleepTime = 1500;

    TestHandler() {
    }

    TestHandler(long sleepTime) {
      this.sleepTime = sleepTime;
    }

    @Override
    public void handle(Event event) {
      try {
        // As long as 10000 events queued
        Thread.sleep(this.sleepTime);
      } catch (InterruptedException e) {
      }
    }
  }

  private enum TestEnum {
    TestEventType
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void dispatchDummyEvents(Dispatcher disp, int count) {
    for (int i = 0; i < count; i++) {
      Event event = mock(Event.class);
      when(event.getType()).thenReturn(DummyType.DUMMY);
      disp.getEventHandler().handle(event);
    }
  }

  // Test if drain dispatcher drains events on stop.
  @SuppressWarnings({ "rawtypes" })
  @Test(timeout=10000)
  public void testDrainDispatcherDrainEventsOnStop() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT, 2000);
    BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
    DrainDispatcher disp = new DrainDispatcher(queue);
    disp.init(conf);
    disp.register(DummyType.class, new DummyHandler());
    disp.setDrainEventsOnStop();
    disp.start();
    disp.waitForEventThreadToWait();
    dispatchDummyEvents(disp, 2);
    disp.close();
    assertEquals(0, queue.size());
  }

  //Test print dispatcher details when the blocking queue is heavy
  @Test(timeout = 10000)
  public void testPrintDispatcherEventDetails() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.
            YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD, 5000);
    Log log = mock(Log.class);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);

    Field logger = AsyncDispatcher.class.getDeclaredField("LOG");
    logger.setAccessible(true);
    Field modifiers = Field.class.getDeclaredField("modifiers");
    modifiers.setAccessible(true);
    modifiers.setInt(logger, logger.getModifiers() & ~Modifier.FINAL);
    Object oldLog = logger.get(null);

    try {
      logger.set(null, log);
      dispatcher.register(TestEnum.class, new TestHandler());
      dispatcher.start();

      for (int i = 0; i < 10000; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType);
        dispatcher.getEventHandler().handle(event);
      }
      verify(log, atLeastOnce()).info("Event type: TestEventType, " +
              "Event record counter: 5000");
      Thread.sleep(2000);
      //Make sure more than one event to take
      verify(log, atLeastOnce()).
              info("Latest dispatch event type: TestEventType");
    } finally {
      //... restore logger object
      logger.set(null, oldLog);
      dispatcher.stop();
    }
  }

  //Test print dispatcher details when the blocking queue is heavy
  @Test(timeout = 60000)
  public void testPrintDispatcherEventDetailsAvoidDeadLoop() throws Exception {
    for (int i = 0; i < 5; i++) {
      testPrintDispatcherEventDetailsAvoidDeadLoopInternal();
    }
  }

  public void testPrintDispatcherEventDetailsAvoidDeadLoopInternal()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.
        YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD, 10);
    Log log = mock(Log.class);
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);

    Field logger = AsyncDispatcher.class.getDeclaredField("LOG");
    logger.setAccessible(true);
    Field modifiers = Field.class.getDeclaredField("modifiers");
    modifiers.setAccessible(true);
    modifiers.setInt(logger, logger.getModifiers() & ~Modifier.FINAL);
    Object oldLog = logger.get(null);

    try {
      logger.set(null, log);
      dispatcher.register(TestEnum.class, new TestHandler(0));
      dispatcher.start();

      for (int i = 0; i < 10000; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType);
        dispatcher.getEventHandler().handle(event);
      }
      Thread.sleep(3000);
    } finally {
      //... restore logger object
      logger.set(null, oldLog);
      dispatcher.stop();
    }
  }

}

