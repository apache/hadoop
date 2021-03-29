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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;
import org.slf4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.metrics2.lib.Interns.info;
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
    TestEventType, TestEventType2
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
    Logger log = mock(Logger.class);
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
    Logger log = mock(Logger.class);
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

  @Test
  public void testMetricsForDispatcher() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    AsyncDispatcher dispatcher = null;

    try {
      dispatcher = new AsyncDispatcher("RM Event dispatcher");

      GenericEventTypeMetrics genericEventTypeMetrics =
          new GenericEventTypeMetrics.EventTypeMetricsBuilder()
              .setMs(DefaultMetricsSystem.instance())
              .setInfo(info("GenericEventTypeMetrics for "
                      + TestEnum.class.getName(),
                  "Metrics for " + dispatcher.getName()))
              .setEnumClass(TestEnum.class)
              .setEnums(TestEnum.class.getEnumConstants())
              .build().registerMetrics();

      // We can the metrics enabled for TestEnum
      dispatcher.addMetrics(genericEventTypeMetrics,
          genericEventTypeMetrics.getEnumClass());
      dispatcher.init(conf);

      // Register handler
      dispatcher.register(TestEnum.class, new TestHandler());
      dispatcher.start();

      for (int i = 0; i < 3; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType);
        dispatcher.getEventHandler().handle(event);
      }

      for (int i = 0; i < 2; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType2);
        dispatcher.getEventHandler().handle(event);
      }

      // Check event type count.
      GenericTestUtils.waitFor(() -> genericEventTypeMetrics.
          get(TestEnum.TestEventType) == 3, 1000, 10000);

      GenericTestUtils.waitFor(() -> genericEventTypeMetrics.
          get(TestEnum.TestEventType2) == 2, 1000, 10000);

      // Check time spend.
      Assert.assertTrue(genericEventTypeMetrics.
          getTotalProcessingTime(TestEnum.TestEventType)
          >= 1500*3);
      Assert.assertTrue(genericEventTypeMetrics.
          getTotalProcessingTime(TestEnum.TestEventType)
          < 1500*4);

      Assert.assertTrue(genericEventTypeMetrics.
          getTotalProcessingTime(TestEnum.TestEventType2)
          >= 1500*2);
      Assert.assertTrue(genericEventTypeMetrics.
          getTotalProcessingTime(TestEnum.TestEventType2)
          < 1500*3);

      // Make sure metrics consistent.
      Assert.assertEquals(Long.toString(genericEventTypeMetrics.
              get(TestEnum.TestEventType)),
          genericEventTypeMetrics.
              getRegistry().get("TestEventType_event_count").toString());
      Assert.assertEquals(Long.toString(genericEventTypeMetrics.
              get(TestEnum.TestEventType2)),
          genericEventTypeMetrics.
              getRegistry().get("TestEventType2_event_count").toString());
      Assert.assertEquals(Long.toString(genericEventTypeMetrics.
              getTotalProcessingTime(TestEnum.TestEventType)),
          genericEventTypeMetrics.
              getRegistry().get("TestEventType_processing_time").toString());
      Assert.assertEquals(Long.toString(genericEventTypeMetrics.
              getTotalProcessingTime(TestEnum.TestEventType2)),
          genericEventTypeMetrics.
              getRegistry().get("TestEventType2_processing_time").toString());

    } finally {
      dispatcher.close();
    }

  }

  @Test
  public void testDispatcherMetricsHistogram() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    AsyncDispatcher dispatcher = null;

    try {
      dispatcher = new AsyncDispatcher("RM Event dispatcher");

      GenericEventTypeMetrics genericEventTypeMetrics =
          new GenericEventTypeMetrics.EventTypeMetricsBuilder()
              .setMs(DefaultMetricsSystem.instance())
              .setInfo(info("GenericEventTypeMetrics for "
                      + TestEnum.class.getName(),
                  "Metrics for " + dispatcher.getName()))
              .setEnumClass(TestEnum.class)
              .setEnums(TestEnum.class.getEnumConstants())
              .build().registerMetrics();

      // We can the metrics enabled for TestEnum
      dispatcher.addMetrics(genericEventTypeMetrics,
          genericEventTypeMetrics.getEnumClass());
      dispatcher.init(conf);

      // Register handler
      dispatcher.register(TestEnum.class, new TestHandler());
      dispatcher.start();

      for (int i = 0; i < 3; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType);
        dispatcher.getEventHandler().handle(event);
      }

      for (int i = 0; i < 2; ++i) {
        Event event = mock(Event.class);
        when(event.getType()).thenReturn(TestEnum.TestEventType2);
        dispatcher.getEventHandler().handle(event);
      }

      // Check event type count.
      GenericTestUtils.waitFor(() -> genericEventTypeMetrics.
          get(TestEnum.TestEventType) == 3, 1000, 10000);

      GenericTestUtils.waitFor(() -> genericEventTypeMetrics.
          get(TestEnum.TestEventType2) == 2, 1000, 10000);

      // submit actual values
      Map<String, Long> expectedValues = new HashMap<>();
      expectedValues.put("TestEventType_event_count",
          genericEventTypeMetrics.get(TestEnum.TestEventType));
      expectedValues.put("TestEventType_processing_time",
          genericEventTypeMetrics.
              getTotalProcessingTime(TestEnum.TestEventType));
      expectedValues.put("TestEventType2_event_count",
          genericEventTypeMetrics.get(TestEnum.TestEventType2));
      expectedValues.put("TestEventType2_processing_time",
          genericEventTypeMetrics.
              getTotalProcessingTime(TestEnum.TestEventType2));
      Set<String> testResults = new HashSet<>();

      MetricsCollectorImpl collector = new MetricsCollectorImpl();
      genericEventTypeMetrics.getMetrics(collector, true);

      for (MetricsRecord record : collector.getRecords()) {
        for (AbstractMetric metric : record.metrics()) {
          String metricName = metric.name();
          if (expectedValues.containsKey(metricName)) {
            Long expectedValue = expectedValues.get(metricName);
            Assert.assertEquals(
                "Metric " + metricName + " doesn't have expected value",
                expectedValue, metric.value());
            testResults.add(metricName);
          }
        }
      }
      Assert.assertEquals(expectedValues.keySet(), testResults);

    } finally {
      dispatcher.close();
    }

  }
}