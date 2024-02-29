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

package org.apache.hadoop.yarn.event.multidispatcher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;

import static org.junit.Assert.assertEquals;

public class TestMultiDispatcher {

  @Test(timeout = 5_000)
  public void testHandle() {
    MultiDispatcher dispatcher = new MultiDispatcher("Test");
    assertEquals(Service.STATE.NOTINITED, dispatcher.getServiceState());
    dispatcher.init(new Configuration());
    assertEquals(Service.STATE.INITED, dispatcher.getServiceState());
    dispatcher.start();
    assertEquals(Service.STATE.STARTED, dispatcher.getServiceState());
    AtomicInteger handledEvent = new AtomicInteger();
    dispatcher.register(MockEventType.class, event -> handledEvent.incrementAndGet());

    IntStream.range(0, 1_000)
        .forEach(value -> dispatcher.getEventHandler().handle(new MockEvent()));

    dispatcher.stop();
    assertEquals(Service.STATE.STOPPED, dispatcher.getServiceState());
    assertEquals(1_000, handledEvent.get());
  }

  @Test(timeout = 5_000, expected = Error.class)
  public void testMissingHandler() {
    MultiDispatcher dispatcher = new MultiDispatcher("Test");
    assertEquals(Service.STATE.NOTINITED, dispatcher.getServiceState());
    dispatcher.init(new Configuration());
    assertEquals(Service.STATE.INITED, dispatcher.getServiceState());
    dispatcher.start();
    assertEquals(Service.STATE.STARTED, dispatcher.getServiceState());

    dispatcher.getEventHandler().handle(new MockEvent());
  }

  @Test(timeout = 5_000)
  public void testBlocksNewOnStop() {
    MultiDispatcher dispatcher = new MultiDispatcher("Test");
    assertEquals(Service.STATE.NOTINITED, dispatcher.getServiceState());
    dispatcher.init(new Configuration());
    assertEquals(Service.STATE.INITED, dispatcher.getServiceState());
    AtomicInteger handledEvent = new AtomicInteger();
    dispatcher.register(MockEventType.class, event -> handledEvent.incrementAndGet());
    dispatcher.start();
    assertEquals(Service.STATE.STARTED, dispatcher.getServiceState());
    dispatcher.stop();
    assertEquals(Service.STATE.STOPPED, dispatcher.getServiceState());
    dispatcher.getEventHandler().handle(new MockEvent());
    assertEquals(0, handledEvent.get());
  }
}
