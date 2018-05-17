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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Testing the basic functionality of the event queue.
 */
public class TestEventQueue {

  private static final Event<Long> EVENT1 =
      new TypedEvent<>(Long.class, "SCM_EVENT1");
  private static final Event<Long> EVENT2 =
      new TypedEvent<>(Long.class, "SCM_EVENT2");

  private static final Event<Long> EVENT3 =
      new TypedEvent<>(Long.class, "SCM_EVENT3");
  private static final Event<Long> EVENT4 =
      new TypedEvent<>(Long.class, "SCM_EVENT4");

  private EventQueue queue;

  @Before
  public void startEventQueue() {
    queue = new EventQueue();
  }

  @After
  public void stopEventQueue() {
    queue.close();
  }

  @Test
  public void simpleEvent() {

    final long[] result = new long[2];

    queue.addHandler(EVENT1, (payload, publisher) -> result[0] = payload);

    queue.fireEvent(EVENT1, 11L);
    queue.processAll(1000);
    Assert.assertEquals(11, result[0]);

  }

  @Test
  public void multipleSubscriber() {
    final long[] result = new long[2];
    queue.addHandler(EVENT2, (payload, publisher) -> result[0] = payload);

    queue.addHandler(EVENT2, (payload, publisher) -> result[1] = payload);

    queue.fireEvent(EVENT2, 23L);
    queue.processAll(1000);
    Assert.assertEquals(23, result[0]);
    Assert.assertEquals(23, result[1]);

  }

  @Test
  public void handlerGroup() {
    final long[] result = new long[2];
    queue.addHandlerGroup(
        "group",
        new EventQueue.HandlerForEvent<>(EVENT3, (payload, publisher) ->
            result[0] = payload),
        new EventQueue.HandlerForEvent<>(EVENT4, (payload, publisher) ->
            result[1] = payload)
    );

    queue.fireEvent(EVENT3, 23L);
    queue.fireEvent(EVENT4, 42L);

    queue.processAll(1000);

    Assert.assertEquals(23, result[0]);
    Assert.assertEquals(42, result[1]);

    Set<String> eventQueueThreadNames =
        Thread.getAllStackTraces().keySet()
            .stream()
            .filter(t -> t.getName().startsWith(SingleThreadExecutor
                .THREAD_NAME_PREFIX))
            .map(Thread::getName)
            .collect(Collectors.toSet());
    System.out.println(eventQueueThreadNames);
    Assert.assertEquals(1, eventQueueThreadNames.size());

  }

}