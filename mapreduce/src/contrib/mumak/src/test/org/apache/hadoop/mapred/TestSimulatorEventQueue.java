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
package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

public class TestSimulatorEventQueue {
  private Random random = new Random();

  public class TestEvent extends SimulatorEvent {
    
    public TestEvent(SimulatorEventListener listener, long timeStamp) {
      super(listener, timeStamp);
    }

  }
  
  public class TestEventWithCount extends TestEvent {
    private int count;
    
    public TestEventWithCount(SimulatorEventListener listener, long timeStamp, 
                              int count) {
      super(listener, timeStamp);
      this.count = count;
    }
    
    public int getCount() {
      return count;
    }
  }

  public static class TestListener implements SimulatorEventListener {

    @Override
    public List<SimulatorEvent> accept(SimulatorEvent event) {
      if (event instanceof TestEvent) {
        return SimulatorEventQueue.EMPTY_EVENTS;
      }
      return null;
    }

    @Override
    public List<SimulatorEvent> init(long when) {
      return null;
    }
  }
  
  @Test
  public void testSimpleGetPut() {
    SimulatorEventQueue queue = new SimulatorEventQueue();
    SimulatorEventListener listener = new TestListener();
    SimulatorEvent event = new TestEvent(listener, 10);
    
    queue.add(event);
    SimulatorEvent first = queue.get();
    
    Assert.assertEquals(first.getTimeStamp(), event.getTimeStamp());
    Assert.assertEquals(first.getListener(), event.getListener());
  }

  @Test
  public void testListPut() {
    SimulatorEventQueue queue = new SimulatorEventQueue();
    SimulatorEventListener listener = new TestListener();
    List<SimulatorEvent> listEvent = new ArrayList<SimulatorEvent>();
    
    listEvent.add(new TestEvent(listener, 10));
    listEvent.add(new TestEvent(listener, 11));
    
    queue.addAll(listEvent);
    SimulatorEvent first = queue.get();    
    Assert.assertEquals(first.getTimeStamp(), 10);
    Assert.assertEquals(first.getListener(), listener);
    
    SimulatorEvent second = queue.get();
    Assert.assertEquals(second.getTimeStamp(), 11);
    Assert.assertEquals(first.getListener(), listener);
  }

  @Test  
  public void testKeepOrder() {
    SimulatorEventQueue queue = new SimulatorEventQueue();
    SimulatorEventListener listener = new TestListener();
    List<SimulatorEvent> listEvent = new ArrayList<SimulatorEvent>();
    int count = 0;
    
    for (int i = 0; i < random.nextInt(100); i++) {
      listEvent.clear();
      for (int j = 0; j < random.nextInt(5); j++) {
        listEvent.add(new TestEventWithCount(listener, random.nextInt(10), count++));
      }
      queue.addAll(listEvent);
    }
    
    TestEventWithCount next;
    //dump(next);
    TestEventWithCount last = null;
    while((next = (TestEventWithCount) queue.get()) != null) {
      if (last != null && last.getTimeStamp() == next.getTimeStamp()) {
        Assert.assertTrue (last.getCount() < next.getCount());
        //dump(next);
      }
      last = next;
    }
  }
  
  public void dump(TestEventWithCount event) {
    System.out.println("timestamp: " + event.getTimeStamp()
        + ", count: " + event.getCount());
  }
  
  @Test
  public void testInsertEventIntoPast() {
    SimulatorEventQueue queue = new SimulatorEventQueue();
    SimulatorEventListener listener = new TestListener();
    
    queue.add(new TestEvent(listener, 10));
    queue.get();
    // current time is 10.
    try {
      // the event to insert happened at 5. It happens in the past because
      // current time is 10.
      queue.add(new TestEvent(listener, 5));
      Assert.fail("Added Event occurred in the past");
    } catch (Exception e) {
    }
  }
}
