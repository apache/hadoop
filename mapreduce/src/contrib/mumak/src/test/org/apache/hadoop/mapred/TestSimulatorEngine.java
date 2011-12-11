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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import junit.framework.Assert;

public class TestSimulatorEngine {
  private static final int TIME_WARP = 1000;
  Random random = new Random();
  
  public static class TestSimpleEvent extends SimulatorEvent {
    public TestSimpleEvent(SimulatorEventListener listener, long timestamp) {
      super(listener, timestamp);
    }
  }
  
  /**
   * Handling each {@link TestComplexEvent1} of level n will produce another
   * {@link TestComplexEvent1} of level n-1 and 4 {@link TestSimpleEvent}s when
   * n>0, produce no event if n=0. All events are created with a random time
   * stamp within 1000 units into the future.
   */
  public static class TestComplexEvent1 extends SimulatorEvent {
    private int level;
    
    public TestComplexEvent1(SimulatorEventListener listener, long timeStamp, 
                             int level) {
      super(listener, timeStamp);
      this.level = level;
    }
    
    public int getLevel() {
      return level;
    }
  }
  
  /**
   * Handling each {@link TestComplexEvent2} of level n will produce 2
   * {@link TestComplexEvent2}s of level n-1 and 1 {@link TestSimpleEvent} when
   * n>0, produce no event if n=0. All events are created with a random time
   * stamp within 1000 units into the future.
   */
  public class TestComplexEvent2 extends TestComplexEvent1 {
    public TestComplexEvent2(SimulatorEventListener listener, long timeStamp, 
                             int level) {
      super(listener, timeStamp, level);
    }
  }
  
  class TestListener implements SimulatorEventListener {

    @Override
    public List<SimulatorEvent> accept(SimulatorEvent event) {
      SimulatorEventListener listener = event.getListener();
      long now = event.getTimeStamp();
      if (event instanceof TestComplexEvent2) {
        // ce2(n) -> 2*ce2(n-1) + se
        int level = ((TestComplexEvent2) event).getLevel();
        if (level == 0)
          return SimulatorEventQueue.EMPTY_EVENTS;
        List<SimulatorEvent> response = new ArrayList<SimulatorEvent>();
        for (int i = 0; i < 2; i++)
          response.add(new TestComplexEvent2(listener,
                                             now + random.nextInt(TIME_WARP),
                                             level-1));
        response.add(new TestSimpleEvent(listener, now + random.nextInt(TIME_WARP)));
        return response;
      } else if (event instanceof TestComplexEvent1) {
        TestComplexEvent1 e = (TestComplexEvent1)event;
        // ce1(n) -> ce1(n-1) + 4*se
        if (e.getLevel() == 0)
          return SimulatorEventQueue.EMPTY_EVENTS;
        List<SimulatorEvent> response = new ArrayList<SimulatorEvent>();
        response.add(new TestComplexEvent1(listener,
                                           now + random.nextInt(TIME_WARP),
                                           e.getLevel()-1));
        for (int i = 0; i < 4; i++)
          response.add(new TestSimpleEvent(listener, 
                                           now + random.nextInt(TIME_WARP)));
        return response;
      } else if (event instanceof TestSimpleEvent) {
        return SimulatorEventQueue.EMPTY_EVENTS;
      } else {
        throw new IllegalArgumentException("unknown event type: "
            + event.getClass());
      }
    }

    @Override
    public List<SimulatorEvent> init(long when) {
      return null;
    }
  }

  public class TestSimulator1 extends SimulatorEngine {
    
    private int level = 10;
    
    @Override
    protected void init() {
      this.queue.add(new TestComplexEvent1(new TestListener(), 
          random.nextInt(TIME_WARP), level));
    }
    
    @Override
    protected void summary(PrintStream out) {
      out.println(queue.getCurrentTime() + ", " + queue.getEventCount() + 
                         ", " + queue.getSize());
      Assert.assertEquals(5*level+1, queue.getEventCount());
    }
  }
  
  public class TestSimulator2 extends SimulatorEngine {
    
    private int level = 10;
    
    @Override
    protected void init() {
      this.queue.add(new TestComplexEvent2(new TestListener(), 
          random.nextInt(TIME_WARP), level));
    }
    
    @Override
    protected void summary(PrintStream out) {
      out.println(queue.getCurrentTime() + ", " + queue.getEventCount() + 
                         ", " + queue.getSize());
      Assert.assertEquals(3*(1<<level)-2, queue.getEventCount());
    }
  }
  
  /**
   * Test {@link SimulatorEngine} using {@link TestSimulator1}. Insert a 
   * {@link TestComplexEvent1} in the beginning. The simulation stops when the
   * {@link SimulatorEventQueue} is empty. Total number of events processed is checked
   * against expected number (5*level+1).
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testComplex1() throws IOException, InterruptedException {
    SimulatorEngine simulation = new TestSimulator1();
    simulation.run();
  }
  
  /**
   * Test {@link SimulatorEngine} using {@link TestSimulator2}. Insert a 
   * {@link TestComplexEvent2} in the beginning. The simulation stops when the
   * {@link SimulatorEventQueue} is empty. Total number of events processed is checked
   * against expected number (3 * 2^level - 2).
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testComplex2() throws IOException, InterruptedException {
    SimulatorEngine simulation = new TestSimulator2();
    simulation.run();
  }
}
