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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * {@link SimulatorEventQueue} maintains a priority queue of events scheduled in the
 * future in virtual time. Events happen in virtual time order. The
 * {@link SimulatorEventQueue} has the notion of "currentTime" which is defined as time
 * stamp of the last event already handled. An event can be inserted into the
 * {@link SimulatorEventQueue}, and its time stamp must be later than "currentTime".
 */
public class SimulatorEventQueue {
  public static final List<SimulatorEvent> EMPTY_EVENTS = new ArrayList<SimulatorEvent>();
  private SimulatorEvent lastEvent = null;
  private long eventCount = 0;
  private final PriorityQueue<SimulatorEvent> events = new PriorityQueue<SimulatorEvent>(1,
      new Comparator<SimulatorEvent>() {
        @Override
        public int compare(SimulatorEvent o1, SimulatorEvent o2) {
          if (o1.getTimeStamp() < o2.getTimeStamp()) {
            return -1;
          } else if (o1.getTimeStamp() > o2.getTimeStamp()) {
            return 1;
          }
          if (o1.getInternalCount() < o2.getInternalCount()) {
            return -1;
          } else if (o1.getInternalCount() > o2.getInternalCount()) {
            return 1;
          }
          return 0;
        }
      });

  /**
   * Get the next earliest {@link SimulatorEvent} to be handled. This {@link SimulatorEvent} has
   * the smallest time stamp among all {@link SimulatorEvent}s currently scheduled in the
   * {@link SimulatorEventQueue}.
   * 
   * @return the next {@link SimulatorEvent} to be handled. Or null if no more events.
   */
  public SimulatorEvent get() {
    lastEvent = events.poll();
    return lastEvent;
  }

  /**
   * Add a single {@link SimulatorEvent} to the {@link SimulatorEventQueue}.
   * 
   * @param event
   *          the {@link SimulatorEvent}
   * @return true if the event is added to the queue (to follow the same
   *         convention as Collection.add()).
   */
  public boolean add(SimulatorEvent event) {
    if (lastEvent != null && event.getTimeStamp() < lastEvent.getTimeStamp())
      throw new IllegalArgumentException("Event happens in the past: "
          + event.getClass());

    event.setInternalCount(eventCount++);
    return events.add(event);
  }

  /**
   * Adding all {@link SimulatorEvent}s.
   * 
   * @param events
   *          The container contains all the events to be added.
   * @return true if the queue is changed as a result of the call (to follow the
   *         same convention as Collection.addAll()).
   */
  public boolean addAll(Collection<? extends SimulatorEvent> events) {
    long lastTimeStamp = (lastEvent == null) ? Long.MIN_VALUE : lastEvent
        .getTimeStamp();
    for (SimulatorEvent e : events) {
      if (e.getTimeStamp() < lastTimeStamp) {
        throw new IllegalArgumentException("Event happens in the past: "
            + e.getClass() + "(" + e.getTimeStamp() + "<" + lastTimeStamp);
      }
      e.setInternalCount(eventCount++);
    }
    return this.events.addAll(events);
  }

  /**
   * Get the current time in the queue. It is defined by time stamp of the last
   * event handled.
   * 
   * @return the current time in the queue
   */
  public long getCurrentTime() {
    if (lastEvent != null)
      return lastEvent.getTimeStamp();
    else
      return 0;
  }

  /**
   * Get the size of currently scheduled events. Number of events in the system
   * is the major scaling factor of the simulator.
   * 
   * @return the size of currently scheduled events
   */
  public int getSize() {
    return events.size();
  }

  /**
   * Get the total number of events handled in a simulation. This is an
   * indicator of how large a particular simulation run is.
   * 
   * @return the total number of events handled in a simulation
   */
  public long getEventCount() {
    return eventCount;
  }
}
