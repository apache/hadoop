/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.executor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Server;


/**
 * Abstract base class for all HBase event handlers. Subclasses should
 * implement the {@link #process()} method.  Subclasses should also do all
 * necessary checks up in their constructor if possible -- check table exists,
 * is disabled, etc. -- so they fail fast rather than later when process is
 * running.  Do it this way because process be invoked directly but event
 * handlers are also
 * run in an executor context -- i.e. asynchronously -- and in this case,
 * exceptions thrown at process time will not be seen by the invoker, not till
 * we implement a call-back mechanism so the client can pick them up later.
 * <p>
 * Event handlers have an {@link EventType}.
 * {@link EventType} is a list of ALL handler event types.  We need to keep
 * a full list in one place -- and as enums is a good shorthand for an
 * implemenations -- because event handlers can be passed to executors when
 * they are to be run asynchronously. The
 * hbase executor, see {@link ExecutorService}, has a switch for passing
 * event type to executor.
 * <p>
 * Event listeners can be installed and will be called pre- and post- process if
 * this EventHandler is run in a Thread (its a Runnable so if its {@link #run()}
 * method gets called).  Implement
 * {@link EventHandlerListener}s, and registering using
 * {@link #setListener(EventHandlerListener)}.
 * @see ExecutorService
 */
public abstract class EventHandler implements Runnable, Comparable<Runnable> {
  private static final Log LOG = LogFactory.getLog(EventHandler.class);

  // type of event this object represents
  protected EventType eventType;

  protected Server server;

  // sequence id generator for default FIFO ordering of events
  protected static AtomicLong seqids = new AtomicLong(0);

  // sequence id for this event
  private final long seqid;

  // Listener to call pre- and post- processing.  May be null.
  private EventHandlerListener listener;

  // Time to wait for events to happen, should be kept short
  protected final int waitingTimeForEvents;

  /**
   * This interface provides pre- and post-process hooks for events.
   */
  public interface EventHandlerListener {
    /**
     * Called before any event is processed
     * @param event The event handler whose process method is about to be called.
     */
    public void beforeProcess(EventHandler event);
    /**
     * Called after any event is processed
     * @param event The event handler whose process method is about to be called.
     */
    public void afterProcess(EventHandler event);
  }

  /**
   * List of all HBase event handler types.  Event types are named by a
   * convention: event type names specify the component from which the event
   * originated and then where its destined -- e.g. RS2ZK_ prefix means the
   * event came from a regionserver destined for zookeeper -- and then what
   * the even is; e.g. REGION_OPENING.
   * 
   * <p>We give the enums indices so we can add types later and keep them
   * grouped together rather than have to add them always to the end as we
   * would have to if we used raw enum ordinals.
   */
  public enum EventType {
    // Messages originating from RS (NOTE: there is NO direct communication from
    // RS to Master). These are a result of RS updates into ZK.
    RS_ZK_REGION_CLOSING      (1),   // RS is in process of closing a region
    RS_ZK_REGION_CLOSED       (2),   // RS has finished closing a region
    RS_ZK_REGION_OPENING      (3),   // RS is in process of opening a region
    RS_ZK_REGION_OPENED       (4),   // RS has finished opening a region
    RS_ZK_REGION_SPLITTING    (5),   // RS has started a region split
    RS_ZK_REGION_SPLIT        (6),   // RS split has completed.

    // Messages originating from Master to RS
    M_RS_OPEN_REGION          (20),  // Master asking RS to open a region
    M_RS_OPEN_ROOT            (21),  // Master asking RS to open root
    M_RS_OPEN_META            (22),  // Master asking RS to open meta
    M_RS_CLOSE_REGION         (23),  // Master asking RS to close a region
    M_RS_CLOSE_ROOT           (24),  // Master asking RS to close root
    M_RS_CLOSE_META           (25),  // Master asking RS to close meta

    // Messages originating from Client to Master
    C_M_DELETE_TABLE          (40),   // Client asking Master to delete a table
    C_M_DISABLE_TABLE         (41),   // Client asking Master to disable a table
    C_M_ENABLE_TABLE          (42),   // Client asking Master to enable a table
    C_M_MODIFY_TABLE          (43),   // Client asking Master to modify a table
    C_M_ADD_FAMILY            (44),   // Client asking Master to add family to table
    C_M_DELETE_FAMILY         (45),   // Client asking Master to delete family of table
    C_M_MODIFY_FAMILY         (46),   // Client asking Master to modify family of table

    // Updates from master to ZK. This is done by the master and there is
    // nothing to process by either Master or RS
    M_ZK_REGION_OFFLINE       (50),  // Master adds this region as offline in ZK

    // Master controlled events to be executed on the master
    M_SERVER_SHUTDOWN         (70),  // Master is processing shutdown of a RS
    M_META_SERVER_SHUTDOWN    (72);  // Master is processing shutdown of RS hosting a meta region (-ROOT- or .META.).

    /**
     * Constructor
     */
    EventType(int value) {}
  }

  /**
   * Default base class constructor.
   */
  public EventHandler(Server server, EventType eventType) {
    this.server = server;
    this.eventType = eventType;
    seqid = seqids.incrementAndGet();
    this.waitingTimeForEvents = server.getConfiguration().
        getInt("hbase.master.event.waiting.time", 1000);
  }

  public void run() {
    try {
      if (getListener() != null) getListener().beforeProcess(this);
      process();
      if (getListener() != null) getListener().afterProcess(this);
    } catch(Throwable t) {
      LOG.error("Caught throwable while processing event " + eventType, t);
    }
  }

  /**
   * This method is the main processing loop to be implemented by the various
   * subclasses.
   * @throws IOException
   */
  public abstract void process() throws IOException;

  /**
   * Return the event type
   * @return The event type.
   */
  public EventType getEventType() {
    return this.eventType;
  }

  /**
   * Get the priority level for this handler instance.  This uses natural
   * ordering so lower numbers are higher priority.
   * <p>
   * Lowest priority is Integer.MAX_VALUE.  Highest priority is 0.
   * <p>
   * Subclasses should override this method to allow prioritizing handlers.
   * <p>
   * Handlers with the same priority are handled in FIFO order.
   * <p>
   * @return Integer.MAX_VALUE by default, override to set higher priorities
   */
  public int getPriority() {
    return Integer.MAX_VALUE;
  }

  /**
   * @return This events' sequence id.
   */
  public long getSeqid() {
    return this.seqid;
  }

  /**
   * Default prioritized runnable comparator which implements a FIFO ordering.
   * <p>
   * Subclasses should not override this.  Instead, if they want to implement
   * priority beyond FIFO, they should override {@link #getPriority()}.
   */
  @Override
  public int compareTo(Runnable o) {
    EventHandler eh = (EventHandler)o;
    if(getPriority() != eh.getPriority()) {
      return (getPriority() < eh.getPriority()) ? -1 : 1;
    }
    return (this.seqid < eh.seqid) ? -1 : 1;
  }

  /**
   * @return Current listener or null if none set.
   */
  public synchronized EventHandlerListener getListener() {
    return listener;
  }

  /**
   * @param listener Listener to call pre- and post- {@link #process()}.
   */
  public synchronized void setListener(EventHandlerListener listener) {
    this.listener = listener;
  }
}
