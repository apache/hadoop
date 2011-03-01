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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.EventHandler.EventHandlerListener;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is a generic executor service. This component abstracts a
 * threadpool, a queue to which {@link EventHandler.EventType}s can be submitted,
 * and a <code>Runnable</code> that handles the object that is added to the queue.
 *
 * <p>In order to create a new service, create an instance of this class and
 * then do: <code>instance.startExecutorService("myService");</code>.  When done
 * call {@link #shutdown()}.
 *
 * <p>In order to use the service created above, call
 * {@link #submit(EventHandler)}. Register pre- and post- processing listeners
 * by registering your implementation of {@link EventHandler.EventHandlerListener}
 * with {@link #registerListener(EventHandler.EventType, EventHandler.EventHandlerListener)}.  Be sure
 * to deregister your listener when done via {@link #unregisterListener(EventHandler.EventType)}.
 */
public class ExecutorService {
  private static final Log LOG = LogFactory.getLog(ExecutorService.class);

  // hold the all the executors created in a map addressable by their names
  private final ConcurrentHashMap<String, Executor> executorMap =
    new ConcurrentHashMap<String, Executor>();

  // listeners that are called before and after an event is processed
  private ConcurrentHashMap<EventHandler.EventType, EventHandlerListener> eventHandlerListeners =
    new ConcurrentHashMap<EventHandler.EventType, EventHandlerListener>();

  // Name of the server hosting this executor service.
  private final String servername;

  /**
   * The following is a list of all executor types, both those that run in the
   * master and those that run in the regionserver.
   */
  public enum ExecutorType {

    // Master executor services
    MASTER_CLOSE_REGION        (1),
    MASTER_OPEN_REGION         (2),
    MASTER_SERVER_OPERATIONS   (3),
    MASTER_TABLE_OPERATIONS    (4),
    MASTER_RS_SHUTDOWN         (5),
    MASTER_META_SERVER_OPERATIONS (6),

    // RegionServer executor services
    RS_OPEN_REGION             (20),
    RS_OPEN_ROOT               (21),
    RS_OPEN_META               (22),
    RS_CLOSE_REGION            (23),
    RS_CLOSE_ROOT              (24),
    RS_CLOSE_META              (25);

    ExecutorType(int value) {}

    /**
     * @param serverName
     * @return Conflation of the executor type and the passed servername.
     */
    String getExecutorName(String serverName) {
      return this.toString() + "-" + serverName;
    }
  }

  /**
   * Returns the executor service type (the thread pool instance) for the
   * passed event handler type.
   * @param type EventHandler type.
   */
  public ExecutorType getExecutorServiceType(final EventHandler.EventType type) {
    switch(type) {
      // Master executor services

      case RS_ZK_REGION_CLOSED:
        return ExecutorType.MASTER_CLOSE_REGION;

      case RS_ZK_REGION_OPENED:
        return ExecutorType.MASTER_OPEN_REGION;

      case RS_ZK_REGION_SPLIT:
      case M_SERVER_SHUTDOWN:
        return ExecutorType.MASTER_SERVER_OPERATIONS;

      case M_META_SERVER_SHUTDOWN:
        return ExecutorType.MASTER_META_SERVER_OPERATIONS;

      case C_M_DELETE_TABLE:
      case C_M_DISABLE_TABLE:
      case C_M_ENABLE_TABLE:
      case C_M_MODIFY_TABLE:
        return ExecutorType.MASTER_TABLE_OPERATIONS;

      // RegionServer executor services

      case M_RS_OPEN_REGION:
        return ExecutorType.RS_OPEN_REGION;

      case M_RS_OPEN_ROOT:
        return ExecutorType.RS_OPEN_ROOT;

      case M_RS_OPEN_META:
        return ExecutorType.RS_OPEN_META;

      case M_RS_CLOSE_REGION:
        return ExecutorType.RS_CLOSE_REGION;

      case M_RS_CLOSE_ROOT:
        return ExecutorType.RS_CLOSE_ROOT;

      case M_RS_CLOSE_META:
        return ExecutorType.RS_CLOSE_META;

      default:
        throw new RuntimeException("Unhandled event type " + type);
    }
  }

  /**
   * Default constructor.
   * @param servername Name of the hosting server.
   */
  public ExecutorService(final String servername) {
    super();
    this.servername = servername;
  }

  /**
   * Start an executor service with a given name. If there was a service already
   * started with the same name, this throws a RuntimeException.
   * @param name Name of the service to start.
   */
  void startExecutorService(String name, int maxThreads) {
    if (this.executorMap.get(name) != null) {
      throw new RuntimeException("An executor service with the name " + name +
        " is already running!");
    }
    Executor hbes = new Executor(name, maxThreads, this.eventHandlerListeners);
    if (this.executorMap.putIfAbsent(name, hbes) != null) {
      throw new RuntimeException("An executor service with the name " + name +
      " is already running (2)!");
    }
    LOG.debug("Starting executor service name=" + name +
      ", corePoolSize=" + hbes.threadPoolExecutor.getCorePoolSize() +
      ", maxPoolSize=" + hbes.threadPoolExecutor.getMaximumPoolSize());
  }

  boolean isExecutorServiceRunning(String name) {
    return this.executorMap.containsKey(name);
  }

  public void shutdown() {
    for(Entry<String, Executor> entry: this.executorMap.entrySet()) {
      List<Runnable> wasRunning =
        entry.getValue().threadPoolExecutor.shutdownNow();
      if (!wasRunning.isEmpty()) {
        LOG.info(entry.getKey() + " had " + wasRunning + " on shutdown");
      }
    }
    this.executorMap.clear();
  }

  Executor getExecutor(final ExecutorType type) {
    return getExecutor(type.getExecutorName(this.servername));
  }

  Executor getExecutor(String name) {
    Executor executor = this.executorMap.get(name);
    if (executor == null) {
      LOG.debug("Executor service [" + name + "] not found in " + this.executorMap);
    }
    return executor;
  }


  public void startExecutorService(final ExecutorType type, final int maxThreads) {
    String name = type.getExecutorName(this.servername);
    if (isExecutorServiceRunning(name)) {
      LOG.debug("Executor service " + toString() + " already running on " +
        this.servername);
      return;
    }
    startExecutorService(name, maxThreads);
  }

  public void submit(final EventHandler eh) {
    getExecutor(getExecutorServiceType(eh.getEventType())).submit(eh);
  }

  /**
   * Subscribe to updates before and after processing instances of
   * {@link EventHandler.EventType}.  Currently only one listener per
   * event type.
   * @param type Type of event we're registering listener for
   * @param listener The listener to run.
   */
  public void registerListener(final EventHandler.EventType type,
      final EventHandlerListener listener) {
    this.eventHandlerListeners.put(type, listener);
  }

  /**
   * Stop receiving updates before and after processing instances of
   * {@link EventHandler.EventType}
   * @param type Type of event we're registering listener for
   * @return The listener we removed or null if we did not remove it.
   */
  public EventHandlerListener unregisterListener(final EventHandler.EventType type) {
    return this.eventHandlerListeners.remove(type);
  }

  /**
   * Executor instance.
   */
  static class Executor {
    // how long to retain excess threads
    final long keepAliveTimeInMillis = 1000;
    // the thread pool executor that services the requests
    final ThreadPoolExecutor threadPoolExecutor;
    // work queue to use - unbounded queue
    final BlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
    private final String name;
    private final Map<EventHandler.EventType, EventHandlerListener> eventHandlerListeners;

    protected Executor(String name, int maxThreads,
        final Map<EventHandler.EventType, EventHandlerListener> eventHandlerListeners) {
      this.name = name;
      this.eventHandlerListeners = eventHandlerListeners;
      // create the thread pool executor
      this.threadPoolExecutor = new ThreadPoolExecutor(maxThreads, maxThreads,
          keepAliveTimeInMillis, TimeUnit.MILLISECONDS, q);
      // name the threads for this threadpool
      ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
      tfb.setNameFormat(this.name + "-%d");
      this.threadPoolExecutor.setThreadFactory(tfb.build());
    }

    /**
     * Submit the event to the queue for handling.
     * @param event
     */
    void submit(final EventHandler event) {
      // If there is a listener for this type, make sure we call the before
      // and after process methods.
      EventHandlerListener listener =
        this.eventHandlerListeners.get(event.getEventType());
      if (listener != null) {
        event.setListener(listener);
      }
      this.threadPoolExecutor.execute(event);
    }
  }
}
