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

package org.apache.hadoop.yarn.server.timelineservice.collector;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that manages adding and removing collectors and their lifecycle. It
 * provides thread safety access to the collectors inside.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TimelineCollectorManager extends CompositeService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollectorManager.class);

  private TimelineWriter writer;
  private ScheduledExecutorService writerFlusher;
  private int flushInterval;
  private boolean writerFlusherRunning;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    writer = createTimelineWriter(conf);
    writer.init(conf);
    // create a single dedicated thread for flushing the writer on a periodic
    // basis
    writerFlusher = Executors.newSingleThreadScheduledExecutor();
    flushInterval = conf.getInt(
        YarnConfiguration.
        TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS,
        YarnConfiguration.
        DEFAULT_TIMELINE_SERVICE_WRITER_FLUSH_INTERVAL_SECONDS);
    super.serviceInit(conf);
  }

  private TimelineWriter createTimelineWriter(final Configuration conf) {
    String timelineWriterClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WRITER_CLASS);
    LOG.info("Using TimelineWriter: " + timelineWriterClassName);
    try {
      Class<?> timelineWriterClazz = Class.forName(timelineWriterClassName);
      if (TimelineWriter.class.isAssignableFrom(timelineWriterClazz)) {
        return (TimelineWriter) ReflectionUtils.newInstance(
            timelineWriterClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + timelineWriterClassName
            + " not instance of " + TimelineWriter.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineWriter: "
          + timelineWriterClassName, e);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    if (writer != null) {
      writer.start();
    }
    // schedule the flush task
    writerFlusher.scheduleAtFixedRate(new WriterFlushTask(writer),
        flushInterval, flushInterval, TimeUnit.SECONDS);
    writerFlusherRunning = true;
  }

  // access to this map is synchronized with the map itself
  private final Map<ApplicationId, TimelineCollector> collectors =
      Collections.synchronizedMap(
          new HashMap<ApplicationId, TimelineCollector>());

  public TimelineCollectorManager(String name) {
    super(name);
  }

  protected TimelineWriter getWriter() {
    return writer;
  }

  /**
   * Put the collector into the collection if an collector mapped by id does
   * not exist.
   *
   * @param appId Application Id for which collector needs to be put.
   * @param collector timeline collector to be put.
   * @throws YarnRuntimeException if there  was any exception in initializing
   *                              and starting the app level service
   * @return the collector associated with id after the potential put.
   */
  public TimelineCollector putIfAbsent(ApplicationId appId,
      TimelineCollector collector) {
    TimelineCollector collectorInTable = null;
    synchronized (collectors) {
      collectorInTable = collectors.get(appId);
      if (collectorInTable == null) {
        try {
          // initialize, start, and add it to the collection so it can be
          // cleaned up when the parent shuts down
          collector.init(getConfig());
          collector.setWriter(writer);
          collector.start();
          collectors.put(appId, collector);
          LOG.info("the collector for " + appId + " was added");
          collectorInTable = collector;
          postPut(appId, collectorInTable);
        } catch (Exception e) {
          throw new YarnRuntimeException(e);
        }
      } else {
        LOG.info("the collector for " + appId + " already exists!");
      }
    }
    return collectorInTable;
  }

  /**
   * Callback handler for the timeline collector manager when a collector has
   * been added into the collector map.
   * @param appId Application id of the collector.
   * @param collector The actual timeline collector that has been added.
   */
  public void postPut(ApplicationId appId, TimelineCollector collector) {
    doPostPut(appId, collector);
    collector.setReadyToAggregate();
  }

  /**
   * A template method that will be called by
   * {@link  #postPut(ApplicationId, TimelineCollector)}.
   * @param appId Application id of the collector.
   * @param collector The actual timeline collector that has been added.
   */
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
  }

  /**
   * Removes the collector for the specified id. The collector is also stopped
   * as a result. If the collector does not exist, no change is made.
   *
   * @param appId Application Id to remove.
   * @return whether it was removed successfully
   */
  public boolean remove(ApplicationId appId) {
    TimelineCollector collector = collectors.remove(appId);
    if (collector == null) {
      LOG.error("the collector for " + appId + " does not exist!");
    } else {
      synchronized (collector) {
        postRemove(appId, collector);
        // stop the service to do clean up
        collector.stop();
      }
      LOG.info("The collector service for " + appId + " was removed");
    }
    return collector != null;
  }

  protected void postRemove(ApplicationId appId, TimelineCollector collector) {

  }

  /**
   * Returns the collector for the specified id.
   *
   * @param appId Application Id for which we need to get the collector.
   * @return the collector or null if it does not exist
   */
  public TimelineCollector get(ApplicationId appId) {
    return collectors.get(appId);
  }

  /**
   * Returns whether the collector for the specified id exists in this
   * collection.
   * @param appId Application Id.
   * @return true if collector for the app id is found, false otherwise.
   */
  public boolean containsTimelineCollector(ApplicationId appId) {
    return collectors.containsKey(appId);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (collectors != null && collectors.size() > 0) {
      synchronized (collectors) {
        for (TimelineCollector c : collectors.values()) {
          c.serviceStop();
        }
      }
    }
    // stop the flusher first
    if (writerFlusher != null) {
      writerFlusher.shutdown();
      writerFlusherRunning = false;
      if (!writerFlusher.awaitTermination(30, TimeUnit.SECONDS)) {
        // in reality it should be ample time for the flusher task to finish
        // even if it times out, writers may be able to handle closing in this
        // situation fine
        // proceed to close the writer
        LOG.warn("failed to stop the flusher task in time. " +
            "will still proceed to close the writer.");
      }
    }
    if (writer != null) {
      writer.close();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  boolean writerFlusherRunning() {
    return writerFlusherRunning;
  }

  /**
   * Task that invokes the flush operation on the timeline writer.
   */
  private static class WriterFlushTask implements Runnable {
    private final TimelineWriter writer;

    public WriterFlushTask(TimelineWriter writer) {
      this.writer = writer;
    }

    public void run() {
      try {
        // synchronize on the writer object to avoid flushing timeline
        // entities placed on the buffer by synchronous putEntities
        // requests.
        synchronized (writer) {
          writer.flush();
        }
      } catch (Throwable th) {
        // we need to handle all exceptions or subsequent execution may be
        // suppressed
        LOG.error("exception during timeline writer flush!", th);
      }
    }
  }
}
