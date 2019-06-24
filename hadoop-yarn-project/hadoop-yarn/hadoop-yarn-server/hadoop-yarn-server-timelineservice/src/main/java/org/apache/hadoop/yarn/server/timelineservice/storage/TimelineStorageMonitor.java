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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * This abstract class is for monitoring Health of Timeline Storage.
 */
public abstract class TimelineStorageMonitor  {
  private static final Logger LOG = LoggerFactory
      .getLogger(TimelineStorageMonitor.class);

  /** Different Storages supported by ATSV2. */
  public enum Storage {
    HBase
  }

  private ScheduledExecutorService monitorExecutorService;
  private long monitorInterval;
  private Storage storage;
  private AtomicBoolean storageDown = new AtomicBoolean();

  public TimelineStorageMonitor(Configuration conf, Storage storage) {
    this.storage = storage;
    this.monitorInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS
        );
  }

  public void start() {
    LOG.info("Scheduling {} storage monitor at interval {}",
        this.storage, monitorInterval);
    monitorExecutorService = Executors.newScheduledThreadPool(1);
    monitorExecutorService.scheduleAtFixedRate(new MonitorThread(), 0,
        monitorInterval, TimeUnit.MILLISECONDS);
  }

  public void stop() throws Exception {
    if (monitorExecutorService != null) {
      monitorExecutorService.shutdownNow();
      if (!monitorExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Failed to stop the monitor task in time. " +
            "will still proceed to close the monitor.");
      }
    }
  }

  abstract public void healthCheck() throws Exception;

  public void checkStorageIsUp() throws IOException {
    if (storageDown.get()) {
      throw new IOException(storage + " is down");
    }
  }

  private class MonitorThread implements Runnable {
    @Override
    public void run() {
      try {
        LOG.debug("Running Timeline Storage monitor");
        healthCheck();
        if (storageDown.getAndSet(false)) {
          LOG.debug("{} health check succeeded, " +
              "assuming storage is up", storage);
        }
      } catch (Exception e) {
        LOG.warn(String.format("Got failure attempting to read from %s, " +
            "assuming Storage is down", storage), e);
        storageDown.getAndSet(true);
      }
    }
  }

}
