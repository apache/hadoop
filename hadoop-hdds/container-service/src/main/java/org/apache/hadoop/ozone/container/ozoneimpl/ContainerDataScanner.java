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
package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VolumeScanner scans a single volume.  Each VolumeScanner has its own thread.
 * <p>They are all managed by the DataNode's BlockScanner.
 */
public class ContainerDataScanner extends Thread {
  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerDataScanner.class);

  /**
   * The volume that we're scanning.
   */
  private final HddsVolume volume;
  private final ContainerController controller;
  private final DataTransferThrottler throttler;
  private final Canceler canceler;
  private final ContainerDataScrubberMetrics metrics;
  private final long dataScanInterval;

  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private volatile boolean stopping = false;


  public ContainerDataScanner(ContainerScrubberConfiguration conf,
                              ContainerController controller,
                              HddsVolume volume) {
    this.controller = controller;
    this.volume = volume;
    dataScanInterval = conf.getDataScanInterval();
    throttler = new HddsDataTransferThrottler(conf.getBandwidthPerVolume());
    canceler = new Canceler();
    metrics = ContainerDataScrubberMetrics.create(volume.toString());
    setName("ContainerDataScanner(" + volume + ")");
    setDaemon(true);
  }

  @Override
  public void run() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: thread starting.", this);
    }
    try {
      while (!stopping) {
        runIteration();
        metrics.resetNumContainersScanned();
        metrics.resetNumUnhealthyContainers();
      }
      LOG.info("{} exiting.", this);
    } catch (Throwable e) {
      LOG.error("{} exiting because of exception ", this, e);
    } finally {
      if (metrics != null) {
        metrics.unregister();
      }
    }
  }

  @VisibleForTesting
  public void runIteration() {
    long startTime = System.nanoTime();
    Iterator<Container<?>> itr = controller.getContainers(volume);
    while (!stopping && itr.hasNext()) {
      Container c = itr.next();
      if (c.shouldScanData()) {
        try {
          if (!c.scanData(throttler, canceler)) {
            metrics.incNumUnHealthyContainers();
            controller.markContainerUnhealthy(
                c.getContainerData().getContainerID());
          }
        } catch (IOException ex) {
          long containerId = c.getContainerData().getContainerID();
          LOG.warn("Unexpected exception while scanning container "
              + containerId, ex);
        } finally {
          metrics.incNumContainersScanned();
        }
      }
    }
    long totalDuration = System.nanoTime() - startTime;
    if (!stopping) {
      if (metrics.getNumContainersScanned() > 0) {
        metrics.incNumScanIterations();
        LOG.info("Completed an iteration of container data scrubber in" +
                " {} minutes." +
                " Number of iterations (since the data-node restart) : {}" +
                ", Number of containers scanned in this iteration : {}" +
                ", Number of unhealthy containers found in this iteration : {}",
            TimeUnit.NANOSECONDS.toMinutes(totalDuration),
            metrics.getNumScanIterations(),
            metrics.getNumContainersScanned(),
            metrics.getNumUnHealthyContainers());
      }
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(totalDuration);
      long remainingSleep = dataScanInterval - elapsedMillis;
      if (remainingSleep > 0) {
        try {
          Thread.sleep(remainingSleep);
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  public synchronized void shutdown() {
    this.stopping = true;
    this.canceler.cancel("ContainerDataScanner("+volume+") is shutting down");
    this.interrupt();
    try {
      this.join();
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected exception while stopping data scanner for volume "
          + volume, ex);
    }
  }

  @VisibleForTesting
  public ContainerDataScrubberMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    return "ContainerDataScanner(" + volume +
        ", " + volume.getStorageID() + ")";
  }

  private class HddsDataTransferThrottler extends DataTransferThrottler {
    HddsDataTransferThrottler(long bandwidthPerSec) {
      super(bandwidthPerSec);
    }

    @Override
    public synchronized void throttle(long numOfBytes) {
      ContainerDataScanner.this.metrics.incNumBytesScanned(numOfBytes);
      super.throttle(numOfBytes);
    }

    @Override
    public synchronized void throttle(long numOfBytes, Canceler c) {
      ContainerDataScanner.this.metrics.incNumBytesScanned(numOfBytes);
      super.throttle(numOfBytes, c);
    }
  }
}
