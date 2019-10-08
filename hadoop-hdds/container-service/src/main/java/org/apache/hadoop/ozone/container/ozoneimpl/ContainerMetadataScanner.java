/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible to perform metadata verification of the
 * containers.
 */
public class ContainerMetadataScanner extends Thread {
  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerMetadataScanner.class);

  private final ContainerController controller;
  private final long metadataScanInterval;
  private final ContainerMetadataScrubberMetrics metrics;
  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private boolean stopping = false;

  public ContainerMetadataScanner(ContainerScrubberConfiguration conf,
                                  ContainerController controller) {
    this.controller = controller;
    this.metadataScanInterval = conf.getMetadataScanInterval();
    this.metrics = ContainerMetadataScrubberMetrics.create();
    setName("ContainerMetadataScanner");
    setDaemon(true);
  }

  @Override
  public void run() {
    /*
     * the outer daemon loop exits on shutdown()
     */
    LOG.info("Background ContainerMetadataScanner starting up");
    while (!stopping) {
      runIteration();
      if(!stopping) {
        metrics.resetNumUnhealthyContainers();
        metrics.resetNumContainersScanned();
      }
    }
  }

  @VisibleForTesting
  void runIteration() {
    long start = System.nanoTime();
    Iterator<Container<?>> containerIt = controller.getContainers();
    while (!stopping && containerIt.hasNext()) {
      Container container = containerIt.next();
      try {
        scrub(container);
      } catch (IOException e) {
        LOG.info("Unexpected error while scrubbing container {}",
            container.getContainerData().getContainerID());
      } finally {
        metrics.incNumContainersScanned();
      }
    }
    long interval = System.nanoTime()-start;
    if (!stopping) {
      metrics.incNumScanIterations();
      LOG.info("Completed an iteration of container metadata scrubber in" +
              " {} minutes." +
              " Number of  iterations (since the data-node restart) : {}" +
              ", Number of containers scanned in this iteration : {}" +
              ", Number of unhealthy containers found in this iteration : {}",
          TimeUnit.NANOSECONDS.toMinutes(interval),
          metrics.getNumScanIterations(),
          metrics.getNumContainersScanned(),
          metrics.getNumUnHealthyContainers());
      // ensure to delay next metadata scan with respect to user config.
      if (interval < metadataScanInterval) {
        try {
          Thread.sleep(metadataScanInterval - interval);
        } catch (InterruptedException e) {
          LOG.info("Background ContainerMetadataScanner interrupted." +
              " Going to exit");
        }
      }
    }
  }

  @VisibleForTesting
  public void scrub(Container container) throws IOException {
    if (!container.scanMetaData()) {
      metrics.incNumUnHealthyContainers();
      controller.markContainerUnhealthy(
          container.getContainerData().getContainerID());
    }
  }

  @VisibleForTesting
  public ContainerMetadataScrubberMetrics getMetrics() {
    return metrics;
  }

  public synchronized void shutdown() {
    this.stopping = true;
    this.interrupt();
    try {
      this.join();
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected exception while stopping metadata scanner.", ex);
    }
  }
}
