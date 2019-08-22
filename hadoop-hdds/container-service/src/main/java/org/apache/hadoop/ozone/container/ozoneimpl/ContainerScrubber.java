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
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Background Metadata scrubbing for Ozone Containers.
 * Future scope : data(chunks) checksum verification.
 */
public class ContainerScrubber implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerScrubber.class);
  private final OzoneConfiguration config;
  private final long timePerContainer = 10000; // 10 sec in millis
  private boolean halt;
  private Thread scrubThread;
  private ContainerController controller;


  public ContainerScrubber(OzoneConfiguration conf,
      ContainerController controller) {
    this.config = conf;
    this.halt = false;
    this.scrubThread = null;
    this.controller = controller;
  }

  @Override public void run() {
    /**
     * the outer daemon loop exits on down()
     */
    LOG.info("Background ContainerScrubber starting up");
    while (true) {

      scrub();

      if (this.halt) {
        break; // stop and exit if requested
      }

      try {
        Thread.sleep(300000); /* 5 min between scans */
      } catch (InterruptedException e) {
        LOG.info("Background ContainerScrubber interrupted. Going to exit");
      }
    }
  }

  /**
   * Start the scrub scanner thread.
   */
  public void up() {

    this.halt = false;
    if (this.scrubThread == null) {
      this.scrubThread = new Thread(this);
      scrubThread.start();
    } else {
      LOG.info("Scrubber up called multiple times. Scrub thread already up.");
    }
  }

  /**
   * Stop the scrub scanner thread. Wait for thread to exit
   */
  public void down() {

    this.halt = true;
    if (scrubThread == null) {
      LOG.info("Scrubber down invoked, but scrub thread is not running");
      return;
    }

    this.scrubThread.interrupt();
    try {
      this.scrubThread.join();
    } catch (Exception e) {
      LOG.warn("Exception when waiting for Container Scrubber thread ", e);
    } finally {
      this.scrubThread = null;
    }
  }

  /**
   * Current implementation : fixed rate scrub, no feedback loop.
   * Dynamic throttling based on system load monitoring to be
   * implemented later as jira [XXX]
   *
   * @param startTime
   */
  private void throttleScrubber(TimeStamp startTime) {
    TimeStamp endTime = new TimeStamp(System.currentTimeMillis());
    long timeTaken = endTime.getTime() - startTime.getTime();

    if (timeTaken < timePerContainer) {
      try {
        Thread.sleep(timePerContainer - timeTaken);
      } catch (InterruptedException e) {
        LOG.debug("Ignoring interrupted sleep inside throttle");
      }
    }
  }

  private void scrub() {
    Iterator<Container> containerIt = controller.getContainers();
    long count = 0;

    while (containerIt.hasNext() && !halt) {
      TimeStamp startTime = new TimeStamp(System.currentTimeMillis());
      Container container = containerIt.next();
      try {
        scrub(container);
      } catch (IOException e) {
        LOG.info("Unexpected error while scrubbing container {}",
                container.getContainerData().getContainerID());
      }

      count++;

      throttleScrubber(startTime);
    }

    LOG.debug("iterator ran integrity checks on {} containers", count);
  }

  @VisibleForTesting
  public void scrub(Container container) throws IOException {
    if (!container.check()) {
      controller.markContainerUnhealthy(
              container.getContainerData().getContainerID());
    }
  }
}