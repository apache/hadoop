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
  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private boolean stopping = false;

  public ContainerMetadataScanner(ContainerController controller,
                                  long metadataScanInterval) {
    this.controller = controller;
    this.metadataScanInterval = metadataScanInterval;
    setName("ContainerMetadataScanner");
    setDaemon(true);
  }

  @Override
  public void run() {
    /**
     * the outer daemon loop exits on down()
     */
    LOG.info("Background ContainerMetadataScanner starting up");
    while (!stopping) {
      long start = System.nanoTime();
      scrub();
      long interval = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start);
      // ensure to delay next metadata scan with respect to user config.
      if (!stopping && interval < metadataScanInterval) {
        try {
          Thread.sleep(metadataScanInterval - interval);
        } catch (InterruptedException e) {
          LOG.info("Background ContainerMetadataScanner interrupted." +
              " Going to exit");
        }
      }
    }
  }

  private void scrub() {
    Iterator<Container> containerIt = controller.getContainers();
    long count = 0;

    while (!stopping && containerIt.hasNext()) {
      Container container = containerIt.next();
      try {
        scrub(container);
      } catch (IOException e) {
        LOG.info("Unexpected error while scrubbing container {}",
            container.getContainerData().getContainerID());
      }
      count++;
    }

    LOG.debug("iterator ran integrity checks on {} containers", count);
  }

  @VisibleForTesting
  public void scrub(Container container) throws IOException {
    if (!container.scanMetaData()) {
      controller.markContainerUnhealthy(
          container.getContainerData().getContainerID());
    }
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
