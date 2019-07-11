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

  /**
   * True if the thread is stopping.<p/>
   * Protected by this object's lock.
   */
  private volatile boolean stopping = false;


  public ContainerDataScanner(ContainerController controller,
                              HddsVolume volume, long bytesPerSec) {
    this.controller = controller;
    this.volume = volume;
    this.throttler = new DataTransferThrottler(bytesPerSec);
    this.canceler = new Canceler();
    setName("ContainerDataScanner(" + volume + ")");
    setDaemon(true);
  }

  @Override
  public void run() {
    LOG.trace("{}: thread starting.", this);
    try {
      while (!stopping) {
        Iterator<Container> itr = controller.getContainers(volume);
        while (!stopping && itr.hasNext()) {
          Container c = itr.next();
          try {
            if (c.shouldScanData()) {
              if(!c.scanData(throttler, canceler)) {
                controller.markContainerUnhealthy(
                    c.getContainerData().getContainerID());
              }
            }
          } catch (IOException ex) {
            long containerId = c.getContainerData().getContainerID();
            LOG.warn("Unexpected exception while scanning container "
                + containerId, ex);
          }
        }
      }
      LOG.info("{} exiting.", this);
    } catch (Throwable e) {
      LOG.error("{} exiting because of exception ", this, e);
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

  @Override
  public String toString() {
    return "ContainerDataScanner(" + volume +
        ", " + volume.getStorageID() + ")";
  }
}
