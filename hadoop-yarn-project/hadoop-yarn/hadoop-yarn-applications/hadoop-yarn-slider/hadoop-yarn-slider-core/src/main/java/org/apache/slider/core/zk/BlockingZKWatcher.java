/*
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

package org.apache.slider.core.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingZKWatcher implements Watcher {
  
  protected static final Logger log =
    LoggerFactory.getLogger(BlockingZKWatcher.class);
  private final AtomicBoolean connectedFlag = new AtomicBoolean(false);

  @Override
  public void process(WatchedEvent event) {
    log.info("ZK binding callback received");
    connectedFlag.set(true);
    synchronized (connectedFlag) {
      try {
        connectedFlag.notify();
      } catch (Exception e) {
        log.warn("failed while waiting for notification", e);
      }
    }
  }

  /**
   * Wait for a flag to go true
   * @param timeout timeout in millis
   */

  public void waitForZKConnection(int timeout)
      throws InterruptedException, ConnectException {
    synchronized (connectedFlag) {
      if (!connectedFlag.get()) {
        log.info("waiting for ZK event");
        //wait a bit
        connectedFlag.wait(timeout);
      }
    }
    if (!connectedFlag.get()) {
      throw new ConnectException("Unable to connect to ZK quorum");
    }
  }

}
