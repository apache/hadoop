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

package org.apache.hadoop.registry.client.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a little event catcher for curator asynchronous
 * operations.
 */
public class CuratorEventCatcher implements BackgroundCallback {

  private static final Logger LOG =
      LoggerFactory.getLogger(CuratorEventCatcher.class);

  public final BlockingQueue<CuratorEvent>
      events = new LinkedBlockingQueue<CuratorEvent>(1);

  private final AtomicInteger eventCounter = new AtomicInteger(0);


  @Override
  public void processResult(CuratorFramework client,
      CuratorEvent event) throws
      Exception {
    LOG.info("received {}", event);
    eventCounter.incrementAndGet();
    events.put(event);
  }


  public int getCount() {
    return eventCounter.get();
  }

  /**
   * Blocking operation to take the first event off the queue
   * @return the first event on the queue, when it arrives
   * @throws InterruptedException if interrupted
   */
  public CuratorEvent take() throws InterruptedException {
    return events.take();
  }
}
