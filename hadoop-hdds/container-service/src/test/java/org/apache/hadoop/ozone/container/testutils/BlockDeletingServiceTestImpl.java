/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.testutils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background
    .BlockDeletingService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test class implementation for {@link BlockDeletingService}.
 */
public class BlockDeletingServiceTestImpl
    extends BlockDeletingService {

  // the service timeout
  private static final int SERVICE_TIMEOUT_IN_MILLISECONDS = 0;

  // tests only
  private CountDownLatch latch;
  private Thread testingThread;
  private AtomicInteger numOfProcessed = new AtomicInteger(0);

  public BlockDeletingServiceTestImpl(ContainerSet containerSet,
      int serviceInterval, Configuration conf) {
    super(containerSet, serviceInterval, SERVICE_TIMEOUT_IN_MILLISECONDS,
        TimeUnit.MILLISECONDS, conf);
  }

  @VisibleForTesting
  public void runDeletingTasks() {
    if (latch.getCount() > 0) {
      this.latch.countDown();
    } else {
      throw new IllegalStateException("Count already reaches zero");
    }
  }

  @VisibleForTesting
  public boolean isStarted() {
    return latch != null && testingThread.isAlive();
  }

  public int getTimesOfProcessed() {
    return numOfProcessed.get();
  }

  // Override the implementation to start a single on-call control thread.
  @Override public void start() {
    PeriodicalTask svc = new PeriodicalTask();
    // In test mode, relies on a latch countdown to runDeletingTasks tasks.
    Runnable r = () -> {
      while (true) {
        latch = new CountDownLatch(1);
        try {
          latch.await();
        } catch (InterruptedException e) {
          break;
        }
        Future<?> future = this.getExecutorService().submit(svc);
        try {
          // for tests, we only wait for 3s for completion
          future.get(3, TimeUnit.SECONDS);
          numOfProcessed.incrementAndGet();
        } catch (Exception e) {
          return;
        }
      }
    };

    testingThread = new ThreadFactoryBuilder()
        .setDaemon(true)
        .build()
        .newThread(r);
    testingThread.start();
  }

  @Override
  public void shutdown() {
    testingThread.interrupt();
    super.shutdown();
  }
}
