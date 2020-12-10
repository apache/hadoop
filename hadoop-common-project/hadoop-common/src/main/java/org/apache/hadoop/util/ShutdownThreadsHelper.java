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

package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Helper class to shutdown {@link Thread}s and {@link ExecutorService}s.
 */
public class ShutdownThreadsHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShutdownThreadsHelper.class);

  @VisibleForTesting
  static final int SHUTDOWN_WAIT_MS = 3000;

  /**
   * @param thread {@link Thread to be shutdown}
   * @return <tt>true</tt> if the thread is successfully interrupted,
   * <tt>false</tt> otherwise
   */
  public static boolean shutdownThread(Thread thread) {
    return shutdownThread(thread, SHUTDOWN_WAIT_MS);
  }

  /**
   * @param thread {@link Thread to be shutdown}
   * @param timeoutInMilliSeconds time to wait for thread to join after being
   *                              interrupted
   * @return <tt>true</tt> if the thread is successfully interrupted,
   * <tt>false</tt> otherwise
   */
  public static boolean shutdownThread(Thread thread,
                                    long timeoutInMilliSeconds) {
    if (thread == null) {
      return true;
    }

    try {
      thread.interrupt();
      thread.join(timeoutInMilliSeconds);
      return true;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted while shutting down thread - " + thread.getName());
      return false;
    }
  }

  /**
   * @param service {@link ExecutorService to be shutdown}
   * @return <tt>true</tt> if the service is terminated,
   * <tt>false</tt> otherwise
   * @throws InterruptedException
   */
  public static boolean shutdownExecutorService(ExecutorService service)
      throws InterruptedException {
    return shutdownExecutorService(service, SHUTDOWN_WAIT_MS);
  }

  /**
   * @param service {@link ExecutorService to be shutdown}
   * @param timeoutInMs time to wait for {@link
   * ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)}
   *                    calls in milli seconds.
   * @return <tt>true</tt> if the service is terminated,
   * <tt>false</tt> otherwise
   * @throws InterruptedException
   */
  public static boolean shutdownExecutorService(ExecutorService service,
                                        long timeoutInMs)
      throws InterruptedException {
    if (service == null) {
      return true;
    }

    service.shutdown();
    if (!service.awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS)) {
      service.shutdownNow();
      return service.awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS);
    } else {
      return true;
    }
  }
}
