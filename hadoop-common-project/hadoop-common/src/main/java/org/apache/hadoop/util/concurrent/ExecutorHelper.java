/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.util.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Helper functions for Executors. */
public final class ExecutorHelper {

  private static final Logger LOG = LoggerFactory
      .getLogger(ExecutorHelper.class);

  static void logThrowableFromAfterExecute(Runnable r, Throwable t) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("afterExecute in thread: " + Thread.currentThread()
          .getName() + ", runnable type: " + r.getClass().getName());
    }

    //For additional information, see: https://docs.oracle
    // .com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor
    // .html#afterExecute(java.lang.Runnable,%20java.lang.Throwable)

    // Handle JDK-8071638
    if (t == null && r instanceof Future<?> && ((Future<?>) r).isDone()) {
      try {
        ((Future<?>) r).get();
      } catch (ExecutionException ee) {
        LOG.warn(
            "Execution exception when running task in " + Thread.currentThread()
                .getName());
        t = ee.getCause();
      } catch (InterruptedException ie) {
        LOG.warn("Thread (" + Thread.currentThread() + ") interrupted: ", ie);
        Thread.currentThread().interrupt();
      } catch (Throwable throwable) {
        t = throwable;
      }
    }

    if (t != null) {
      LOG.warn("Caught exception in thread " + Thread
          .currentThread().getName() + ": ", t);
    }
  }

  private ExecutorHelper() {}
}
