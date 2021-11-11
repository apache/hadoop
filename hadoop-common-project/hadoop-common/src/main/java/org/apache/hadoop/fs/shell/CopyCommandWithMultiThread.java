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

package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Abstract command to enable sub copy commands run with multi-thread.
 */
public abstract class CopyCommandWithMultiThread
    extends CommandWithDestination {

  private int threadCount = 1;
  private ThreadPoolExecutor executor = null;
  private int threadPoolQueueSize = DEFAULT_QUEUE_SIZE;

  public static final int DEFAULT_QUEUE_SIZE = 1024;
  public static final int MAX_THREAD_COUNT =
      Runtime.getRuntime().availableProcessors() * 2;

  public static final Logger LOG =
      LoggerFactory.getLogger(CopyCommandWithMultiThread.class);

  protected void setThreadCount(String optValue) {
    if (optValue != null) {
      int count = Integer.parseInt(optValue);
      threadCount = count < 1 ? 1 : Math.min(count, MAX_THREAD_COUNT);
    }
  }

  protected void setThreadPoolQueueSize(String optValue) {
    if (optValue != null) {
      int size = Integer.parseInt(optValue);
      if (size < 1) {
        LOG.warn("The value of the thread pool queue size can't be less than "
            + "1, and the default value {} is used here.", DEFAULT_QUEUE_SIZE);
      } else {
        threadPoolQueueSize = size;
      }
    }
  }

  @VisibleForTesting
  protected int getThreadCount() {
    return this.threadCount;
  }

  @VisibleForTesting
  protected int getThreadPoolQueueSize() {
    return this.threadPoolQueueSize;
  }

  @VisibleForTesting
  protected ThreadPoolExecutor getExecutor() {
    return this.executor;
  }

  @Override
  protected void processArguments(LinkedList<PathData> args)
      throws IOException {

    if (isMultiThreadNecessary(args)) {
      initThreadPoolExecutor();
    }

    super.processArguments(args);

    if (executor != null) {
      waitForCompletion();
    }
  }

  // if thread count is 1 or the source is only one single file,
  // don't init executor to avoid threading overhead.
  @VisibleForTesting
  protected boolean isMultiThreadNecessary(LinkedList<PathData> args)
      throws IOException {
    return this.threadCount > 1 && hasMoreThanOneSourcePaths(args);
  }

  // check if source is only one single file.
  private boolean hasMoreThanOneSourcePaths(LinkedList<PathData> args)
      throws IOException {
    if (args.size() > 1) {
      return true;
    }
    if (args.size() == 1) {
      PathData src = args.get(0);
      if (src.stat == null) {
        src.refreshStatus();
      }
      return isPathRecursable(src);
    }
    return false;
  }

  private void initThreadPoolExecutor() {
    executor =
        new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threadPoolQueueSize),
            new ThreadPoolExecutor.CallerRunsPolicy());
  }

  private void waitForCompletion() {
    if (executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        executor.shutdownNow();
        displayError(e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  protected void copyFileToTarget(PathData src, PathData target)
      throws IOException {
    if (executor == null) {
      super.copyFileToTarget(src, target);
    } else {
      executor.submit(() -> {
        try {
          super.copyFileToTarget(src, target);
        } catch (IOException e) {
          displayError(e);
        }
      });
    }
  }
}
