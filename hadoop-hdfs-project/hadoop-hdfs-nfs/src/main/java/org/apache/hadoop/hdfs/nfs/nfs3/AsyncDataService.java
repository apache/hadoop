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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a thread pool to easily schedule async data operations. Current
 * async data operation is write back operation. In the future, we could use it
 * for readahead operations too.
 */
public class AsyncDataService {
  static final Logger LOG = LoggerFactory.getLogger(AsyncDataService.class);

  // ThreadPool core pool size
  private static final int CORE_THREADS_PER_VOLUME = 1;
  // ThreadPool maximum pool size
  private static final int MAXIMUM_THREADS_PER_VOLUME = 4;
  // ThreadPool keep-alive time for threads over core pool size
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;
  private final ThreadGroup threadGroup = new ThreadGroup("async data service");
  private ThreadFactory threadFactory = null;
  private ThreadPoolExecutor executor = null;

  public AsyncDataService() {
    threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(threadGroup, r);
      }
    };

    executor = new ThreadPoolExecutor(CORE_THREADS_PER_VOLUME,
        MAXIMUM_THREADS_PER_VOLUME, THREADS_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory);

    // This can reduce the number of running threads
    executor.allowCoreThreadTimeOut(true);
  }

  /**
   * Execute the task sometime in the future.
   */
  synchronized void execute(Runnable task) {
    if (executor == null) {
      throw new RuntimeException("AsyncDataService is already shutdown");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current active thread number: " + executor.getActiveCount()
          + " queue size: " + executor.getQueue().size()
          + " scheduled task number: " + executor.getTaskCount());
    }
    executor.execute(task);
  }

  /**
   * Gracefully shut down the ThreadPool. Will wait for all data tasks to
   * finish.
   */
  synchronized void shutdown() {
    if (executor == null) {
      LOG.warn("AsyncDataService has already shut down.");
    } else {
      LOG.info("Shutting down all async data service threads...");
      executor.shutdown();

      // clear the executor so that calling execute again will fail.
      executor = null;
      LOG.info("All async data service threads have been shut down");
    }
  }

  /**
   * Write the data to HDFS asynchronously
   */
  void writeAsync(OpenFileCtx openFileCtx) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scheduling write back task for fileId: "
          + openFileCtx.getLatestAttr().getFileId());
    }
    WriteBackTask wbTask = new WriteBackTask(openFileCtx);
    execute(wbTask);
  }

  /**
   * A task to write data back to HDFS for a file. Since only one thread can
   * write to a file, there should only be one task at any time for a file
   * (in queue or executing), and this should be guaranteed by the caller.
   */
  static class WriteBackTask implements Runnable {

    OpenFileCtx openFileCtx;

    WriteBackTask(OpenFileCtx openFileCtx) {
      this.openFileCtx = openFileCtx;
    }

    OpenFileCtx getOpenFileCtx() {
      return openFileCtx;
    }

    @Override
    public String toString() {
      // Called in AsyncDataService.execute for displaying error messages.
      return "write back data for fileId"
          + openFileCtx.getLatestAttr().getFileId() + " with nextOffset "
          + openFileCtx.getNextOffset();
    }

    @Override
    public void run() {
      try {
        openFileCtx.executeWriteBack();
      } catch (Throwable t) {
        LOG.error("Async data service got error: ", t);
      }
    }
  }
}
