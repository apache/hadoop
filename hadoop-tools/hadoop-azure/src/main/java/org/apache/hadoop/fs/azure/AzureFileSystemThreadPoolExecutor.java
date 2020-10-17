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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

class AzureFileSystemThreadPoolExecutor {

  public static final Logger LOG = LoggerFactory.getLogger(AzureFileSystemThreadPoolExecutor.class);

  /*
   * Number of threads to keep in the pool.
   */
  private int threadCount;

  /*
   * Prefix to be used for naming threads.
   */
  private String threadNamePrefix;

  /*
   * File system operation like delete/rename. Used for logging purpose.
   */
  private String operation;

  /*
   * Source blob key used for file operation. Used for logging purpose.
   */
  private String key;

  /*
   * Configuration name for recommendations. Used for logging purpose.
   */
  private String config;

  /**
   * Creates a new AzureFileSystemThreadPoolExecutor object.
   *
   * @param threadCount
   *        Number of threads to be used after reading user configuration.
   * @param threadNamePrefix
   *        Prefix to be used to name threads for the file operation.
   * @param operation
   *        File system operation like delete/rename. Used for logging purpose.
   * @param key
   *        Source blob key used for file operation. Used for logging purpose.
   * @param config
   *        Configuration name for recommendations. Used for logging purpose.
   */
  public AzureFileSystemThreadPoolExecutor(int threadCount, String threadNamePrefix,
      String operation, String key, String config) {
    this.threadCount = threadCount;
    this.threadNamePrefix = threadNamePrefix;
    this.operation = operation;
    this.key = key;
    this.config = config;
  }

  /**
   * Gets a new thread pool
   * @param threadCount
   *        Number of threads to keep in the pool.
   * @param threadNamePrefix
   *        Prefix to be used for naming threads.
   *
   * @return
   *        Returns a new thread pool.
   */
  @VisibleForTesting
  ThreadPoolExecutor getThreadPool(int threadCount) throws Exception {
    return new ThreadPoolExecutor(threadCount, threadCount, 2, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(), new AzureFileSystemThreadFactory(this.threadNamePrefix));
  }

  /**
   * Execute the file operation parallel using threads. All threads works on a
   * single working set of files stored in input 'contents'. The synchronization
   * between multiple threads is achieved through retrieving atomic index value
   * from the array. Once thread gets the index, it retrieves the file and initiates
   * the file operation. The advantage with this method is that file operations
   * doesn't get serialized due to any thread. Also, the input copy is not changed
   * such that caller can reuse the list for other purposes.
   *
   * This implementation also considers that failure of operation on single file
   * is considered as overall operation failure. All threads bail out their execution
   * as soon as they detect any single thread either got exception or operation is failed.
   *
   * @param contents
   *        List of blobs on which operation to be done.
   * @param threadOperation
   *        The actual operation to be executed by each thread on a file.
   *
   * @param operationStatus
   *        Returns true if the operation is success, false if operation is failed.
   * @throws IOException
   *
   */
  boolean executeParallel(FileMetadata[] contents, AzureFileSystemThreadTask threadOperation) throws IOException {

    boolean operationStatus = false;
    boolean threadsEnabled = false;
    int threadCount = this.threadCount;
    ThreadPoolExecutor ioThreadPool = null;

    // Start time for file operation
    long start = Time.monotonicNow();

    // If number of files  are less then reduce threads to file count.
    threadCount = Math.min(contents.length, threadCount);

    if (threadCount > 1) {
      try {
        ioThreadPool = getThreadPool(threadCount);
        threadsEnabled = true;
      } catch(Exception e) {
        // The possibility of this scenario is very remote. Added this code as safety net.
        LOG.warn("Failed to create thread pool with threads {} for operation {} on blob {}."
            + " Use config {} to set less number of threads. Setting config value to <= 1 will disable threads.",
            threadCount, operation, key, config);
      }
    } else {
      LOG.warn("Disabling threads for {} operation as thread count {} is <= 1", operation, threadCount);
    }

    if (threadsEnabled) {
      LOG.debug("Using thread pool for {} operation with threads {}", operation, threadCount);
      boolean started = false;
      AzureFileSystemThreadRunnable runnable = new AzureFileSystemThreadRunnable(contents, threadOperation, operation);

      // Don't start any new requests if there is an exception from any one thread.
      for (int i = 0; i < threadCount && runnable.lastException == null && runnable.operationStatus; i++)
      {
        try {
          ioThreadPool.execute(runnable);
          started = true;
        } catch (RejectedExecutionException ex) {
          // If threads can't be scheduled then report error and move ahead with next thread.
          // Don't fail operation due to this issue.
          LOG.error("Rejected execution of thread for {} operation on blob {}."
              + " Continuing with existing threads. Use config {} to set less number of threads"
              + " to avoid this error", operation, key, config);
        }
      }

      // Stop accepting any new execute requests.
      ioThreadPool.shutdown();

      try {
        // Wait for threads to terminate. Keep time out as large value
        ioThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch(InterruptedException intrEx) {
        // If current thread got interrupted then shutdown all threads now.
        ioThreadPool.shutdownNow();

        // Restore the interrupted status
        Thread.currentThread().interrupt();
        LOG.error("Threads got interrupted {} blob operation for {} "
            , operation, key);
      }

      int threadsNotUsed = threadCount - runnable.threadsUsed.get();
      if (threadsNotUsed > 0) {
        LOG.warn("{} threads not used for {} operation on blob {}", threadsNotUsed, operation, key);
      }

      if (!started) {
        // No threads started. Fall back to serial mode.
        threadsEnabled = false;
        LOG.info("Not able to schedule threads to {} blob {}. Fall back to {} blob serially."
            , operation, key, operation);
      } else {
        IOException lastException = runnable.lastException;

        // There are no exceptions from threads and no operation failures. Consider this scenario
        // as failure only if file operations are not done on all files.
        if (lastException == null && runnable.operationStatus && runnable.filesProcessed.get() < contents.length) {
          LOG.error("{} failed as operation on subfolders and files failed.", operation);
          lastException = new IOException(operation + " failed as operation on subfolders and files failed.");
        }

        if (lastException != null) {
          // Threads started and executed. One or more threads seems to have hit exception.
          // Raise the same exception.
          throw lastException;
        }

        operationStatus = runnable.operationStatus;
      }
    }

    if (!threadsEnabled) {
      // No threads. Serialize the operation. Clear any last exceptions.
      LOG.debug("Serializing the {} operation", operation);
      for (int i = 0; i < contents.length; i++) {
        if (!threadOperation.execute(contents[i])) {
          LOG.warn("Failed to {} file {}", operation, contents[i]);
          return false;
        }
      }

      // Operation is success
      operationStatus = true;
    }

    // Find the duration of time taken for file operation
    long end = Time.monotonicNow();
    LOG.info("Time taken for {} operation is: {} ms with threads: {}", operation, (end - start), threadCount);

    return operationStatus;
  }

  /**
   * A ThreadFactory for Azure File operation threads with meaningful names helpful
   * for debugging purposes.
   */
  static class AzureFileSystemThreadFactory implements ThreadFactory {

    private String threadIdPrefix = "AzureFileSystemThread";

    /**
     * Atomic integer to provide thread id for thread names.
     */
    private AtomicInteger threadSequenceNumber = new AtomicInteger(0);

    public AzureFileSystemThreadFactory(String prefix) {
      threadIdPrefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);

      // Use current thread name as part in naming thread such that use of
      // same file system object will have unique names.
      t.setName(String.format("%s-%s-%d", threadIdPrefix, Thread.currentThread().getName(),
          threadSequenceNumber.getAndIncrement()));
      return t;
    }

  }

  static class AzureFileSystemThreadRunnable implements Runnable {

    // Tracks if any thread has raised exception.
    private volatile IOException lastException = null;

    // Tracks if any thread has failed execution.
    private volatile boolean operationStatus = true;

    // Atomic tracker to retrieve index of next file to be processed
    private AtomicInteger fileIndex = new AtomicInteger(0);

    // Atomic tracker to count number of files successfully processed
    private AtomicInteger filesProcessed = new AtomicInteger(0);

    // Atomic tracker to retrieve number of threads used to do at least one file operation.
    private AtomicInteger threadsUsed = new AtomicInteger(0);

    // Type of file system operation
    private String operation = "Unknown";

    // List of files to be processed.
    private final FileMetadata[] files;

    // Thread task which encapsulates the file system operation work on a file.
    private AzureFileSystemThreadTask task;

    public AzureFileSystemThreadRunnable(final FileMetadata[] files,
        AzureFileSystemThreadTask task, String operation) {
      this.operation = operation;
      this.files = files;
      this.task = task;
    }

    @Override
    public void run() {
      long start = Time.monotonicNow();
      int currentIndex;
      int processedFilesCount = 0;

      while ((currentIndex = fileIndex.getAndIncrement()) < files.length) {
        processedFilesCount++;
        FileMetadata file = files[currentIndex];

        try {
          // Execute the file operation.
          if (!task.execute(file)) {
            LOG.error("{} operation failed for file {}",
                this.operation, file.getKey());
            operationStatus = false;
          } else {
            filesProcessed.getAndIncrement();
          }
        } catch (Exception e) {
          LOG.error("Encountered Exception for {} operation for file {}",
              this.operation, file.getKey());
          lastException = new IOException("Encountered Exception for "
              + this.operation + " operation for file " + file.getKey(), e);
        }

        // If any thread has seen exception or operation failed then we
        // don't have to process further.
        if (lastException != null || !operationStatus) {
          LOG.warn("Terminating execution of {} operation now as some other thread"
              + " already got exception or operation failed", this.operation, file.getKey());
          break;
        }
      }

      long end = Time.monotonicNow();
      LOG.debug("Time taken to process {} files count for {} operation: {} ms",
          processedFilesCount, this.operation, (end - start));
      if (processedFilesCount > 0) {
        threadsUsed.getAndIncrement();
      }
    }
  }
}
