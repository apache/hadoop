/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.ABFSContentSummary;

public class ContentSummaryProcessor implements AutoCloseable {
  private final AtomicLong fileCount = new AtomicLong(0L);
  private final AtomicLong directoryCount = new AtomicLong(0L);
  private final AtomicLong totalBytes = new AtomicLong(0L);
  private final AtomicInteger numTasks = new AtomicInteger(0);
  private final AzureBlobFileSystemStore abfsStore;
  private static final int NUM_THREADS = 16;
  private final ExecutorService executorService = new ThreadPoolExecutor(1,
      NUM_THREADS, 5, TimeUnit.SECONDS, new SynchronousQueue<>());
  private final CompletionService<Void> completionService = new ExecutorCompletionService<>(
      executorService);
  private final LinkedBlockingQueue<FileStatus> queue = new LinkedBlockingQueue<>();
  private final Set<Future<Void>> futures =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  private static final Logger LOG =
      LoggerFactory.getLogger(ContentSummaryProcessor.class);
  private static final int POLL_TIMEOUT = 100;
  private Listener listener = null;

  public ContentSummaryProcessor(AzureBlobFileSystemStore abfsStore) {
    this.abfsStore = abfsStore;
  }

  public ABFSContentSummary getContentSummary(Path path)
      throws IOException, InterruptedException {
    processDirectoryTree(path);

    try {
      while (!queue.isEmpty() || numTasks.get() > 0
          || ((ThreadPoolExecutor) executorService).getActiveCount() > 0) {
        numTasks.decrementAndGet();
        completionService.take().get();
        if (listener != null) {
          listener.checkInterrupt();
        }
      }
    } catch (ExecutionException e) {
      LOG.debug(e.getMessage());
      throw new IOException(e);
    } finally {
      executorService.shutdown();
      if (listener != null) {
        listener.checkShutdown(((ThreadPoolExecutor)executorService).getActiveCount());
      }
    }

//    close();
    executorService.shutdownNow();
    if (listener != null) {
      // statement reachable only when no exceptions thrown by threads
      listener.checkAllTasksComplete(numTasks,
          ((ThreadPoolExecutor)executorService).getActiveCount());
    }

    return new ABFSContentSummary(totalBytes.get(), directoryCount.get(),
        fileCount.get(), totalBytes.get());
  }

  private void processDirectoryTree(Path path)
      throws IOException, InterruptedException {
    FileStatus[] fileStatuses = abfsStore.listStatus(path);
    if (listener != null) {
      synchronized (this) {
        listener.verifyThreadCount(numTasks,
            (ThreadPoolExecutor) executorService, NUM_THREADS);
      }
    }

    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        queue.put(fileStatus);
        processDirectory();
        synchronized (this) {
          if (!queue.isEmpty() && numTasks.get() < NUM_THREADS) {
            numTasks.incrementAndGet();
            Future<Void> future = completionService.submit(() -> {
              FileStatus fileStatus1;
              while ((fileStatus1 = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS))
                  != null) {
                processDirectoryTree(fileStatus1.getPath());
              }
              if (listener != null) {
                synchronized (this) {
                  if (!listener.isInterrupted() && listener.shouldInterrupt()) {
                    listener.setInterrupted();
                    throw new InterruptedException();
                  }
                }
              }
              return null;
            });
            futures.add(future);
          }
        }
      } else {
        processFile(fileStatus);
      }
    }
  }

  private void processDirectory() {
    directoryCount.incrementAndGet();
  }

  private void processFile(FileStatus fileStatus) {
    fileCount.incrementAndGet();
    totalBytes.addAndGet(fileStatus.getLen());
  }

  @Override
  public void close() {
    while (completionService.poll() != null);
    executorService.shutdown();
    if (!executorService.isTerminated()) {
      executorService.shutdownNow();
    }
  }

  @VisibleForTesting
  void registerListener(Listener listener) {
    this.listener = listener;
  }
}
