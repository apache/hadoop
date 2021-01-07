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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.ABFSContentSummary;

public class ContentSummaryProcessor {
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
  private static final int POLL_TIMEOUT = 100;

  public ContentSummaryProcessor(AzureBlobFileSystemStore abfsStore) {
    this.abfsStore = abfsStore;
  }

  public ABFSContentSummary getContentSummary(Path path)
          throws IOException, ExecutionException, InterruptedException {

    processDirectoryTree(path);

    try {
      while (!queue.isEmpty() || numTasks.get() > 0) {
        completionService.take().get();
        numTasks.decrementAndGet();
      }
    } finally {
      numTasks.decrementAndGet();
      executorService.shutdown();
    }
    executorService.awaitTermination(1, TimeUnit.SECONDS);

    return new ABFSContentSummary(totalBytes.get(), directoryCount.get(),
        fileCount.get(), totalBytes.get());
  }

  private void processDirectoryTree(Path path)
      throws IOException, InterruptedException {
    FileStatus[] fileStatuses = abfsStore.listStatus(path);

    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        queue.put(fileStatus);
        processDirectory();
        conditionalSubmitTaskToExecutor();
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

  private synchronized void conditionalSubmitTaskToExecutor() {
    if (!queue.isEmpty() && numTasks.get() < NUM_THREADS) {
      numTasks.incrementAndGet();
      completionService.submit(() -> {
        FileStatus fileStatus1;
        while ((fileStatus1 = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS))
                != null) {
          processDirectoryTree(fileStatus1.getPath());
        }
        return null;
      });
    }
  }

}
