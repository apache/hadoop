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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
  private final LinkedBlockingQueue<FileStatus> queue = new LinkedBlockingQueue<>();
  private final Logger LOG =
      LoggerFactory.getLogger(ContentSummaryProcessor.class);
  private final AzureBlobFileSystemStore abfsStore;
  private static final int NUM_THREADS = 16;
  private final AtomicInteger NUM_TASKS = new AtomicInteger(0);
  ExecutorService executorService = new ThreadPoolExecutor(1, NUM_THREADS,
      5, TimeUnit.SECONDS, new SynchronousQueue<>());

  public ContentSummaryProcessor(AzureBlobFileSystemStore abfsStore) {
    this.abfsStore = abfsStore;
  }

  public ABFSContentSummary getContentSummary(Path path)
      throws IOException, InterruptedException {
    processDirectoryTree(path);

    while(!queue.isEmpty() || NUM_TASKS.get() > 0) {
      Thread.sleep(10);
    }
    executorService.shutdown();
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
        synchronized (this) {
          if (!queue.isEmpty() && NUM_TASKS.get() < NUM_THREADS) {
            NUM_TASKS.incrementAndGet();
            executorService.submit(() -> {
              try {
                FileStatus fileStatus1;
                while ((fileStatus1 = queue.poll(100, TimeUnit.MILLISECONDS))
                    != null) {
                  processDirectoryTree(fileStatus1.getPath());
                }
                NUM_TASKS.decrementAndGet();
              } catch (IOException | InterruptedException e) {
                NUM_TASKS.decrementAndGet();
                LOG.debug(e.toString());
                throw new IOException(e.getMessage());
              }
              return null;
            });
          }
        }
      }else {
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
}
