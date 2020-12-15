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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.ContentSummary;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ContentSummaryProcessor {
  private final AtomicLong fileCount = new AtomicLong(0L);
  private final AtomicLong directoryCount = new AtomicLong(0L);
  private final AtomicLong totalBytes = new AtomicLong(0L);
  private final ProcessingQueue<FileStatus> queue = new ProcessingQueue<>();
  private final AzureBlobFileSystemStore abfsStore;
  private static final int NUM_THREADS = 16;

  public ContentSummaryProcessor(AzureBlobFileSystemStore abfsStore) {
    this.abfsStore = abfsStore;
  }

  public ContentSummary getContentSummary(Path path) throws IOException {
    processDirectoryTree(path);
    Thread[] threads = new Thread[16];

    for (int i = 0; i < NUM_THREADS; ++i) {
      threads[i] = new Thread(new ContentSummaryProcessor.ThreadProcessor());
      threads[i].start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return new ContentSummary(totalBytes.get(), directoryCount.get(),
        fileCount.get(), totalBytes.get());
  }

  private void processDirectoryTree(Path path) throws IOException {
    FileStatus[] fileStatuses = abfsStore.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        this.processDirectory();
        this.queue.add(fileStatus);
      } else {
        this.processFile(fileStatus);
      }
    }
  }

  private void processDirectory() {
    this.directoryCount.incrementAndGet();
  }

  private void processFile(FileStatus fileStatus) {
    this.fileCount.incrementAndGet();
    this.totalBytes.addAndGet(fileStatus.getLen());
  }

  private class ThreadProcessor implements Runnable {
    private ThreadProcessor() {
    }

    public void run() {
      try {
        FileStatus fileStatus;
        while ((fileStatus = ContentSummaryProcessor.this.queue.poll())
            != null) {
          if (fileStatus.isDirectory()) {
            ContentSummaryProcessor.this
                .processDirectoryTree(fileStatus.getPath());
          }
          ContentSummaryProcessor.this.queue.unregister();
        }
      } catch (IOException e) {
        throw new RuntimeException("IOException processing Directory tree", e);
      }
    }
  }
}
