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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class AbfsListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsListStatusRemoteIterator.class);

  private static final boolean FETCH_ALL_FALSE = false;
  private static final int MAX_QUEUE_SIZE = 10;

  private final Path path;
  private final ListingSupport listingSupport;
  private final ArrayBlockingQueue<Iterator<FileStatus>> iteratorsQueue;
  private final Object asyncOpLock = new Object();

  private volatile boolean isAsyncInProgress = false;
  private boolean firstBatch = true;
  private String continuation;
  private Iterator<FileStatus> currIterator;
  private IOException ioException;

  public AbfsListStatusRemoteIterator(final Path path,
      final ListingSupport listingSupport) {
    this.path = path;
    this.listingSupport = listingSupport;
    iteratorsQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    currIterator = Collections.emptyIterator();
    fetchBatchesAsync();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currIterator.hasNext()) {
      return true;
    }
    updateCurrentIterator();
    return currIterator.hasNext();
  }

  @Override
  public FileStatus next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return currIterator.next();
  }

  private void updateCurrentIterator() throws IOException {
    fetchBatchesAsync();
    synchronized (this) {
      if (iteratorsQueue.isEmpty()) {
        if (ioException != null) {
          throw ioException;
        }
        if (isListingComplete()) {
          return;
        }
      }
    }
    try {
      currIterator = iteratorsQueue.take();
      if (!currIterator.hasNext() && !isListingComplete()) {
        updateCurrentIterator();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Thread got interrupted: {}", e);
    }
  }

  private synchronized boolean isListingComplete() {
    return !firstBatch && (continuation == null || continuation.isEmpty());
  }

  private void fetchBatchesAsync() {
    if (isAsyncInProgress) {
      return;
    }
    synchronized (asyncOpLock) {
      if (isAsyncInProgress) {
        return;
      }
      isAsyncInProgress = true;
    }
    CompletableFuture.runAsync(() -> asyncOp());
  }

  private void asyncOp() {
    try {
      while (!isListingComplete() && iteratorsQueue.size() <= MAX_QUEUE_SIZE) {
        addNextBatchIteratorToQueue();
      }
    } catch (IOException e) {
      ioException = e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Thread got interrupted: {}", e);
    } finally {
      synchronized (asyncOpLock) {
        try {
          iteratorsQueue.put(Collections.emptyIterator());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Thread got interrupted: {}", e);
        }
        isAsyncInProgress = false;
      }
    }
  }

  private synchronized void addNextBatchIteratorToQueue()
      throws IOException, InterruptedException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    continuation = listingSupport
        .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE, continuation);
    iteratorsQueue.put(fileStatuses.iterator());
    if (firstBatch) {
      firstBatch = false;
    }
  }

}
