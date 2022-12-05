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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class AbfsListStatusRemoteIterator
    implements RemoteIterator<FileStatus> {

  private static final Logger LOG = LoggerFactory
      .getLogger(AbfsListStatusRemoteIterator.class);

  private static final boolean FETCH_ALL_FALSE = false;
  private static final int MAX_QUEUE_SIZE = 10;
  private static final long POLL_WAIT_TIME_IN_MS = 250;

  private final Path path;
  private final ListingSupport listingSupport;
  private final ArrayBlockingQueue<AbfsListResult> listResultQueue;
  private final TracingContext tracingContext;

  private volatile boolean isAsyncInProgress = false;
  private boolean isIterationComplete = false;
  private String continuation;
  private Iterator<FileStatus> currIterator;

  public AbfsListStatusRemoteIterator(final Path path,
      final ListingSupport listingSupport, TracingContext tracingContext)
      throws IOException {
    this.path = path;
    this.listingSupport = listingSupport;
    this.tracingContext = tracingContext;
    listResultQueue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE);
    currIterator = Collections.emptyIterator();
    addNextBatchIteratorToQueue();
    fetchBatchesAsync();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currIterator.hasNext()) {
      return true;
    }
    currIterator = getNextIterator();
    return currIterator.hasNext();
  }

  @Override
  public FileStatus next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return currIterator.next();
  }

  private Iterator<FileStatus> getNextIterator() throws IOException {
    fetchBatchesAsync();
    try {
      AbfsListResult listResult = null;
      while (listResult == null
          && (!isIterationComplete || !listResultQueue.isEmpty())) {
        listResult = listResultQueue.poll(POLL_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS);
      }
      if (listResult == null) {
        return Collections.emptyIterator();
      } else if (listResult.isFailedListing()) {
        throw listResult.getListingException();
      } else {
        return listResult.getFileStatusIterator();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Thread got interrupted: {}", e);
      throw new IOException(e);
    }
  }

  private void fetchBatchesAsync() {
    if (isAsyncInProgress || isIterationComplete) {
      return;
    }
    synchronized (this) {
      if (isAsyncInProgress || isIterationComplete) {
        return;
      }
      isAsyncInProgress = true;
    }
    CompletableFuture.runAsync(() -> asyncOp());
  }

  private void asyncOp() {
    try {
      while (!isIterationComplete && listResultQueue.size() <= MAX_QUEUE_SIZE) {
        addNextBatchIteratorToQueue();
      }
    } catch (IOException ioe) {
      LOG.error("Fetching filestatuses failed", ioe);
      try {
        listResultQueue.put(new AbfsListResult(ioe));
      } catch (InterruptedException interruptedException) {
        Thread.currentThread().interrupt();
        LOG.error("Thread got interrupted: {}", interruptedException);
      }
    } finally {
      synchronized (this) {
        isAsyncInProgress = false;
      }
    }
  }

  private synchronized void addNextBatchIteratorToQueue()
      throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    try {
      try {
        continuation = listingSupport.listStatus(path, null, fileStatuses,
            FETCH_ALL_FALSE, continuation, tracingContext);
      } catch (AbfsRestOperationException ex) {
        AzureBlobFileSystem.checkException(path, ex);
      }
      if (!fileStatuses.isEmpty()) {
        listResultQueue.put(new AbfsListResult(fileStatuses.iterator()));
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.error("Thread interrupted", ie);
    }
    if (continuation == null || continuation.isEmpty()) {
      isIterationComplete = true;
    }
  }

}
