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
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

public class ListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final Logger LOG = LoggerFactory
      .getLogger(ListStatusRemoteIterator.class);

  private static final boolean FETCH_ALL_FALSE = false;

  private final Path path;
  private final AzureBlobFileSystemStore abfsStore;
  private final Queue<ListIterator<FileStatus>> iteratorsQueue =
      new LinkedList<>();

  private boolean firstRead = true;
  private String continuation;
  private ListIterator<FileStatus> currIterator;
  private IOException ioException;

  public ListStatusRemoteIterator(final Path path,
      final AzureBlobFileSystemStore abfsStore) throws IOException {
    this.path = path;
    this.abfsStore = abfsStore;
    fetchAllAsync();
    updateCurrentIterator();
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
    synchronized (this) {
      while (!isIterationComplete() && iteratorsQueue.isEmpty()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Thread got interrupted: {}", e);
        }
      }
      if (!iteratorsQueue.isEmpty()) {
        currIterator = iteratorsQueue.poll();
      } else if (ioException != null) {
        throw ioException;
      }
    }
  }

  private void fetchAllAsync() {
    CompletableFuture.supplyAsync(() -> {
      while (!isIterationComplete()) {
        List<FileStatus> fileStatuses = new ArrayList<>();
        try {
          continuation = abfsStore
              .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE,
                  continuation);
        } catch (IOException e) {
          ioException = e;
          return null;
        } finally {
          if (firstRead) {
            firstRead = false;
          }
        }
        if (fileStatuses != null && !fileStatuses.isEmpty()) {
          iteratorsQueue.add(fileStatuses.listIterator());
          synchronized (this) {
            this.notifyAll();
          }
        }
      }
      return null;
    });
  }

  private boolean isIterationComplete() {
    return !firstRead && (continuation == null || continuation.isEmpty());
  }

}
