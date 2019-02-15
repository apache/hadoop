/*
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

package org.apache.hadoop.fs.s3a.multipart;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link S3Downloader} that downloads the next several parts
 * in parallel.
 * <p>
 * This allows us to exceed the bandwidth of a single TCP socket.
 */
public final class MultipartDownloader implements S3Downloader {
  private static final Logger LOG = LoggerFactory
          .getLogger(MultipartDownloader.class);

  private final long partSize;
  private final ListeningExecutorService downloadExecutorService;
  private final S3Downloader partDownloader;
  private final long chunkSize;
  private final long bufferSize;

  public MultipartDownloader(
          long partSize,
          ListeningExecutorService downloadExecutorService,
          S3Downloader partDownloader,
          long chunkSize,
          long bufferSize) {
    Preconditions.checkArgument(partSize > 0);
    Preconditions.checkArgument(chunkSize > 0);
    Preconditions.checkArgument(bufferSize > 0);
    Preconditions.checkArgument(chunkSize <= bufferSize);
    Preconditions.checkArgument(chunkSize <= partSize);

    this.partSize = partSize;
    this.downloadExecutorService = downloadExecutorService;
    this.partDownloader = partDownloader;
    this.chunkSize = chunkSize;
    this.bufferSize = bufferSize;
  }

  @Override
  public AbortableInputStream download(
          final String bucket,
          final String key,
          long rangeStart,
          long rangeEnd,
          ChangeTracker changeTracker,
          String operation) throws IOException {
    Preconditions.checkArgument(rangeEnd > rangeStart,
            "Range end must be larger than range start");
    Preconditions.checkArgument(rangeStart >= 0,
            "Range end must be non-negative");

    final long size = rangeEnd - rangeStart;
    int numParts = (int) Math.ceil((double) size / partSize);

    final OrderingQueue orderingQueue = new OrderingQueue(
            rangeStart,
            size,
            bufferSize);

    final List<ListenableFuture<?>> parts = new ArrayList<>(numParts);
    final AtomicBoolean isAbort = new AtomicBoolean();

    for (long i = 0; i < numParts; i++) {
      final long partRangeStart = rangeStart + i * partSize;
      final long partRangeEnd = i == numParts - 1 ?
              rangeEnd : partRangeStart + partSize;

      ListenableFuture<?> part = downloadExecutorService.submit(new Runnable() {
        @Override
        public void run() {
          LOG.info("Downloading part {} - {}", partRangeStart, partRangeEnd);
          InputStream inputStream = null;
          try {
            inputStream = new DecideAbortInputStream(
                partDownloader.download(
                    bucket, key, partRangeStart, partRangeEnd,
                    changeTracker, operation), isAbort);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          try {
            long currentOffset = partRangeStart;
            while (currentOffset < partRangeEnd) {
              long bytesLeft = partRangeEnd - currentOffset;
              int bytesToRead = Ints.checkedCast(
                      bytesLeft > chunkSize ? chunkSize : bytesLeft);

              byte[] chunk = new byte[bytesToRead];
              ByteStreams.readFully(inputStream, chunk);

              LOG.debug("Pushing offset: {}", currentOffset);
              orderingQueue.push(currentOffset, chunk);
              currentOffset += chunk.length;
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      });

      parts.add(part);
    }

    for (ListenableFuture<?> partFuture : parts) {
      Futures.addCallback(partFuture, new FutureCallback<Object>() {
        @Override
        public void onSuccess(Object o) {

        }

        @Override
        public void onFailure(Throwable throwable) {
          orderingQueue.closeWithException(new RuntimeException(
                  "Exception caught while downloading part", throwable));
          cancelAllDownloads(parts);
        }
      }, DirectExecutor.INSTANCE);
    }

    return new OrderingQueueInputStream(orderingQueue, new Runnable() {
      @Override
      public void run() {
        isAbort.set(false);
        cancelAllDownloads(parts);
      }
    }, new Runnable() {
      @Override
      public void run() {
        isAbort.set(true);
        cancelAllDownloads(parts);
      }
    });
  }

  private void cancelAllDownloads(List<ListenableFuture<?>> partFutures) {
    for (ListenableFuture<?> partFuture : partFutures) {
      partFuture.cancel(true);
    }
  }

  /**
   * Copied from Guava's MoreExecutors#directExecutor.
   * <p>
   * Had to copy this because directExecutor doesn't exist in Guava 11.0,
   * and sameThreadExecutor from Guava 11.0 doesn't exist in newer versions of
   * Guava.
   */
  private enum DirectExecutor implements Executor {
    INSTANCE;

    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public String toString() {
      return "MultipartDownloader.directExecutor()";
    }
  }
}
