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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link MultipartDownloader}.
 */
public final class MultipartDownloaderTest {

  private final byte[] bytes = ContractTestUtils.dataset(1000, 0, 1000);

  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));

  private final MultipartDownloader downloader = new MultipartDownloader(
      100,
      executorService,
      new S3Downloader() {
        @Override
        public AbortableInputStream download(
            String bucket, String key, long rangeStart, long rangeEnd,
            ChangeTracker changeTracker, String operation) {
          byte[] selectedBytes = Arrays.copyOfRange(
              MultipartDownloaderTest.this.bytes,
              (int) rangeStart,
              (int) rangeEnd);
          final ByteArrayInputStream byteArrayInputStream =
              new ByteArrayInputStream(selectedBytes);
          return new AbortableInputStream() {
            @Override
            public void abort() {
              try {
                close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public int read() {
              return byteArrayInputStream.read();
            }
          };
        }
      },
      10,
      1000);

  @After
  public void after() {
    executorService.shutdownNow();
  }

  @Test
  public void testDownloadAll() throws IOException {
    assertDownloadRange(0, bytes.length);
  }

  @Test
  public void testDownloadRange() throws IOException {
    assertDownloadRange(10, 20);
  }

  @Test
  public void testDownloadTail() throws IOException {
    assertDownloadRange(bytes.length - 10, bytes.length);
  }

  @Test
  public void testCancelPartsIfFailure()
      throws InterruptedException, IOException {
    final CountDownLatch interrupted = new CountDownLatch(1);
    final CountDownLatch blockForever = new CountDownLatch(1);
    S3Downloader partDownloader = new S3Downloader() {
      @Override
      public AbortableInputStream download(
          String bucket, String key, long rangeStart, long rangeEnd,
          ChangeTracker changeTracker, String operation) {
        if (rangeStart == 0) {
          throw new RuntimeException();
        } else {
          return new AbortableInputStream() {
            @Override
            public void abort() {
              try {
                close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public int read() {
              try {
                blockForever.await();
              } catch (InterruptedException e) {
                interrupted.countDown();
              }
              return -1;
            }
          };
        }
      }
    };
    MultipartDownloader multipartDownloader = new MultipartDownloader(
            100, executorService, partDownloader, 10, 1000);
    multipartDownloader.download("bucket", "key", 0, 1000, null, null);

    assertTrue(interrupted.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCancelPartsIfClosed()
          throws InterruptedException, IOException {
    testCompletion(false);
  }

  @Test
  public void testPropagateAbortIfAborted()
          throws InterruptedException, IOException {
    testCompletion(true);
  }

  private void testCompletion(boolean checkAborted)
          throws IOException, InterruptedException {
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicBoolean aborted = new AtomicBoolean();
    final CountDownLatch interrupted = new CountDownLatch(1);
    final CountDownLatch blockForever = new CountDownLatch(1);

    S3Downloader partDownloader = new S3Downloader() {
      @Override
      public AbortableInputStream download(
          String bucket, String key, long rangeStart, long rangeEnd,
          ChangeTracker changeTracker, String operation) {
        return new AbortableInputStream() {
          @Override
          public void abort() {
            aborted.set(true);
          }

          @Override
          public int read() {
            try {
              blockForever.await();
            } catch (InterruptedException e) {
              interrupted.countDown();
            }
            return -1;
          }

          @Override
          public void close() {
            closed.set(true);
          }
        };
      }
    };
    MultipartDownloader multipartDownloader = new MultipartDownloader(
            100, executorService, partDownloader, 10, 1000);
    AbortableInputStream download =
            multipartDownloader.download("bucket", "key", 0, 1000, null, null);
    if (checkAborted) {
      download.abort();
    } else {
      download.close();
    }

    assertTrue(interrupted.await(1, TimeUnit.SECONDS));

    if (checkAborted) {
      assertFalse(closed.get());
      assertTrue(aborted.get());
    } else {
      assertFalse(aborted.get());
      assertTrue(closed.get());
    }
  }

  private void assertDownloadRange(int from, int to) throws IOException {
    Assert.assertArrayEquals(
            Arrays.copyOfRange(bytes, from, to),
            IOUtils.toByteArray(
                    downloader.download(
                        "bucket",
                        "key",
                        from,
                        to,
                        null,
                        null)));
  }
}