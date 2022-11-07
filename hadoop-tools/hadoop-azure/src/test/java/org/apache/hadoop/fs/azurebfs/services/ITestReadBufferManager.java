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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.io.IOUtils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_BLOCK_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticGauge;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_PREFETCH_OPERATIONS;
import static org.apache.hadoop.test.LambdaTestUtils.eventually;

public class ITestReadBufferManager extends AbstractAbfsIntegrationTest {

  /**
   * Time before the JUnit test times out for eventually() clauses
   * to fail. This copes with slow network connections and debugging
   * sessions, yet still allows for tests to fail with meaningful
   * messages.
   */
  public static final int TIMEOUT_OFFSET = 5 * 60_000;

  /**
   * Interval between eventually preobes.
   */
  public static final int PROBE_INTERVAL_MILLIS = 1_000;

  public ITestReadBufferManager() throws Exception {
    }

    @Test
    public void testPurgeBufferManagerForParallelStreams() throws Exception {
        describe("Testing purging of buffers from ReadBufferManager for "
                + "parallel input streams");
        final int numBuffers = 16;
        final LinkedList<Integer> freeList = new LinkedList<>();
        for (int i=0; i < numBuffers; i++) {
            freeList.add(i);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        AzureBlobFileSystem fs = getABFSWithReadAheadConfig();
        try {
            for (int i = 0; i < 4; i++) {
                final String fileName = methodName.getMethodName() + i;
                executorService.submit((Callable<Void>) () -> {
                    byte[] fileContent = getRandomBytesArray(ONE_MB);
                    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
                    try (FSDataInputStream iStream = fs.open(testFilePath)) {
                        iStream.read();
                    }
                    return null;
                });
            }
        } finally {
            executorService.shutdown();
            // wait for all tasks to finish
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }

        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        // verify there is no work in progress or the readahead queue.
        assertListEmpty("InProgressList", bufferManager.getInProgressCopiedList());
        assertListEmpty("ReadAheadQueue", bufferManager.getReadAheadQueueCopy());
    }

    private void assertListEmpty(String listName, List<ReadBuffer> list) {
        Assertions.assertThat(list)
                .describedAs("After closing all streams %s should be empty", listName)
                .hasSize(0);
    }

    @Test
    public void testPurgeBufferManagerForSequentialStream() throws Exception {
      describe("Testing purging of buffers in ReadBufferManager for "
          + "sequential input streams");
      AzureBlobFileSystem fs = getABFSWithReadAheadConfig();
      final String fileName = methodName.getMethodName();
      byte[] fileContent = getRandomBytesArray(ONE_MB);
      Path testFilePath = createFileWithContent(fs, fileName, fileContent);

      AbfsInputStream iStream1 = null;
        // stream1 will be closed right away.
      AbfsInputStream iStream2 = null;
      try {
        iStream1 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
        // Just reading one byte will trigger all read ahead calls.
        iStream1.read();
        assertThatStatisticGauge(iStream1.getIOStatistics(), STREAM_READ_ACTIVE_PREFETCH_OPERATIONS)
            .isGreaterThan(0);
      } finally {
        IOUtils.closeStream(iStream1);
      }
      ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();

      try {
        iStream2 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
        iStream2.read();
        // After closing stream1, none of the buffers associated with stream1 should be present.
        AbfsInputStream s1 = iStream1;
        eventually(getTestTimeoutMillis() - TIMEOUT_OFFSET, PROBE_INTERVAL_MILLIS, () ->
            assertListDoesnotContainBuffersForIstream("InProgressList",
                bufferManager.getInProgressCopiedList(), s1));
        assertListDoesnotContainBuffersForIstream("CompletedList",
            bufferManager.getCompletedReadListCopy(), iStream1);
        assertListDoesnotContainBuffersForIstream("ReadAheadQueue",
            bufferManager.getReadAheadQueueCopy(), iStream1);
      } finally {
        // closing the stream later.
        IOUtils.closeStream(iStream2);
      }
      // After closing stream2, none of the buffers associated with stream2 should be present.
      IOStatisticsStore stream2IOStatistics = iStream2.getIOStatistics();
      AbfsInputStream s2 = iStream2;
      eventually(getTestTimeoutMillis() - TIMEOUT_OFFSET, PROBE_INTERVAL_MILLIS, () ->
          assertListDoesnotContainBuffersForIstream("InProgressList",
              bufferManager.getInProgressCopiedList(), s2));
      // no in progress reads in the stats
      assertThatStatisticGauge(stream2IOStatistics, STREAM_READ_ACTIVE_PREFETCH_OPERATIONS)
          .isEqualTo(0);

      assertListDoesnotContainBuffersForIstream("CompletedList",
          bufferManager.getCompletedReadListCopy(), iStream2);
      assertListDoesnotContainBuffersForIstream("ReadAheadQueue",
          bufferManager.getReadAheadQueueCopy(), iStream2);

        // After closing both the streams, all lists should be empty.
      eventually(getTestTimeoutMillis() - TIMEOUT_OFFSET, PROBE_INTERVAL_MILLIS, () ->
          assertListEmpty("InProgressList", bufferManager.getInProgressCopiedList()));
      assertListEmpty("CompletedList", bufferManager.getCompletedReadListCopy());
      assertListEmpty("ReadAheadQueue", bufferManager.getReadAheadQueueCopy());
    }


    private void assertListDoesnotContainBuffersForIstream(String name,
        List<ReadBuffer> list, AbfsInputStream inputStream) {
      Assertions.assertThat(list)
          .describedAs("list %s contains entries for closed stream %s",
              name, inputStream)
          .filteredOn(b -> b.getStream() == inputStream)
          .isEmpty();
    }

    private void assertListContainBuffersForIstream(List<ReadBuffer> list,
        AbfsInputStream inputStream) {
      Assertions.assertThat(list)
          .describedAs("buffer expected to contain closed stream %s", inputStream )
          .filteredOn(b -> b.getStream() == inputStream)
          .isNotEmpty();
    }

  /**
   * Does a list contain a read buffer for stream?
   * @param list list to scan
   * @param inputStream stream to look for
   * @return true if there is at least one reference in the list.
   */
    boolean listContainsStreamRead(List<ReadBuffer> list,
            AbfsInputStream inputStream) {
      return list.stream()
          .filter(b -> b.getStream() == inputStream)
          .count() > 0;
    }


    private AzureBlobFileSystem getABFSWithReadAheadConfig() throws Exception {
        Configuration conf = getRawConfiguration();
        conf.setBoolean(FS_AZURE_ENABLE_READAHEAD, true);
        conf.setLong(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, 8);
        conf.setInt(AZURE_READ_BUFFER_SIZE, MIN_BUFFER_SIZE);
        conf.setInt(FS_AZURE_READ_AHEAD_BLOCK_SIZE, MIN_BUFFER_SIZE);
        return (AzureBlobFileSystem) FileSystem.newInstance(conf);
    }

    protected byte[] getRandomBytesArray(int length) {
        final byte[] b = new byte[length];
        new Random().nextBytes(b);
        return b;
    }

    protected Path createFileWithContent(FileSystem fs, String fileName,
                                         byte[] fileContent) throws IOException {
        Path testFilePath = path(fileName);
        try (FSDataOutputStream oStream = fs.create(testFilePath)) {
            oStream.write(fileContent);
            oStream.flush();
        }
        return testFilePath;
    }
}
