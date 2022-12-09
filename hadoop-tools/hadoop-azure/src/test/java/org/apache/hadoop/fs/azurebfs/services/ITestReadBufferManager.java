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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.io.IOUtils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_BLOCK_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestReadBufferManager extends AbstractAbfsIntegrationTest {

    public ITestReadBufferManager() throws Exception {
    }

    @Test
    public void testPurgeBufferManagerForParallelStreams() throws Exception {
        describe("Testing purging of buffers from ReadBufferManager for "
                + "parallel input streams");
        final long checkExecutionWaitTime = 1_000L;
        final int numBuffers = 16;
        final LinkedList<Integer> freeList = new LinkedList<>();
        for (int i=0; i < numBuffers; i++) {
            freeList.add(i);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        AzureBlobFileSystem fs = getABFSWithReadAheadConfig();
        final Set<ReadBuffer> inProgressBuffers = new HashSet<>();
        final Set<AbfsInputStream> streamsInTest = new HashSet<>();
        final ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        final Boolean[] executionCompletion = new Boolean[4];
        Arrays.fill(executionCompletion, false);
        try {
            for (int i = 0; i < 4; i++) {
                final String fileName = methodName.getMethodName() + i;
                final int iteration = i;
                executorService.submit((Callable<Void>) () -> {
                    byte[] fileContent = getRandomBytesArray(ONE_MB);
                    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
                    try (FSDataInputStream iStream = fs.open(testFilePath)) {
                      streamsInTest.add(
                          (AbfsInputStream) iStream.getWrappedStream());
                      iStream.read();
                      inProgressBuffers.addAll(
                          bufferManager.getInProgressCopiedList());
                    }
                    executionCompletion[iteration] = true;
                    return null;
                });
            }
        } finally {
            executorService.shutdown();
        }

        /*
        * Since, the read from inputStream is happening in parallel thread, the
        * test has to wait for the execution to get over. If we don't wait, test
        * main thread will go on to do assertion where the stream execution may or
        * may not happen.
        */
        while (!checkIfAllExecutionCompleted(executionCompletion)) {
          Thread.sleep(checkExecutionWaitTime);
        }

        /*
        * The close() method of AbfsInputStream would lead to purge of completedList.
        * Because the readBufferWorkers are running in parallel thread, due to race condition,
        * after close and before assert, it can happen that processing of inProgress buffer
        * can get completed and hence we cannot assert on completedList to be empty.
        * That is why completedList are checked to not have a buffer other than the
        * buffers in inProgressQueue just before the closure of AbfsInputStream object.
        */
        assertCompletedListContainsSubSetOfCertainSet(
            bufferManager.getCompletedReadListCopy(), inProgressBuffers,
            streamsInTest);
        for (AbfsInputStream stream : streamsInTest) {
          assertListDoesnotContainBuffersForIstream(
              bufferManager.getReadAheadQueueCopy(), stream);
        }
    }

  private void assertCompletedListContainsSubSetOfCertainSet(final List<ReadBuffer> completedList,
      Set<ReadBuffer> bufferSet, final Set<AbfsInputStream> streamsInTest) {
    for (ReadBuffer buffer : completedList) {
      if (!streamsInTest.contains(buffer.getStream())) {
        return;
      }
      Assertions.assertThat(bufferSet)
          .describedAs(
              "CompletedList contains a buffer which is not part of bufferSet.")
          .contains(buffer);
    }
  }

  private Boolean checkIfAllExecutionCompleted(Boolean[] completionFlagArray) {
    for (Boolean completionFlag : completionFlagArray) {
      if (!completionFlag) {
        return false;
      }
    }
    return true;
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
        Set<ReadBuffer> inProgressBufferSet = new HashSet<>();
        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        final Set<AbfsInputStream> streamsInTest = new HashSet<>();

        AbfsInputStream iStream1 =  null;
        // stream1 will be closed right away.
        try {
            iStream1 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
            streamsInTest.add(iStream1);
            // Just reading one byte will trigger all read ahead calls.
            iStream1.read();
        } finally {
            IOUtils.closeStream(iStream1);
            inProgressBufferSet.addAll(bufferManager.getInProgressCopiedList());
        }
        AbfsInputStream iStream2 = null;
        try {
            iStream2 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
            streamsInTest.add(iStream2);
            iStream2.read();
            // After closing stream1, no queued buffers of stream1 should be present.
            assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream1);
        } finally {
            // closing the stream later.
            IOUtils.closeStream(iStream2);
            inProgressBufferSet.addAll(bufferManager.getInProgressCopiedList());
        }
        //  After closing stream2, no queued buffers of stream2 should be present.
        assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream2);

        /*
        * The close() method of AbfsInputStream would lead to purge of completedList.
        * Because the readBufferWorkers are running in parallel thread, due to race condition,
        * after close and before assert, it can happen that processing of inProgress buffer
        * can get completed and hence we cannot assert on completedList to be empty.
        * That is why completedList are checked to not have a buffer other than the
        * buffers in inProgressQueue just before the closure of AbfsInputStream object.
        */
        assertCompletedListContainsSubSetOfCertainSet(
            bufferManager.getCompletedReadListCopy(), inProgressBufferSet,
            streamsInTest);
    }


    private void assertListDoesnotContainBuffersForIstream(List<ReadBuffer> list,
                                                           AbfsInputStream inputStream) {
        for (ReadBuffer buffer : list) {
            Assertions.assertThat(buffer.getStream())
                    .describedAs("Buffers associated with closed input streams shouldn't be present")
                    .isNotEqualTo(inputStream);
        }
    }

    private AzureBlobFileSystem getABFSWithReadAheadConfig() throws Exception {
        Configuration conf = getRawConfiguration();
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
