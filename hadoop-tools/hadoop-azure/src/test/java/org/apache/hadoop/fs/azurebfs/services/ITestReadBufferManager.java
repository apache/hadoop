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
        }

        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        assertListEmpty("CompletedList", bufferManager.getCompletedReadListCopy());
        assertListEmpty("InProgressList", bufferManager.getInProgressCopiedList());
        assertListEmpty("ReadAheadQueue", bufferManager.getReadAheadQueueCopy());
        Assertions.assertThat(bufferManager.getFreeListCopy())
                .describedAs("After closing all streams free list contents should match with " + freeList)
                .hasSize(numBuffers)
                .containsExactlyInAnyOrderElementsOf(freeList);

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

        AbfsInputStream iStream1 =  null;
        // stream1 will be closed right away.
        try {
            iStream1 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
            // Just reading one byte will trigger all read ahead calls.
            iStream1.read();
        } finally {
            IOUtils.closeStream(iStream1);
        }
        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        AbfsInputStream iStream2 = null;
        try {
            iStream2 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
            iStream2.read();
            // After closing stream1, none of the buffers associated with stream1 should be present.
            assertListDoesnotContainBuffersForIstream(bufferManager.getInProgressCopiedList(), iStream1);
            assertListDoesnotContainBuffersForIstream(bufferManager.getCompletedReadListCopy(), iStream1);
            assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream1);
        } finally {
            // closing the stream later.
            IOUtils.closeStream(iStream2);
        }
        // After closing stream2, none of the buffers associated with stream2 should be present.
        assertListDoesnotContainBuffersForIstream(bufferManager.getInProgressCopiedList(), iStream2);
        assertListDoesnotContainBuffersForIstream(bufferManager.getCompletedReadListCopy(), iStream2);
        assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream2);

        // After closing both the streams, all lists should be empty.
        assertListEmpty("CompletedList", bufferManager.getCompletedReadListCopy());
        assertListEmpty("InProgressList", bufferManager.getInProgressCopiedList());
        assertListEmpty("ReadAheadQueue", bufferManager.getReadAheadQueueCopy());

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
