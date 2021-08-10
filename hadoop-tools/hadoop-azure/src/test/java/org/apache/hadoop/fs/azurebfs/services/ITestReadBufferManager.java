package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;

public class ITestReadBufferManager extends AbstractAbfsIntegrationTest {

    public ITestReadBufferManager() throws Exception {
    }

    @Test
    public void testPurgeBufferManagerForParallelStreams() throws Exception {
        describe("Testing purging of buffers from ReadBufferManager for " +
                "parallel input streams");
        final int numBuffers = 16;
        final LinkedList<Integer> freeList = new LinkedList<>();
        for (int i=0; i < numBuffers; i++) {
            freeList.add(i);
        }
        AzureBlobFileSystem fs = getABFSWithReadAheadConfig();
        for(int i=0; i < 4; i++) {
            String fileName = methodName.getMethodName() + i;
            byte[] fileContent = getRandomBytesArray(ONE_MB);
            Path testFilePath = createFileWithContent(fs, fileName, fileContent);
            try (FSDataInputStream iStream = fs.open(testFilePath)) {
                iStream.read();
            }
        }
        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        Assertions.assertThat(bufferManager.getCompletedReadListCopy().size())
                .describedAs("After closing all streams completed list size should be 0")
                .isEqualTo(0);

        Assertions.assertThat(bufferManager.getInProgressCopiedList().size())
                .describedAs("After closing all streams inProgress list size should be 0")
                .isEqualTo(0);
        Assertions.assertThat(bufferManager.getFreeListCopy())
                .describedAs("After closing all streams free list contents should match with " + freeList)
                .hasSize(numBuffers)
                .containsExactlyInAnyOrderElementsOf(freeList);
        Assertions.assertThat(bufferManager.getReadAheadQueueCopy())
                .describedAs("After closing all stream ReadAheadQueue should be empty")
                .hasSize(0);

    }

    @Test
    public void testPurgeBufferManagerForSequentialStream() throws Exception {
        describe("Testing purging of buffers in ReadBufferManager for " +
                "sequential input streams");
        AzureBlobFileSystem fs = getABFSWithReadAheadConfig();
        final String fileName = methodName.getMethodName();
        byte[] fileContent = getRandomBytesArray(ONE_MB);
        Path testFilePath = createFileWithContent(fs, fileName, fileContent);
        AbfsInputStream iStream1 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
        iStream1.read();
        // closing the stream right away.
        iStream1.close();
        AbfsInputStream iStream2 = (AbfsInputStream) fs.open(testFilePath).getWrappedStream();
        iStream2.read();
        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        assertListDoesnotContainBuffersForIstream(bufferManager.getInProgressCopiedList(), iStream1);
        assertListDoesnotContainBuffersForIstream(bufferManager.getCompletedReadListCopy(), iStream1);
        assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream1);
        // closing the stream later.
        iStream2.close();
        assertListDoesnotContainBuffersForIstream(bufferManager.getInProgressCopiedList(), iStream2);
        assertListDoesnotContainBuffersForIstream(bufferManager.getCompletedReadListCopy(), iStream2);
        assertListDoesnotContainBuffersForIstream(bufferManager.getReadAheadQueueCopy(), iStream2);

    }


    private void assertListDoesnotContainBuffersForIstream(LinkedList<ReadBuffer> list,
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
        return getFileSystem(conf);
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
