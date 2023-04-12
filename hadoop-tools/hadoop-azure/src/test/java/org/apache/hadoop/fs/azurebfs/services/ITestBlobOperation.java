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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_BLOCKID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Class to test create, append and flush over blob endpoint.
 */
public class ITestBlobOperation extends AbstractAbfsIntegrationTest {
    private static final String TEST_PATH = "/testfile";
    AzureBlobFileSystem fs;
    private final Path testPath = new Path("/testfile");

    public ITestBlobOperation() throws Exception {
        super.setup();
        fs = getFileSystem();
        PrefixMode prefixMode = getPrefixMode(fs);
        Assume.assumeTrue(prefixMode == PrefixMode.BLOB);
    }

    /** Generates the blockList xml. */
    private static String generateBlockListXml(List<String> blockIds) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        stringBuilder.append("<BlockList>\n");
        for (String blockId : blockIds) {
            String blockId1 = Base64.getEncoder().encodeToString(blockId.getBytes());
            stringBuilder.append(String.format("<Latest>%s</Latest>\n", blockId1));
        }
        stringBuilder.append("</BlockList>\n");
        return stringBuilder.toString();
    }

    /**
     * Test case to verify that if we do put block when data is null error is thrown.
     * @throws Exception
     */
    @Test
    public void testPutBlockWithNullData() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));

        AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        String blockId = "block1";
        byte[] data = null;
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
        final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);

        String blockId1 = Base64.getEncoder().encodeToString(blockId.getBytes());
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, blockId1);
        URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString()));
        requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, "10"));

        final AbfsRestOperation op = new AbfsRestOperation(
                AbfsRestOperationType.PutBlock,
                testClient,
                HTTP_METHOD_PUT,
                url,
                requestHeaders,
                data, 0, 0, null);

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                "abcde", FSOperationType.APPEND,
                TracingHeaderFormat.ALL_ID_FORMAT, null));

        intercept(IOException.class, () -> op.execute(tracingContext));
    }

    /**
     * Test case to verify that all block id's of a block should be of the same length.
     * @throws Exception
     */
    @Test
    public void testPutBlockWithDifferentLengthBlockIds() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2122",
                "block-312234"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));

        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
        final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);

        for (int i = 0; i < blockIds.size(); i++) {
            String blockId1 = Base64.getEncoder().encodeToString(blockIds.get(i).getBytes());
            abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, blockId1);
            URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString()));
            byte[] data = blockData.get(i);
            requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(data.length)));

            final AbfsRestOperation op = new AbfsRestOperation(
                    AbfsRestOperationType.PutBlock,
                    testClient,
                    HTTP_METHOD_PUT,
                    url,
                    requestHeaders,
                    data, 0, data.length, null);

            TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                    "abcde", FSOperationType.APPEND,
                    TracingHeaderFormat.ALL_ID_FORMAT, null));

            if (i >= 1) {
                intercept(IOException.class, () -> op.execute(tracingContext));
                Assertions.assertThat(op.getResult().getStatusCode())
                        .describedAs("The status code is incorrect")
                        .isEqualTo(HTTP_BAD_REQUEST);
                Assertions.assertThat(op.getResult().getConnResponseMessage())
                        .describedAs("The exception message is incorrect")
                        .isEqualTo("The specified blob or block content is invalid.");
            } else {
                op.execute(tracingContext);
            }
        }
    }

    /**
     * Verify getBlockList after flush returns same list of encoded blockId's sent.
     * @throws IOException
     * @throws IllegalAccessException
     */
    @Test
    public void testGetCommittedBlockList() throws IOException, IllegalAccessException {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2",
                "block-3"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
        final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);
        List<String> encodedBlockIds = new ArrayList<>();
        for (int i = 0; i < blockIds.size(); i++) {
            String blockId1 = Base64.getEncoder().encodeToString(blockIds.get(i).getBytes());
            encodedBlockIds.add(blockId1);
            abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, blockId1);
            URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString()));
            byte[] data = blockData.get(i);
            requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(data.length)));

            final AbfsRestOperation op = new AbfsRestOperation(
                    AbfsRestOperationType.PutBlock,
                    testClient,
                    HTTP_METHOD_PUT,
                    url,
                    requestHeaders,
                    data, 0, data.length, null);

            TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                    "abcde", FSOperationType.APPEND,
                    TracingHeaderFormat.ALL_ID_FORMAT, null));

            op.execute(tracingContext);
        }
        byte[] bufferString = generateBlockListXml(blockIds).getBytes(StandardCharsets.UTF_8);
        final AbfsUriQueryBuilder abfsUriQueryBuilder1 = testClient.createDefaultUriQueryBuilder();
        final List<AbfsHttpHeader> requestHeaders1 = TestAbfsClient.getTestRequestHeaders(testClient);
        abfsUriQueryBuilder1.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
        requestHeaders1.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(bufferString.length)));
        requestHeaders1.add(new AbfsHttpHeader(CONTENT_TYPE, "application/xml"));
        URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder1.toString()));
        final AbfsRestOperation op = new AbfsRestOperation(
                AbfsRestOperationType.PutBlockList,
                testClient,
                HTTP_METHOD_PUT,
                url,
                requestHeaders1,
                bufferString, 0, bufferString.length, null);

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                "abcde", FSOperationType.APPEND,
                TracingHeaderFormat.ALL_ID_FORMAT, null));

        op.execute(tracingContext);

        /* Validates that all blocks are committed and fetched */
        AbfsRestOperation op1 = testClient.getBlockList(finalTestPath, tracingContext);
        List<String> committedBlockList = op1.getResult().getBlockIdList();
        assertEquals(encodedBlockIds, committedBlockList);
    }

    /**
     * If we put an additional block id in putblocklist which is not staged it should give error.
     * This test case verifies the same.
     * @throws Exception
     */
    @Test
    public void testPutBlockListForAdditionalBlockId() throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final Configuration configuration = new Configuration();
        configuration.addResource(TEST_CONFIGURATION_FILE_NAME);
        AbfsClient abfsClient = getClient(fs);

        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
                configuration.get(FS_AZURE_ABFS_ACCOUNT_NAME));
        List<String> blockIds = new ArrayList<>(Arrays.asList(
                "block-1",
                "block-2",
                "block-3",
                "block-4"
        ));
        List<byte[]> blockData = new ArrayList<>(Arrays.asList(
                "hello".getBytes(),
                "world".getBytes(),
                "!".getBytes()
        ));
        AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
                abfsClient,
                abfsConfiguration));
        Path testPath = path(TEST_PATH);
        String finalTestPath = testPath.toString().substring(testPath.toString().lastIndexOf("/"));
        final List<AbfsHttpHeader> requestHeaders = TestAbfsClient.getTestRequestHeaders(testClient);
        final AbfsUriQueryBuilder abfsUriQueryBuilder = testClient.createDefaultUriQueryBuilder();
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);

        for (int i = 0; i < blockIds.size() - 1; i++) {
            String blockId1 = Base64.getEncoder().encodeToString(blockIds.get(i).getBytes());
            abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, blockId1);
            URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder.toString()));
            byte[] data = blockData.get(i);
            requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(data.length)));

            final AbfsRestOperation op = new AbfsRestOperation(
                    AbfsRestOperationType.PutBlock,
                    testClient,
                    HTTP_METHOD_PUT,
                    url,
                    requestHeaders,
                    data, 0, data.length, null);

            TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                    "abcde", FSOperationType.APPEND,
                    TracingHeaderFormat.ALL_ID_FORMAT, null));

            op.execute(tracingContext);
        }
        byte[] bufferString = generateBlockListXml(blockIds).getBytes(StandardCharsets.UTF_8);
        final AbfsUriQueryBuilder abfsUriQueryBuilder1 = testClient.createDefaultUriQueryBuilder();
        final List<AbfsHttpHeader> requestHeaders1 = TestAbfsClient.getTestRequestHeaders(testClient);
        abfsUriQueryBuilder1.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
        requestHeaders1.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(bufferString.length)));
        requestHeaders1.add(new AbfsHttpHeader(CONTENT_TYPE, "application/xml"));
        URL url = Mockito.spy(testClient.createRequestUrl(finalTestPath, abfsUriQueryBuilder1.toString()));
        final AbfsRestOperation op = new AbfsRestOperation(
                AbfsRestOperationType.PutBlockList,
                testClient,
                HTTP_METHOD_PUT,
                url,
                requestHeaders1,
                bufferString, 0, bufferString.length, null);

        TracingContext tracingContext = Mockito.spy(new TracingContext("abcd",
                "abcde", FSOperationType.APPEND,
                TracingHeaderFormat.ALL_ID_FORMAT, null));

        /* Verify that an additional blockId which is not staged if we try to commit, it throws an exception */
        intercept(IOException.class, () -> op.execute(tracingContext));
        Assertions.assertThat(op.getResult().getStatusCode())
                .describedAs("The error code is not correct")
                .isEqualTo(HTTP_BAD_REQUEST);
    }

    /*
     * Helper method that creates test data of size provided by the
     * "size" parameter.
     */
    private static byte[] getTestData(int size) {
        byte[] testData = new byte[size];
        System.arraycopy(RandomStringUtils.randomAlphabetic(size).getBytes(), 0, testData, 0, size);
        return testData;
    }

    // Helper method to create file and write fileSize bytes of data on it.
    private byte[] createBaseFileWithData(int fileSize, Path testPath) throws Throwable {
        // To create versions
        try (FSDataOutputStream createStream = fs.create(testPath)) {
        }
        fs.delete(testPath, false);

        try (FSDataOutputStream createStream = fs.create(testPath)) {
            byte[] fileData = null;

            if (fileSize != 0) {
                fileData = getTestData(fileSize);
                createStream.write(fileData);
            }
            assertTrue(fs.exists(testPath));
            return fileData;
        }
    }

    /*
     * Helper method to verify a file data equal to "dataLength" parameter
     */
    private boolean verifyFileData(int dataLength, byte[] testData, int testDataIndex,
                                   FSDataInputStream srcStream) {
        try {
            byte[] fileBuffer = new byte[dataLength];
            byte[] testDataBuffer = new byte[dataLength];
            int fileBytesRead = srcStream.read(fileBuffer);
            if (fileBytesRead < dataLength) {
                return false;
            }
            System.arraycopy(testData, testDataIndex, testDataBuffer, 0, dataLength);
            if (!Arrays.equals(fileBuffer, testDataBuffer)) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*
     * Helper method to verify Append on a testFile.
     */
    private boolean verifyAppend(byte[] testData, Path testFile) {
        try (FSDataInputStream srcStream = fs.open(testFile)) {
            int baseBufferSize = 2048;
            int testDataSize = testData.length;
            int testDataIndex = 0;
            while (testDataSize > baseBufferSize) {
                if (!verifyFileData(baseBufferSize, testData, testDataIndex, srcStream)) {
                    return false;
                }
                testDataIndex += baseBufferSize;
                testDataSize -= baseBufferSize;
            }
            if (!verifyFileData(testDataSize, testData, testDataIndex, srcStream)) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /*
     * Test case to verify if an append on small size data works. This tests
     * append E2E
     */
    @Test
    public void testSingleAppend() throws Throwable {
        FSDataOutputStream appendStream = null;
        try {
            int baseDataSize = 50;
            byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);

            int appendDataSize = 20;
            byte[] appendDataBuffer = getTestData(appendDataSize);
            appendStream = fs.append(testPath, 10);
            appendStream.write(appendDataBuffer);
            appendStream.close();
            byte[] testData = new byte[baseDataSize + appendDataSize];
            System.arraycopy(baseDataBuffer, 0, testData, 0, baseDataSize);
            System.arraycopy(appendDataBuffer, 0, testData, baseDataSize, appendDataSize);

            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Test case to verify append to an empty file.
     */
    @Test
    public void testSingleAppendOnEmptyFile() throws Throwable {
        FSDataOutputStream appendStream = null;
        try {
            createBaseFileWithData(0, testPath);
            int appendDataSize = 20;
            byte[] appendDataBuffer = getTestData(appendDataSize);
            appendStream = fs.append(testPath, 10);
            appendStream.write(appendDataBuffer);
            appendStream.close();

            assertTrue(verifyAppend(appendDataBuffer, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Tests to verify multiple appends on a Blob.
     */
    @Test
    public void testMultipleAppends() throws Throwable {

        int baseDataSize = 50;
        byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);

        int appendDataSize = 100;
        int targetAppendCount = 50;
        byte[] testData = new byte[baseDataSize + (appendDataSize * targetAppendCount)];
        int testDataIndex = 0;
        System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
        testDataIndex += baseDataSize;
        int appendCount = 0;
        FSDataOutputStream appendStream = null;
        try {
            while (appendCount < targetAppendCount) {

                byte[] appendDataBuffer = getTestData(appendDataSize);
                appendStream = fs.append(testPath, 30);
                appendStream.write(appendDataBuffer);
                appendStream.close();

                System.arraycopy(appendDataBuffer, 0, testData, testDataIndex, appendDataSize);
                testDataIndex += appendDataSize;
                appendCount++;
            }
            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /*
     * Test to verify we multiple appends on the same stream.
     */
    @Test
    public void testMultipleAppendsOnSameStream() throws Throwable {

        int baseDataSize = 50;
        byte[] baseDataBuffer = createBaseFileWithData(baseDataSize, testPath);
        int appendDataSize = 100;
        int targetAppendCount = 50;
        byte[] testData = new byte[baseDataSize + (appendDataSize * targetAppendCount)];
        int testDataIndex = 0;
        System.arraycopy(baseDataBuffer, 0, testData, testDataIndex, baseDataSize);
        testDataIndex += baseDataSize;
        int appendCount = 0;

        FSDataOutputStream appendStream = null;
        try {
            while (appendCount < targetAppendCount) {
                appendStream = fs.append(testPath, 50);
                int singleAppendChunkSize = 20;
                int appendRunSize = 0;
                while (appendRunSize < appendDataSize) {

                    byte[] appendDataBuffer = getTestData(singleAppendChunkSize);
                    appendStream.write(appendDataBuffer);
                    System.arraycopy(appendDataBuffer, 0, testData,
                            testDataIndex + appendRunSize, singleAppendChunkSize);

                    appendRunSize += singleAppendChunkSize;
                }
                appendStream.close();
                testDataIndex += appendDataSize;
                appendCount++;
            }

            assertTrue(verifyAppend(testData, testPath));
        } finally {
            if (appendStream != null) {
                appendStream.close();
            }
        }
    }

    /**
     * Verify that parallel flush for same path on same blockId throws exception.
     **/
    @Test
    public void testParallelFlush() throws Exception {
        Configuration configuration = getRawConfiguration();
        configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
        FileSystem fs = FileSystem.newInstance(configuration);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    FSDataOutputStream out = fs.create(testPath);
                    out.write('1');
                    out.hsync();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        int exceptionCaught = 0;
        for (Future<?> future : futures) {
            try {
                future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    exceptionCaught++;
                    intercept(RuntimeException.class, "The condition specified using HTTP conditional header(s) is not met.", () -> {
                        throw (RuntimeException) cause; // re-throw the RuntimeException
                    });
                } else {
                    System.err.println("Unexpected exception caught: " + cause);
                }
            } catch (InterruptedException e) {
                // handle interruption
            }
        }
        assertEquals(exceptionCaught, 4);
    }
}

