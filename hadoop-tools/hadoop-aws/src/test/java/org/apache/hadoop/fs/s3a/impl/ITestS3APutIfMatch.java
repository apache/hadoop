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

package org.apache.hadoop.fs.s3a.impl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import org.apache.hadoop.fs.s3a.statistics.BlockOutputStreamStatistics;
import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_OVERWRITE_SUPPORTED;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_PERFORMANCE_FLAGS;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_412_PRECONDITION_FAILED;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1KB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


public class ITestS3APutIfMatch extends AbstractS3ACostTest {

    private static final int UPDATED_MULTIPART_THRESHOLD = 100 * _1KB;

    private static final byte[] SMALL_FILE_BYTES = dataset(TEST_FILE_LEN, 0, 255);
    private static final byte[] MULTIPART_FILE_BYTES = dataset(UPDATED_MULTIPART_THRESHOLD * 5, 'a', 'z' - 'a');

    private BlockOutputStreamStatistics statistics;

    @Override
    public Configuration createConfiguration() {
        Configuration conf = super.createConfiguration();

        S3ATestUtils.disableFilesystemCaching(conf);
        removeBaseAndBucketOverrides(
                conf,
                MULTIPART_SIZE,
                UPLOAD_PART_COUNT_LIMIT,
                MIN_MULTIPART_THRESHOLD);
        conf.setLong(UPLOAD_PART_COUNT_LIMIT, 2);
        conf.setLong(MIN_MULTIPART_THRESHOLD, UPDATED_MULTIPART_THRESHOLD);
        conf.setInt(MULTIPART_SIZE, UPDATED_MULTIPART_THRESHOLD);
        return conf;
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        Configuration conf = getConfiguration();
        skipIfNotEnabled(conf, FS_S3A_CREATE_OVERWRITE_SUPPORTED,
                "Skipping IfNoneMatch tests");
    }

    private static void assertS3ExceptionStatusCode(int code, Exception ex) {
        S3Exception s3Exception = (S3Exception) ex.getCause();

        if (s3Exception.statusCode() != code) {
            throw new AssertionError("Expected status code " + code + " from " + ex, ex);
        }
    }

    protected String getBlockOutputBufferName() {
        return FAST_UPLOAD_BUFFER_ARRAY;
    }

    private static void createFileWithFlags(
            FileSystem fs,
            Path path,
            byte[] data,
            boolean ifNoneMatchFlag,
            String etag,
            boolean forceMultipart) throws Exception {
        FSDataOutputStream stream = getStreamWithFlags(fs, path, ifNoneMatchFlag, etag, forceMultipart);
        if (data != null && data.length > 0) {
            stream.write(data);
        }
        stream.close();
    }

    private static void createFileWithFlags(
            FileSystem fs,
            Path path,
            byte[] data,
            boolean ifNoneMatchFlag,
            String etag) throws Exception {
        createFileWithFlags(fs, path, data, ifNoneMatchFlag, etag, false);
    }

     private static FSDataOutputStream getStreamWithFlags(
            FileSystem fs,
            Path path,
            boolean ifNoneMatchFlag,
            String etag,
            boolean forceMultipart) throws Exception {
        FSDataOutputStreamBuilder builder = fs.createFile(path);
        if (ifNoneMatchFlag) {
            builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, "true");
        }
        if (etag != null) {
            builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, etag);
        }
        if (forceMultipart) {
            builder.opt(FS_S3A_CREATE_MULTIPART, "true");
        }
        return builder.create().build();
    }

    private static FSDataOutputStream getStreamWithFlags(
            FileSystem fs,
            Path path,
            boolean ifNoneMatchFlag,
            String etag) throws Exception {
        return getStreamWithFlags(fs, path, ifNoneMatchFlag, etag, false);
    }

    private static String readFileContent(FileSystem fs, Path path) throws Throwable {
        try (FSDataInputStream inputStream = fs.open(path)) {
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    private void updateStatistics(FSDataOutputStream stream) {
        statistics = S3ATestUtils.getOutputStreamStatistics(stream);
    }

    @Test
    public void testIfNoneMatchConflictOnOverwrite() throws Throwable {
        describe("generate conflict on overwrites");
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create a file over an empty path: all good
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

        // attempted overwrite fails
        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

        // second attempt also fails
        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);

        // Delete file and verify an overwrite works again
        fs.delete(testFile, false);
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);
    }

    @Test
    public void testIfNoneMatchConflictOnMultipartUpload() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip if multipart upload not supported
        Assume.assumeTrue("Skipping as multipart upload not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true);

        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);
    }

    @Test
    public void testIfNoneMatchMultipartUploadWithRaceCondition() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file with multipart upload but do not close the stream
        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
        stream.write(MULTIPART_FILE_BYTES);

        // create and close another small file in parallel
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

        // Closing the first stream should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfNoneMatchTwoConcurrentMultipartUploads() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file with multipart upload but do not close the stream
        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
        stream.write(MULTIPART_FILE_BYTES);

        // create and close another multipart file in parallel
        createFileWithFlags(fs, testFile, MULTIPART_FILE_BYTES, true, null, true);

        // Closing the first stream should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfNoneMatchOverwriteWithEmptyFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create a non-empty file
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

        // overwrite with zero-byte file (no write)
        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null);

        // close the stream, should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfNoneMatchOverwriteEmptyFileWithFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create an empty file (no write)
        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null);
        stream.close();

        // overwrite with non-empty file, should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfNoneMatchOverwriteEmptyWithEmptyFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create an empty file (no write)
        FSDataOutputStream stream1 = getStreamWithFlags(fs, testFile, true, null);
        stream1.close();

        // overwrite with another empty file, should throw RemoteFileChangedException
        FSDataOutputStream stream2 = getStreamWithFlags(fs, testFile, true, null);
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream2::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfMatchOverwriteWithCorrectEtag() throws Throwable {
        FileSystem fs = getFileSystem();
        Path path = methodPath();
        fs.mkdirs(path.getParent());

        // Create a file
        createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

        // Retrieve the etag from the created file
        String etag = ((S3AFileStatus) fs.getFileStatus(path)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        String updatedFileContent = "Updated content";
        byte[] updatedData = updatedFileContent.getBytes(StandardCharsets.UTF_8);

        // overwrite file with etag
        createFileWithFlags(fs, path, updatedData, false, etag);

        // read file and verify overwritten content
        String fileContent = readFileContent(fs, path);
        assertEquals(
                "File content should be correctly updated after overwriting with the correct ETag",
                updatedFileContent,
                fileContent
        );
    }

    @Test
    public void testIfMatchOverwriteWithOutdatedEtag() throws Throwable {
        FileSystem fs = getFileSystem();
        Path path = methodPath();
        fs.mkdirs(path.getParent());

        // Create a file
        createFileWithFlags(fs, path, SMALL_FILE_BYTES, true, null);

        // Retrieve the etag from the created file
        String etag = ((S3AFileStatus) fs.getFileStatus(path)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        // Overwrite the file. Will update the etag, making the previously fetched etag outdated.
        createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

        // overwrite file with outdated etag. Should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class,
                () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, etag));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfMatchOverwriteDeletedFileWithEtag() throws Throwable {
        FileSystem fs = getFileSystem();
        Path path = methodPath();
        fs.mkdirs(path.getParent());

        // Create a file
        createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

        // Retrieve the etag from the created file
        String etag = ((S3AFileStatus) fs.getFileStatus(path)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        // delete the file
        fs.delete(path);

        // overwrite file with etag. Should throw FileNotFoundException
        FileNotFoundException exception = intercept(FileNotFoundException.class,
                () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, etag));
        assertS3ExceptionStatusCode(SC_404_NOT_FOUND, exception);
    }

    @Test
    public void testIfMatchOverwriteFileWithEmptyEtag() throws Throwable {
        FileSystem fs = getFileSystem();
        Path path = methodPath();
        fs.mkdirs(path.getParent());

        // Create a file
        createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, null);

        // overwrite file with empty etag. Should throw IllegalArgumentException
        intercept(IllegalArgumentException.class,
                () -> createFileWithFlags(fs, path, SMALL_FILE_BYTES, false, ""));
    }

    @Test
    public void testIfMatchMultipartUploadWithRaceCondition() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file with multipart upload but do not close the stream
        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
        stream.write(MULTIPART_FILE_BYTES);

        // create and close another small file in parallel
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, true, null);

        // Closing the first stream should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfMatchTwoMultipartUploadsRaceConditionOneClosesFirst() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file and retrieve its etag
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, false, null);
        String etag = ((S3AFileStatus) fs.getFileStatus(testFile)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        // Start two multipart uploads with the same etag
        FSDataOutputStream stream1 = getStreamWithFlags(fs, testFile, false, etag, true);
        FSDataOutputStream stream2 = getStreamWithFlags(fs, testFile, false, etag, true);

        // Write data to both streams
        stream1.write(MULTIPART_FILE_BYTES);
        stream2.write(MULTIPART_FILE_BYTES);

        // Close the first stream successfully. Will update the etag
        stream1.close();

        // Close second stream, should fail due to etag mismatch
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream2::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testIfMatchIthCreateFileWithoutOverwrite() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // Create a file and retrieve the etag
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, false, null);
        String etag = ((S3AFileStatus) fs.getFileStatus(testFile)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        // Attempt to create a file at the same path without overwrite, using If-Match with the etag
        FileAlreadyExistsException exception = intercept(FileAlreadyExistsException.class, () -> {
            fs.createFile(testFile)
                    .overwrite(false)
                    .opt(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, etag)
                    .build();
        });
    }

    @Test
    public void testIfMatchIthCreateFileWithoutOverwriteWithPerformanceFlag() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        getConfiguration().set(FS_S3A_PERFORMANCE_FLAGS, "create");

        // Create a file and retrieve the etag
        createFileWithFlags(fs, testFile, SMALL_FILE_BYTES, false, null);
        String etag = ((S3AFileStatus) fs.getFileStatus(testFile)).getETag();
        assertNotNull("ETag should not be null after file creation", etag);

        // Attempt to create a file at the same path without overwrite, using If-Match with the etag
        FileAlreadyExistsException exception = intercept(FileAlreadyExistsException.class, () -> {
            fs.createFile(testFile)
                    .overwrite(false)
                    .opt(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG, etag)
                    .build();
        });
    }

    @Test
    public void testConditionalWriteStatistics() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        FSDataOutputStream stream = getStreamWithFlags(fs, testFile, true, null, true);
        updateStatistics(stream);
        stream.write(SMALL_FILE_BYTES);
        stream.close();

        long conditionalCreate = statistics.lookupCounterValue(Statistic.CONDITIONAL_CREATE.getSymbol());
        long conditionalCreateFailed = statistics.lookupCounterValue(Statistic.CONDITIONAL_CREATE_FAILED.getSymbol());

        assertEquals(1L, conditionalCreate);
        assertEquals(0L, conditionalCreateFailed);

        try {
            // try again. should fail
            stream = getStreamWithFlags(fs, testFile, true, null, true);
            updateStatistics(stream);
            stream.write(SMALL_FILE_BYTES);
            stream.close();
        } catch (Exception e) {}

        conditionalCreate = statistics.lookupCounterValue(Statistic.CONDITIONAL_CREATE.getSymbol());
        conditionalCreateFailed = statistics.lookupCounterValue(Statistic.CONDITIONAL_CREATE_FAILED.getSymbol());

        assertEquals(1L, conditionalCreate);
        assertEquals(1L, conditionalCreateFailed);
    }
}
