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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_OVERWRITE_SUPPORTED;
import static org.apache.hadoop.fs.s3a.Constants.IF_NONE_MATCH_STAR;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.IF_NONE_MATCH;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_412_PRECONDITION_FAILED;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1KB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


public class ITestS3APutIfMatch extends AbstractS3ACostTest {

    private static final int UPDATED_MULTIPART_THRESHOLD = 100 * _1KB;

    private static final byte[] SMALL_FILE_BYTES = dataset(TEST_FILE_LEN, 0, 255);
    private static final byte[] MULTIPART_FILE_BYTES = dataset(UPDATED_MULTIPART_THRESHOLD * 5, 'a', 'z' - 'a');

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

    /**
     * Create a file using the If-None-Match feature from S3
     * @param fs filesystem
     * @param path       path to write
     * @param data source dataset. Can be null
     * @throws Exception on any problem
     */
    private static void createFileWithIfNoneMatchFlag(
            FileSystem fs,
            Path path,
            byte[] data,
            boolean forceMultipart) throws Exception {
        FSDataOutputStream stream = getStreamWithIfNoneMatchFlag(fs, path, forceMultipart);
        if (data != null && data.length > 0) {
            stream.write(data);
        }
        stream.close();
    }

    private static void createFileWithIfNoneMatchFlag(
            FileSystem fs,
            Path path,
            byte[] data) throws Exception {
        createFileWithIfNoneMatchFlag(fs, path, data, false);
    }

    /**
     * Creates an {@link FSDataOutputStream} for writing a file with an If-None-Match
     * @param fs filesystem
     * @param path       path to write
     */
     private static FSDataOutputStream getStreamWithIfNoneMatchFlag(
            FileSystem fs,
            Path path,
            boolean forceMultipart) throws Exception {
        FSDataOutputStreamBuilder builder = fs.createFile(path);
        builder.must(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE, "true");
        builder.opt(FS_S3A_CREATE_HEADER + "." + IF_NONE_MATCH, IF_NONE_MATCH_STAR);
        if (forceMultipart) {
            builder.opt(FS_S3A_CREATE_MULTIPART, "true");
        }
        return builder.create().build();
    }

    private static FSDataOutputStream getStreamWithIfNoneMatchFlag(
            FileSystem fs,
            Path path) throws Exception {
        return getStreamWithIfNoneMatchFlag(fs, path, false);
    }

    @Test
    public void testPutIfAbsentConflict() throws Throwable {
        describe("generate conflict on overwrites");
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create a file over an empty path: all good
        createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES);

        // attempted overwrite fails
        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

        // second attempt also fails
        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);

        // Delete file and verify an overwrite works again
        fs.delete(testFile, false);
        createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES);
    }

    @Test
    public void testPutIfAbsentLargeFileConflict() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip if multipart upload not supported
        Assume.assumeTrue("Skipping as multipart upload not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        createFileWithIfNoneMatchFlag(fs, testFile, MULTIPART_FILE_BYTES, true);

        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, MULTIPART_FILE_BYTES, true));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, firstException);

        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, MULTIPART_FILE_BYTES, true));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, secondException);
    }

    @Test
    public void testMultipartFileWithRaceCondition() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file with multipart upload but do not close the stream
        FSDataOutputStream stream = getStreamWithIfNoneMatchFlag(fs, testFile, true);
        stream.write(MULTIPART_FILE_BYTES);

        // create and close another small file in parallel
        createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES);

        // Closing the first stream should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testTwoMultipartFileWithRaceCondition() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // Skip test if multipart uploads are not supported
        Assume.assumeTrue("Skipping test as multipart uploads are not supported",
                fs.hasPathCapability(testFile, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED));

        // Create a file with multipart upload but do not close the stream
        FSDataOutputStream stream = getStreamWithIfNoneMatchFlag(fs, testFile, true);
        stream.write(MULTIPART_FILE_BYTES);

        // create and close another multipart file in parallel
        createFileWithIfNoneMatchFlag(fs, testFile, MULTIPART_FILE_BYTES, true);

        // Closing the first stream should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testOverwriteWithEmptyFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create a non-empty file
        createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES);

        // overwrite with zero-byte file (no write)
        FSDataOutputStream stream = getStreamWithIfNoneMatchFlag(fs, testFile);

        // close the stream, should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testOverwriteEmptyFileWithFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create an empty file (no write)
        FSDataOutputStream stream = getStreamWithIfNoneMatchFlag(fs, testFile);
        stream.close();

        // overwrite with non-empty file, should throw RemoteFileChangedException
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, SMALL_FILE_BYTES));
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }

    @Test
    public void testOverwriteEmptyWithEmptyFile() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();
        fs.mkdirs(testFile.getParent());

        // create an empty file (no write)
        FSDataOutputStream stream1 = getStreamWithIfNoneMatchFlag(fs, testFile);
        stream1.close();

        // overwrite with another empty file, should throw RemoteFileChangedException
        FSDataOutputStream stream2 = getStreamWithIfNoneMatchFlag(fs, testFile);
        RemoteFileChangedException exception = intercept(RemoteFileChangedException.class, stream2::close);
        assertS3ExceptionStatusCode(SC_412_PRECONDITION_FAILED, exception);
    }
}
