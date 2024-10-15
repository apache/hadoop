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
import org.apache.hadoop.io.IOUtils;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CONDITIONAL_FILE_CREATE;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1MB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


public class ITestS3APutIfMatch extends AbstractS3ACostTest {

    private Configuration conf;

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
        conf.setLong(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
        conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
        return conf;
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        conf = createConfiguration();
        skipIfNotEnabled(conf, FS_S3A_CONDITIONAL_FILE_CREATE,
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
     * Create a file using the PutIfMatch feature from S3
     * @param fs filesystem
     * @param path       path to write
     * @param data source dataset. Can be null
     * @throws IOException on any problem
     */
    private static void createFileWithIfNoneMatchFlag(
            FileSystem fs,
            Path path,
            byte[] data,
            String ifMatchTag) throws Exception {
          FSDataOutputStreamBuilder builder = fs.createFile(path);
          builder.must(FS_S3A_CONDITIONAL_FILE_CREATE, ifMatchTag);
          FSDataOutputStream stream = builder.create().build();
          if (data != null && data.length > 0) {
              stream.write(data);
          }
          stream.close();
          IOUtils.closeStream(stream);
    }

    @Test
    public void testPutIfAbsentConflict() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        fs.mkdirs(testFile.getParent());
        byte[] fileBytes = dataset(TEST_FILE_LEN, 0, 255);

        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, fileBytes, "*"));
        assertS3ExceptionStatusCode(412, firstException);

        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, fileBytes, "*"));
        assertS3ExceptionStatusCode(412, secondException);
    }


    @Test
    public void testPutIfAbsentLargeFileConflict() throws Throwable {
        FileSystem fs = getFileSystem();
        Path testFile = methodPath();

        // enough bytes for Multipart Upload
        byte[] fileBytes = dataset(6 * _1MB, 'a', 'z' - 'a');

        RemoteFileChangedException firstException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, fileBytes, "*"));
        assertS3ExceptionStatusCode(412, firstException);

        RemoteFileChangedException secondException = intercept(RemoteFileChangedException.class,
                () -> createFileWithIfNoneMatchFlag(fs, testFile, fileBytes, "*"));
        assertS3ExceptionStatusCode(412, secondException);

    }
}
