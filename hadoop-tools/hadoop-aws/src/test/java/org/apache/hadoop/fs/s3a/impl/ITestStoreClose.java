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

import java.util.concurrent.RejectedExecutionException;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test for store closure.
 */
public class ITestStoreClose extends AbstractS3ATestBase {

  /**
   * Open a file in forced multipart, then close the fs.
   */
  @Test
  public void testStreamWriteClosed() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    Assumptions.assumeThat(fs.hasPathCapability(path, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
        .describedAs("Path capability %s is required", STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED)
        .isTrue();
    final FSDataOutputStreamBuilder builder = fs.createFile(path);
    final FSDataOutputStream out = builder.build();
    out.write('a');
    out.flush();

    fs.close();
    intercept(IllegalStateException.class, out::close);
  }

  /**
   * Open a file in forced multipart, then close the fs.
   */
  @Test
  public void testMultipartUploadClosed() throws Throwable {

    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    Assumptions.assumeThat(fs.hasPathCapability(path, STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED))
        .describedAs("Path capability %s is required", STORE_CAPABILITY_MULTIPART_UPLOAD_ENABLED)
        .isTrue();
    final FSDataOutputStreamBuilder builder = fs.createFile(path);
    builder.must(FS_S3A_CREATE_MULTIPART, true);
    final FSDataOutputStream out = builder.build();
    out.write('a');
    out.flush();

    fs.close();
    intercept(RejectedExecutionException.class, out::close);
  }

}
