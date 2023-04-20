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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.api.UnsupportedRequestException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_UPLOADS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use a single PUT for the whole upload/rename/delete workflow; include verification
 * that the transfer manager will fail fast unless the multipart threshold is huge.
 */
public class ITestS3AHugeFilesNoMultipart extends AbstractSTestS3AHugeFiles {

  /**
   * Size to ensure MPUs don't happen in transfer manager.
   */
  public static final String S_1T = "1T";

  public static final String SINGLE_PUT_REQUEST_TIMEOUT = "1h";

  /**
   * Always use disk storage.
   * @return disk block store always.
   */
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_DISK;
  }

  @Override
  protected boolean expectMultipartUpload() {
    return false;
  }

  /**
   * Create a configuration without multipart upload,
   * and a long request timeout to allow for a very slow
   * PUT in close.
   * @return the configuration to create the test FS with.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    removeBaseAndBucketOverrides(conf,
        IO_CHUNK_BUFFER_SIZE,
        MIN_MULTIPART_THRESHOLD,
        MULTIPART_UPLOADS_ENABLED,
        MULTIPART_SIZE,
        REQUEST_TIMEOUT);
    conf.setInt(IO_CHUNK_BUFFER_SIZE, 655360);
    conf.set(MIN_MULTIPART_THRESHOLD, S_1T);
    conf.set(MULTIPART_SIZE, S_1T);
    conf.setBoolean(MULTIPART_UPLOADS_ENABLED, false);
    conf.set(REQUEST_TIMEOUT, SINGLE_PUT_REQUEST_TIMEOUT);
    return conf;
  }

  /**
   * After the file is created, attempt a rename with an FS
   * instance with a small multipart threshold;
   * this MUST be rejected.
   */
  @Override
  public void test_030_postCreationAssertions() throws Throwable {
    assumeHugeFileExists();
    final Path hugefile = getHugefile();
    final Path hugefileRenamed = getHugefileRenamed();
    describe("renaming %s to %s", hugefile, hugefileRenamed);
    S3AFileSystem fs = getFileSystem();
    fs.delete(hugefileRenamed, false);
    // create a new fs with a small multipart threshold; expect rename failure.
    final Configuration conf = new Configuration(fs.getConf());
    conf.setInt(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    S3ATestUtils.disableFilesystemCaching(conf);

    try (FileSystem fs2 = FileSystem.get(fs.getUri(), conf)) {
      intercept(UnsupportedRequestException.class, () ->
          fs2.rename(hugefile, hugefileRenamed));
    }
  }
}
