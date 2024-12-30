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

import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_EXPECT_CONTINUE;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_MIN_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_UPLOADS_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.PART_UPLOAD_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Use a single PUT for the whole upload/rename/delete workflow; include verification
 * that the transfer manager will fail fast unless the multipart threshold is huge.
 */
public class ITestS3AHugeFilesNoMultipart extends AbstractSTestS3AHugeFiles {

  public static final String SINGLE_PUT_REQUEST_TIMEOUT = "1h";

  /**
   * Always use disk storage.
   * @return disk block store always.
   */
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_DISK;
  }

  /**
   * Multipart upload is always disabled.
   * @return false
   */
  @Override
  protected boolean expectMultipartUpload() {
    return false;
  }

  /**
   * Is multipart copy enabled?
   * @return true if the transfer manager is used to copy files.
   */
  private boolean isMultipartCopyEnabled() {
    return getFileSystem().getS3AInternals().isMultipartCopyEnabled();
  }

  /**
   * Create a configuration without multipart upload,
   * and a long request timeout to allow for a very slow
   * PUT in close.
   * <p>
   * 100-continue is disabled so as to verify the behavior
   * on a large PUT.
   * @return the configuration to create the test FS with.
   */
  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    removeBaseAndBucketOverrides(conf,
        CONNECTION_EXPECT_CONTINUE,
        IO_CHUNK_BUFFER_SIZE,
        MIN_MULTIPART_THRESHOLD,
        MULTIPART_UPLOADS_ENABLED,
        MULTIPART_SIZE,
        PART_UPLOAD_TIMEOUT,
        REQUEST_TIMEOUT);
    conf.setBoolean(CONNECTION_EXPECT_CONTINUE, false);
    conf.setInt(IO_CHUNK_BUFFER_SIZE, 655360);
    conf.setInt(MIN_MULTIPART_THRESHOLD, MULTIPART_MIN_SIZE);
    conf.setInt(MULTIPART_SIZE, MULTIPART_MIN_SIZE);
    conf.setBoolean(MULTIPART_UPLOADS_ENABLED, false);
    conf.set(PART_UPLOAD_TIMEOUT, SINGLE_PUT_REQUEST_TIMEOUT);
    return conf;
  }

  /**
   * Verify multipart copy is disabled.
   */
  @Override
  public void test_030_postCreationAssertions() throws Throwable {
    super.test_030_postCreationAssertions();
    Assertions.assertThat(isMultipartCopyEnabled())
        .describedAs("Multipart copy should be disabled in %s", getFileSystem())
        .isFalse();
  }
}
