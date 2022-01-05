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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.Constants.STORAGE_CLASS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

public class ITestS3AHugeFilesStorageClass extends AbstractSTestS3AHugeFiles {

  private static final String TEST_STORAGE_CLASS = "REDUCED_REDUNDANCY";

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();
    disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf, STORAGE_CLASS);

    conf.set(STORAGE_CLASS, TEST_STORAGE_CLASS);
    return conf;
  }

  @Override
  protected String getBlockOutputBufferName() {
    return Constants.FAST_UPLOAD_BUFFER_ARRAY;
  }

  @Override
  protected void assertStorageClass(Path hugeFile) throws IOException {
    String expected = TEST_STORAGE_CLASS;

    S3AFileSystem fs = getFileSystem();
    String actual = fs.getObjectMetadata(hugeFile).getStorageClass();

    assertEquals("Storage class of object is " + actual + ", expected " + expected, expected,
        actual);
  }
}
