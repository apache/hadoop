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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.UPLOAD_PART_COUNT_LIMIT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Testing S3 multipart upload for s3.
 */
public class ITestS3AMultipartUpload extends S3AScaleTestBase {

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration configuration = super.createScaleConfiguration();
    configuration.setLong(MULTIPART_SIZE, 5 * _1MB);
    // Setting the part count limit to 2 such that we get failures.
    configuration.setLong(UPLOAD_PART_COUNT_LIMIT, 2);
    return configuration;
  }

  /**
   * Tests to validate that exception is thrown during a
   * multi part upload when the number of parts is greater
   * than the allowed limit.
   */
  @Test
  public void testMultiPartUploadFailure() throws Throwable {
    FileSystem fs = getFileSystem();
    Path file = path(getMethodName());
    // Creating a file having parts less than configured
    // part count will succeed.
    createFile(fs, file, true,
            dataset(10 * _1MB, 'a', 'z' - 'a'));
    // Now with more than configured part count should
    // throw a PathIOE
    intercept(PathIOException.class,
            () -> createFile(fs,
                    file,
                    true,
                    dataset(15 * _1MB, 'a', 'z' - 'a')));

  }
}
