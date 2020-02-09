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

package org.apache.hadoop.fs.s3a;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Class to test bucket existence api.
 * See {@link S3AFileSystem#doBucketProbing()}.
 */
public class ITestS3ABucketExistence extends AbstractS3ATestBase {

  private FileSystem fs;

  private final String randomBucket =
          "random-bucket-" + UUID.randomUUID().toString();

  private final URI uri = URI.create(FS_S3A + "://" + randomBucket);

  @Test
  public void testNoBucketProbing() throws Exception {
    Configuration configuration = getConfiguration();
    configuration.setInt(S3A_BUCKET_PROBE, 0);
    try {
      fs = FileSystem.get(uri, configuration);
    } catch (IOException ex) {
      LOG.error("Exception : ", ex);
      throw ex;
    }

    Path path = new Path(uri);
    intercept(FileNotFoundException.class,
            "No such file or directory: " + path,
        () -> fs.getFileStatus(path));

    Path src = new Path(fs.getUri() + "/testfile");
    byte[] data = dataset(1024, 'a', 'z');
    intercept(FileNotFoundException.class,
            "The specified bucket does not exist",
        () -> writeDataset(fs, src, data, data.length, 1024 * 1024, true));
  }

  @Test
  public void testBucketProbingV1() throws Exception {
    Configuration configuration = getConfiguration();
    configuration.setInt(S3A_BUCKET_PROBE, 1);
    intercept(FileNotFoundException.class,
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbingV2() throws Exception {
    Configuration configuration = getConfiguration();
    configuration.setInt(S3A_BUCKET_PROBE, 2);
    intercept(FileNotFoundException.class,
        () -> FileSystem.get(uri, configuration));
  }

  @Test
  public void testBucketProbingParameterValidation() throws Exception {
    Configuration configuration = getConfiguration();
    configuration.setInt(S3A_BUCKET_PROBE, 3);
    intercept(IllegalArgumentException.class,
            "Value of " + S3A_BUCKET_PROBE + " should be between 0 to 2",
            "Should throw IllegalArgumentException",
        () -> FileSystem.get(uri, configuration));
    configuration.setInt(S3A_BUCKET_PROBE, -1);
    intercept(IllegalArgumentException.class,
            "Value of " + S3A_BUCKET_PROBE + " should be between 0 to 2",
            "Should throw IllegalArgumentException",
        () -> FileSystem.get(uri, configuration));
  }

  @Override
  protected Configuration getConfiguration() {
    Configuration configuration = super.getConfiguration();
    S3ATestUtils.disableFilesystemCaching(configuration);
    return configuration;
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(getLogger(), fs);
    super.teardown();
  }
}
