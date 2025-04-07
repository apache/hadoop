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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChecksumSupport;

import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS;

/**
 * Tests S3 checksum algorithm.
 * If CHECKSUM_ALGORITHM config is not set in auth-keys.xml,
 * SHA256 algorithm will be picked.
 */
public class ITestS3AChecksum extends AbstractS3ATestBase {

  private static final ChecksumAlgorithm DEFAULT_CHECKSUM_ALGORITHM = ChecksumAlgorithm.SHA256;

  private ChecksumAlgorithm checksumAlgorithm;

  private static final int[] SIZES = {
      1, 2, 3, 4, 5, 254, 255, 256, 257, 2 ^ 12 - 1
  };

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        CHECKSUM_ALGORITHM,
        REJECT_OUT_OF_SPAN_OPERATIONS);
    S3ATestUtils.disableFilesystemCaching(conf);
    checksumAlgorithm = ChecksumSupport.getChecksumAlgorithm(conf);
    if (checksumAlgorithm == null) {
      checksumAlgorithm = DEFAULT_CHECKSUM_ALGORITHM;
      LOG.info("No checksum algorithm found in configuration, will use default {}",
          checksumAlgorithm);
      conf.set(CHECKSUM_ALGORITHM, checksumAlgorithm.toString());
    }
    conf.setBoolean(REJECT_OUT_OF_SPAN_OPERATIONS, false);
    return conf;
  }

  @Test
  public void testChecksum() throws IOException {
    for (int size : SIZES) {
      validateChecksumForFilesize(size);
    }
  }

  private void validateChecksumForFilesize(int len) throws IOException {
    describe("Create a file of size " + len);
    String src = String.format("%s-%04x", methodName.getMethodName(), len);
    Path path = writeThenReadFile(src, len);
    assertChecksum(path);
    rm(getFileSystem(), path, false, false);
  }

  private void assertChecksum(Path path) throws IOException {
    final String key = getFileSystem().pathToKey(path);
    HeadObjectRequest.Builder requestBuilder = getFileSystem().getRequestFactory()
        .newHeadObjectRequestBuilder(key)
        .checksumMode(ChecksumMode.ENABLED);
    HeadObjectResponse headObject = getFileSystem().getS3AInternals()
        .getAmazonS3Client("Call head object with checksum enabled")
        .headObject(requestBuilder.build());
    switch (checksumAlgorithm) {
    case CRC32:
      Assertions.assertThat(headObject.checksumCRC32())
          .describedAs("headObject.checksumCRC32()")
          .isNotNull();
      break;
    case CRC32_C:
      Assertions.assertThat(headObject.checksumCRC32C())
          .describedAs("headObject.checksumCRC32C()")
          .isNotNull();
      break;
    case SHA1:
      Assertions.assertThat(headObject.checksumSHA1())
          .describedAs("headObject.checksumSHA1()")
          .isNotNull();
      break;
    case SHA256:
      Assertions.assertThat(headObject.checksumSHA256())
          .describedAs("headObject.checksumSHA256()")
          .isNotNull();
      break;
    default:
      fail("Checksum algorithm not supported: " + checksumAlgorithm);
    }
  }

}
