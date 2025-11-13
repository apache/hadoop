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
import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChecksumSupport;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_GENERATION;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_CHECKSUM_GENERATION;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_VALIDATION;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.REJECT_OUT_OF_SPAN_OPERATIONS;

/**
 * Tests S3 checksum algorithm.
 * If CHECKSUM_ALGORITHM config is not set in auth-keys.xml,
 * SHA256 algorithm will be picked.
 */
@ParameterizedClass(name="checksum={0}")
@MethodSource("params")
public class ITestS3AChecksum extends AbstractS3ATestBase {

  public static final String UNKNOWN = "UNKNOWN_TO_SDK_VERSION";

  private ChecksumAlgorithm checksumAlgorithm;

  /**
   * Parameterization.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"SHA256"},
        {"CRC32C"},
        {"SHA1"},
        {UNKNOWN},
    });
  }

  private static final int[] SIZES = {
      5, 255, 256, 257, 2 ^ 12 - 1
  };

  private final String algorithmName;

  public ITestS3AChecksum(final String algorithmName) {
    this.algorithmName = algorithmName;
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    // get the base checksum algorithm, if set it will be left alone.
    final String al = conf.getTrimmed(CHECKSUM_ALGORITHM, "");
    if (!UNKNOWN.equals(algorithmName) &&
        (ChecksumSupport.NONE.equalsIgnoreCase(al) || UNKNOWN.equalsIgnoreCase(al))) {
      skip("Skipping checksum algorithm tests");
    }
    S3ATestUtils.removeBaseAndBucketOverrides(conf,
        CHECKSUM_ALGORITHM,
        CHECKSUM_VALIDATION,
        REJECT_OUT_OF_SPAN_OPERATIONS);
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(CHECKSUM_ALGORITHM, algorithmName);
    conf.setBoolean(CHECKSUM_VALIDATION, true);
    conf.setBoolean(REJECT_OUT_OF_SPAN_OPERATIONS, false);
    checksumAlgorithm = ChecksumSupport.getChecksumAlgorithm(conf);
    LOG.info("Using checksum algorithm {}/{}", algorithmName, checksumAlgorithm);
    assume("Skipping checksum tests as " + CHECKSUM_GENERATION + " is set",
        propagateBucketOptions(conf, getTestBucketName(conf))
            .getBoolean(CHECKSUM_GENERATION, DEFAULT_CHECKSUM_GENERATION));
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
    final Path path = methodPath();
    writeThenReadFile(path, len);
    assertChecksum(path);
  }

  private void assertChecksum(Path path) throws IOException {
    final String key = getFileSystem().pathToKey(path);
    // issue a head request and include asking for the checksum details.
    // such a query may require extra IAM permissions.
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
      Assertions.assertThat(headObject.checksumSHA256())
          .describedAs("headObject.checksumSHA256()")
          .isNull();
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
    case UNKNOWN_TO_SDK_VERSION:
      // expect values to be null
      // this is brittle with different stores; crc32 assertions have been cut
      // because S3 express always set them.
      Assertions.assertThat(headObject.checksumSHA256())
          .describedAs("headObject.checksumSHA256()")
          .isNull();
      break;
    default:
      fail("Checksum algorithm not supported: " + checksumAlgorithm);
    }
  }

}
