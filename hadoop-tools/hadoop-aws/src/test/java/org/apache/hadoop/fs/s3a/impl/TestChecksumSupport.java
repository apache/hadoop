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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;

public class TestChecksumSupport {

  @Test
  public void testGetSupportedChecksumAlgorithmCRC32() {
    testGetSupportedChecksumAlgorithm(ChecksumAlgorithm.CRC32);
  }

  @Test
  public void testGetSupportedChecksumAlgorithmCRC32C() {
    testGetSupportedChecksumAlgorithm(ChecksumAlgorithm.CRC32_C);
  }

  @Test
  public void testGetSupportedChecksumAlgorithmSHA1() {
    testGetSupportedChecksumAlgorithm(ChecksumAlgorithm.SHA1);
  }

  @Test
  public void testGetSupportedChecksumAlgorithmSHA256() {
    testGetSupportedChecksumAlgorithm(ChecksumAlgorithm.SHA256);
  }

  private void testGetSupportedChecksumAlgorithm(ChecksumAlgorithm checksumAlgorithm) {
    final Configuration conf = new Configuration();
    conf.set(CHECKSUM_ALGORITHM, checksumAlgorithm.toString());
    Assertions.assertThat(ChecksumSupport.getChecksumAlgorithm(conf))
        .describedAs("Checksum algorithm must match value set in the configuration")
        .isEqualTo(checksumAlgorithm);
  }

  @Test
  public void testGetChecksumAlgorithmWhenNull() {
    final Configuration conf = new Configuration();
    conf.unset(CHECKSUM_ALGORITHM);
    Assertions.assertThat(ChecksumSupport.getChecksumAlgorithm(conf))
        .describedAs("If configuration is not set, checksum algorithm must be null")
        .isNull();
  }

  @Test
  public void testGetNotSupportedChecksumAlgorithm() {
    final Configuration conf = new Configuration();
    conf.set(CHECKSUM_ALGORITHM, "INVALID");
    Assertions.assertThatThrownBy(() -> ChecksumSupport.getChecksumAlgorithm(conf))
        .describedAs("Invalid checksum algorithm should throw an exception")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
