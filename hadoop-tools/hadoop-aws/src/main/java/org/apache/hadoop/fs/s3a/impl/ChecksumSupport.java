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

import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ConfigurationHelper;

import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;

/**
 * Utility class to support operations on S3 object checksum.
 */
public final class ChecksumSupport {

  private ChecksumSupport() {
  }

  /**
   * Checksum algorithms that are supported by S3A.
   */
  private static final Set<ChecksumAlgorithm> SUPPORTED_CHECKSUM_ALGORITHMS = ImmutableSet.of(
      ChecksumAlgorithm.CRC32,
      ChecksumAlgorithm.CRC32_C,
      ChecksumAlgorithm.SHA1,
      ChecksumAlgorithm.SHA256);

  /**
   * Get the checksum algorithm to be used for data integrity check of the objects in S3.
   * This operation includes validating if the provided value is a supported checksum algorithm.
   * @param conf configuration to scan
   * @return the checksum algorithm to be passed on S3 requests
   * @throws IllegalArgumentException if the checksum algorithm is not known or not supported
   */
  public static ChecksumAlgorithm getChecksumAlgorithm(Configuration conf) {
    return ConfigurationHelper.resolveEnum(conf,
        CHECKSUM_ALGORITHM,
        ChecksumAlgorithm.class,
        configValue -> {
          if (StringUtils.isBlank(configValue)) {
            return null;
          }
          if (ChecksumAlgorithm.CRC32_C.toString().equalsIgnoreCase(configValue)) {
            // In case the configuration value is CRC32C, without underscore.
            return ChecksumAlgorithm.CRC32_C;
          }
          throw new IllegalArgumentException("Checksum algorithm is not supported: " + configValue);
        });
  }
}
