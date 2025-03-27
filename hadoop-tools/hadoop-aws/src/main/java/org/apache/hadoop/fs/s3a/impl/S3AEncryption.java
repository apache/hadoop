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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CONTEXT;

/**
 * Utility methods for S3A encryption properties.
 */
public final class S3AEncryption {

  private static final Logger LOG = LoggerFactory.getLogger(S3AEncryption.class);

  private S3AEncryption() {
  }

  /**
   * Get any SSE context from a configuration/credential provider.
   * @param bucket bucket to query for
   * @param conf configuration to examine
   * @return the encryption context value or ""
   * @throws IOException if reading a JCEKS file raised an IOE
   * @throws IllegalArgumentException bad arguments.
   */
  public static String getS3EncryptionContext(String bucket, Configuration conf)
      throws IOException {
    // look up the per-bucket value of the encryption context
    String encryptionContext = S3AUtils.lookupBucketSecret(bucket, conf, S3_ENCRYPTION_CONTEXT);
    if (encryptionContext == null) {
      // look up the global value of the encryption context
      encryptionContext = S3AUtils.lookupPassword(null, conf, S3_ENCRYPTION_CONTEXT);
    }
    if (encryptionContext == null) {
      // no encryption context, return ""
      return "";
    }
    return encryptionContext;
  }

  /**
   * Get any SSE context from a configuration/credential provider.
   * This includes converting the values to a base64-encoded UTF-8 string
   * holding JSON with the encryption context key-value pairs
   * @param bucket bucket to query for
   * @param conf configuration to examine
   * @param propagateExceptions should IO exceptions be rethrown?
   * @return the Base64 encryption context or ""
   * @throws IllegalArgumentException bad arguments.
   * @throws IOException if propagateExceptions==true and reading a JCEKS file raised an IOE
   */
  public static String getS3EncryptionContextBase64Encoded(
      String bucket,
      Configuration conf,
      boolean propagateExceptions) throws IOException {
    try {
      final String encryptionContextValue = getS3EncryptionContext(bucket, conf);
      if (StringUtils.isBlank(encryptionContextValue)) {
        return "";
      }
      final Map<String, String> encryptionContextMap = S3AUtils
          .getTrimmedStringCollectionSplitByEquals(encryptionContextValue);
      if (encryptionContextMap.isEmpty()) {
        return "";
      }
      final String encryptionContextJson = new ObjectMapper().writeValueAsString(
          encryptionContextMap);
      return Base64.encodeBase64String(encryptionContextJson.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      if (propagateExceptions) {
        throw e;
      }
      LOG.warn("Cannot retrieve {} for bucket {}",
          S3_ENCRYPTION_CONTEXT, bucket, e);
      return "";
    }
  }
}
