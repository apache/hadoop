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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.util.Optional;

import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;

/**
 * These support operations on {@link EncryptionSecrets} which use the AWS SDK
 * operations. Isolating them here ensures that that class is not required on
 * the classpath.
 */
public final class EncryptionSecretOperations {

  private EncryptionSecretOperations() {
  }

  /***
   * Gets the SSE-C client side key if present.
   *
   * @param secrets source of the encryption secrets.
   * @return an optional key to attach to a request.
   */
  public static Optional<String> getSSECustomerKey(final EncryptionSecrets secrets) {
    if (secrets.hasEncryptionKey() && secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_C) {
      return Optional.of(secrets.getEncryptionKey());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Gets the SSE-KMS key if present, else let S3 use AWS managed key.
   *
   * @param secrets source of the encryption secrets.
   * @return an optional key to attach to a request.
   */
  public static Optional<String> getSSEAwsKMSKey(final EncryptionSecrets secrets) {
    if ((secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_KMS
        || secrets.getEncryptionMethod() == S3AEncryptionMethods.DSSE_KMS)
        && secrets.hasEncryptionKey()) {
      return Optional.of(secrets.getEncryptionKey());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Gets the SSE-KMS context if present, else don't set it in the S3 request.
   *
   * @param secrets source of the encryption secrets.
   * @return an optional AWS KMS encryption context to attach to a request.
   */
  public static Optional<String> getSSEAwsKMSEncryptionContext(final EncryptionSecrets secrets) {
    if ((secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_KMS
        || secrets.getEncryptionMethod() == S3AEncryptionMethods.DSSE_KMS)
        && secrets.hasEncryptionContext()) {
      return Optional.of(secrets.getEncryptionContext());
    } else {
      return Optional.empty();
    }
  }
}
