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

import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;

import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;

/**
 * These support operations on {@link EncryptionSecrets} which use the AWS SDK
 * operations. Isolating them here ensures that that class is not required on
 * the classpath.
 */
public final class EncryptionSecretOperations {

  private EncryptionSecretOperations() {
  }

  /**
   * Create SSE-C client side key encryption options on demand.
   * @return an optional key to attach to a request.
   * @param secrets source of the encryption secrets.
   */
  public static Optional<SSECustomerKey> createSSECustomerKey(
      final EncryptionSecrets secrets) {
    if (secrets.hasEncryptionKey() &&
        secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_C) {
      return Optional.of(new SSECustomerKey(secrets.getEncryptionKey()));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Create SSE-KMS options for a request, iff the encryption is SSE-KMS.
   * @return an optional SSE-KMS param to attach to a request.
   * @param secrets source of the encryption secrets.
   */
  public static Optional<SSEAwsKeyManagementParams> createSSEAwsKeyManagementParams(
      final EncryptionSecrets secrets) {

    //Use specified key, otherwise default to default master aws/s3 key by AWS
    if (secrets.getEncryptionMethod() == S3AEncryptionMethods.SSE_KMS) {
      if (secrets.hasEncryptionKey()) {
        return Optional.of(new SSEAwsKeyManagementParams(
            secrets.getEncryptionKey()));
      } else {
        return Optional.of(new SSEAwsKeyManagementParams());
      }
    } else {
      return Optional.empty();
    }
  }
}
