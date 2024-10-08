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
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.util.Preconditions;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_CUSTOM_KEYRING_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.CSE_CUSTOM;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.CSE_KMS;
import static org.apache.hadoop.fs.s3a.S3AUtils.formatRange;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.CRYPTO_CEK_ALGORITHM;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.UNENCRYPTED_CONTENT_LENGTH;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CSE_PADDING_LENGTH;

/**
 * S3 client side encryption (CSE) utility class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CSEUtils {

  private CSEUtils() {
  }

  /**
   * Checks if the file suffix ends CSE file suffix.
   * {@link org.apache.hadoop.fs.s3a.Constants#S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX}
   * when the config
   * @param key file name
   * @return true if file name ends with CSE instruction file suffix
   */
  public static boolean isCSEInstructionFile(String key) {
    return key.endsWith(S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX);
  }

  /**
   * Checks if CSE-KMS or CSE-CUSTOM is set.
   * @param encryptionMethod type of encryption used
   * @return true if encryption method is CSE-KMS or CSE-CUSTOM
   */
  public static boolean isCSEEnabled(String encryptionMethod) {
    return CSE_KMS.getMethod().equals(encryptionMethod) ||
        CSE_CUSTOM.getMethod().equals(encryptionMethod);
  }

  /**
   * Checks if a given S3 object is encrypted or not by checking following two conditions
   * 1. if object metadata contains x-amz-cek-alg
   * 2. if instruction file is present
   *
   * @param s3Client S3 client
   * @param factory   S3 request factory
   * @param key      key value of the s3 object
   * @return true if S3 object is encrypted
   */
  public static boolean isObjectEncrypted(S3Client s3Client, RequestFactory factory, String key) {
    HeadObjectRequest.Builder requestBuilder = factory.newHeadObjectRequestBuilder(key);
    HeadObjectResponse headObjectResponse = s3Client.headObject(requestBuilder.build());
    if (headObjectResponse.hasMetadata() &&
        headObjectResponse.metadata().get(CRYPTO_CEK_ALGORITHM) != null) {
      return true;
    }
    HeadObjectRequest.Builder instructionFileRequestBuilder =
        factory.newHeadObjectRequestBuilder(key + S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX);
    try {
      s3Client.headObject(instructionFileRequestBuilder.build());
      return true;
    } catch (NoSuchKeyException e) {
      // Ignore. This indicates no instruction file is present
    }
    return false;
  }

  /**
   * Get the unencrypted length of the object.
   *
   * @param s3Client           S3 client
   * @param bucket             bucket name of the s3 object
   * @param key                key value of the s3 object
   * @param factory            S3 request factory
   * @param contentLength      S3 object length
   * @param headObjectResponse response from headObject call
   * @return unencrypted length of the object
   * @throws IOException IO failures
   */
  public static long getUnPaddedObjectLength(S3Client s3Client,
      String bucket,
      String key,
      RequestFactory factory,
      long contentLength,
      HeadObjectResponse headObjectResponse) throws IOException {

    // if object is unencrypted, return the actual size
    if (!isObjectEncrypted(s3Client, factory, key)) {
      return contentLength;
    }

    // check if unencrypted content length metadata is present or not.
    if (headObjectResponse != null) {
      String plaintextLength = headObjectResponse.metadata().get(UNENCRYPTED_CONTENT_LENGTH);
      if (headObjectResponse.hasMetadata() && plaintextLength != null
          && !plaintextLength.isEmpty()) {
        return Long.parseLong(plaintextLength);
      }
    }

    // identify the length by doing a ranged GET operation.
    if (contentLength >= CSE_PADDING_LENGTH) {
      long minPlaintextLength = contentLength - CSE_PADDING_LENGTH;
      if (minPlaintextLength < 0) {
        minPlaintextLength = 0;
      }
      GetObjectRequest getObjectRequest = GetObjectRequest.builder()
          .bucket(bucket)
          .key(key)
          .range(formatRange(minPlaintextLength, contentLength))
          .build();
      try (InputStream is = s3Client.getObject(getObjectRequest)) {
        int i = 0;
        while (is.read() != -1) {
          i++;
        }
        return minPlaintextLength + i;
      } catch (Exception e) {
        throw new IOException("Failed to compute unencrypted length", e);
      }
    }
    return contentLength;
  }

  /**
   * Configure CSE params based on encryption algorithm.
   * @param conf Configuration
   * @param bucket bucket name
   * @param algorithm encryption algorithm
   * @return CSEMaterials
   * @throws IOException IO failures
   */
  public static CSEMaterials getClientSideEncryptionMaterials(Configuration conf, String bucket,
      S3AEncryptionMethods algorithm) throws IOException {
    switch (algorithm) {
    case CSE_KMS:
      String kmsKeyId = getS3EncryptionKey(bucket, conf, true);
      Preconditions.checkArgument(kmsKeyId != null && !kmsKeyId.isEmpty(),
          "KMS keyId cannot be null or empty");
      return new CSEMaterials()
          .withCSEKeyType(CSEMaterials.CSEKeyType.KMS)
          .withConf(conf)
          .withKmsKeyId(kmsKeyId);
    case CSE_CUSTOM:
      String customCryptoClassName = conf.getTrimmed(S3_ENCRYPTION_CSE_CUSTOM_KEYRING_CLASS_NAME);
      Preconditions.checkArgument(customCryptoClassName != null &&
              !customCryptoClassName.isEmpty(),
          "CSE custom cryptographic class name cannot be null or empty");
      return new CSEMaterials()
          .withCSEKeyType(CSEMaterials.CSEKeyType.CUSTOM)
          .withConf(conf)
          .withCustomCryptographicClassName(customCryptoClassName);
    default:
      throw new IllegalArgumentException("Invalid client side encryption algorithm."
          + " Only CSE-KMS and CSE-CUSTOM are supported");
    }
  }
}
