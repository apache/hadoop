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
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.util.Preconditions;

import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_CUSTOM_KEYRING_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.CSE_CUSTOM;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.CSE_KMS;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.CRYPTO_CEK_ALGORITHM;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.UNENCRYPTED_CONTENT_LENGTH;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CSE_PADDING_LENGTH;

/**
 * S3 client side encryption (CSE) utility class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class CSEUtils {

  private CSEUtils() {
  }

  /**
   * Checks if Client-Side Encryption (CSE) is enabled based on the encryption method.
   *
   * Validates if the provided encryption method matches either CSE-KMS or CSE-Custom
   * encryption methods. These are the two supported client-side encryption methods.
   *
   * @param encryptionMethod The encryption method to check (case-sensitive)
   * @return                 true if the encryption method is either CSE-KMS or CSE-Custom,
   *                         false otherwise
   * @see S3AEncryptionMethods#CSE_KMS
   * @see S3AEncryptionMethods#CSE_CUSTOM
   */
  public static boolean isCSEEnabled(String encryptionMethod) {
    return CSE_KMS.getMethod().equals(encryptionMethod) ||
        CSE_CUSTOM.getMethod().equals(encryptionMethod);
  }

  /**
   * Checks if an S3 object is encrypted by examining its metadata.
   *
   * This method performs a HEAD request on the object and checks for the presence
   * of encryption metadata (specifically the CEK algorithm indicator).
   *
   * @param store The S3AStore instance used to access the S3 object
   * @param key   The key (path) of the S3 object to check
   * @return      true if the object is encrypted (has CEK algorithm metadata),
   *              false otherwise
   * @throws IOException If there's an error accessing the object metadata or
   *                    communicating with S3
   */
  public static boolean isObjectEncrypted(S3AStore store,
      String key) throws IOException {
    HeadObjectResponse headObjectResponse = store.headObject(key,
        null,
        null,
        null,
        "getObjectMetadata");

    if (headObjectResponse.hasMetadata() &&
        headObjectResponse.metadata().get(CRYPTO_CEK_ALGORITHM) != null) {
      return true;
    }
    return false;
  }

  /**
   * Determines the actual unencrypted length of an S3 object.
   *
   * This method uses a three-step process to determine the object's unencrypted length:
   * 1. If the object is not encrypted, returns the original content length
   * 2. If encrypted, attempts to read the unencrypted length from object metadata
   * 3. If metadata is unavailable, calculates length by performing a ranged GET operation
   *
   * @param store              The S3AStore instance used to access the S3 object
   * @param key               The key (path) of the S3 object
   * @param contentLength     The encrypted object's content length
   * @param headObjectResponse The object's metadata from a HEAD request, may be null
   * @return                  The length of the object's unencrypted content
   * @throws IOException      If there's an error:
   *                         - accessing the object or its metadata
   *                         - parsing the unencrypted length from metadata
   *                         - performing the ranged GET operation
   *                         - computing the unencrypted length
   */
  public static long getUnencryptedObjectLength(S3AStore store,
      String key,
      long contentLength,
      HeadObjectResponse headObjectResponse) throws IOException {

    // if object is unencrypted, return the actual size
    if (!isObjectEncrypted(store, key)) {
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
      try (InputStream is = store.getRangedS3Object(key, minPlaintextLength, contentLength)) {
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
   * Creates encryption materials for client-side encryption based on the specified algorithm.
   *
   * Supports two types of client-side encryption:
   * <ul>
   *   <li>CSE_KMS: Uses AWS KMS for key management</li>
   *   <li>CSE_CUSTOM: Uses a custom cryptographic implementation</li>
   * </ul>
   *
   * @param conf      The configuration containing encryption settings
   * @param bucket    The S3 bucket name for which encryption materials are being created
   * @param algorithm The encryption algorithm to use (CSE_KMS or CSE_CUSTOM)
   * @return         CSEMaterials configured with the appropriate encryption settings
   * @throws IOException If there's an error retrieving encryption configuration
   * @throws IllegalArgumentException If:
   *                                 - KMS key ID is null or empty (for CSE_KMS)
   *                                 - Custom crypto class name is null or empty (for CSE_CUSTOM)
   *                                 - Unsupported encryption algorithm is specified
   */
  public static CSEMaterials getClientSideEncryptionMaterials(Configuration conf,
      String bucket,
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
