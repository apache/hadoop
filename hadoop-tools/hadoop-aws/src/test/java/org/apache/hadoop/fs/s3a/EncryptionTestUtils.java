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

import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public final class EncryptionTestUtils {

  /** Private constructor */
  private EncryptionTestUtils() {
  }

  public static final String AWS_KMS_SSE_ALGORITHM = "aws:kms";

  public static final String AWS_KMS_DSSE_ALGORITHM = "aws:kms:dsse";

  public static final String SSE_C_ALGORITHM = "AES256";

  /**
   * Decodes the SERVER_SIDE_ENCRYPTION_KEY from base64 into an AES key, then
   * gets the md5 of it, then encodes it in base64 so it will match the version
   * that AWS returns to us.
   *
   * @return md5'd base64 encoded representation of the server side encryption
   * key
   */
  public static String convertKeyToMd5(FileSystem fs) {
    String base64Key = fs.getConf().getTrimmed(
        S3_ENCRYPTION_KEY
    );
    byte[] key = Base64.decodeBase64(base64Key);
    byte[] md5 =  DigestUtils.md5(key);
    return Base64.encodeBase64String(md5).trim();
  }

  /**
   * Assert that a path is encrypted with right encryption settings.
   * @param path file path.
   * @param algorithm encryption algorithm.
   * @param kmsKeyArn full kms key.
   */
  public static void assertEncrypted(S3AFileSystem fs,
                                     final Path path,
                                     final S3AEncryptionMethods algorithm,
                                     final String kmsKeyArn)
          throws IOException {
    HeadObjectResponse md = fs.getS3AInternals().getObjectMetadata(path);
    String details = String.format(
            "file %s with encryption algorithm %s and key %s",
            path,
            md.serverSideEncryptionAsString(),
            md.ssekmsKeyId());
    switch(algorithm) {
    case SSE_C:
      assertThat(md.serverSideEncryptionAsString())
          .describedAs("Details of the server-side encryption algorithm used: %s", details)
          .isNull();
      assertThat(md.sseCustomerAlgorithm())
          .describedAs("Details of SSE-C algorithm: %s", details)
          .isEqualTo(SSE_C_ALGORITHM);
      String md5Key = convertKeyToMd5(fs);
      assertThat(md.sseCustomerKeyMD5())
          .describedAs("Details of the customer provided encryption key: %s", details)
          .isEqualTo(md5Key);
      break;
    case SSE_KMS:
      assertThat(md.serverSideEncryptionAsString())
          .describedAs("Details of the server-side encryption algorithm used: %s", details)
          .isEqualTo(AWS_KMS_SSE_ALGORITHM);
      assertThat(md.ssekmsKeyId())
          .describedAs("Details of the KMS key: %s", details)
          .isEqualTo(kmsKeyArn);
      break;
    case DSSE_KMS:
      assertThat(md.serverSideEncryptionAsString())
          .describedAs("Details of the server-side encryption algorithm used: %s", details)
          .isEqualTo(AWS_KMS_DSSE_ALGORITHM);
      assertThat(md.ssekmsKeyId())
          .describedAs("Details of the KMS key: %s", details)
          .isEqualTo(kmsKeyArn);
      break;
    default:
      assertThat(md.serverSideEncryptionAsString())
          .isEqualTo("AES256");
    }
  }

}
