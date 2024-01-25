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

import com.amazonaws.services.s3.model.ObjectMetadata;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class EncryptionTestUtils {

  /** Private constructor */
  private EncryptionTestUtils() {
  }

  public static final String AWS_KMS_SSE_ALGORITHM = "aws:kms";

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
            SERVER_SIDE_ENCRYPTION_KEY
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
    ObjectMetadata md = fs.getObjectMetadata(path);
    String details = String.format(
            "file %s with encryption algorithm %s and key %s",
            path,
            md.getSSEAlgorithm(),
            md.getSSEAwsKmsKeyId());
    switch(algorithm) {
    case SSE_C:
      assertNull("Metadata algorithm should have been null in "
                      + details,
              md.getSSEAlgorithm());
      assertEquals("Wrong SSE-C algorithm in "
                      + details,
              SSE_C_ALGORITHM, md.getSSECustomerAlgorithm());
      String md5Key = convertKeyToMd5(fs);
      assertEquals("getSSECustomerKeyMd5() wrong in " + details,
              md5Key, md.getSSECustomerKeyMd5());
      break;
    case SSE_KMS:
      assertEquals("Wrong algorithm in " + details,
              AWS_KMS_SSE_ALGORITHM, md.getSSEAlgorithm());
      assertEquals("Wrong KMS key in " + details,
              kmsKeyArn,
              md.getSSEAwsKmsKeyId());
      break;
    default:
      assertEquals("AES256", md.getSSEAlgorithm());
    }
  }

}
