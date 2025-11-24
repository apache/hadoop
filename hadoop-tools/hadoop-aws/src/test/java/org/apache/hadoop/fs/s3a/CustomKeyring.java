/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.encryption.s3.materials.DecryptionMaterials;
import software.amazon.encryption.s3.materials.EncryptedDataKey;
import software.amazon.encryption.s3.materials.EncryptionMaterials;
import software.amazon.encryption.s3.materials.Keyring;
import software.amazon.encryption.s3.materials.KmsKeyring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_DEFAULT_REGION;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_KMS_REGION;

/**
 * Custom Keyring implementation.
 * This is used for testing {@link ITestS3AClientSideEncryptionCustom}.
 */
public class CustomKeyring implements Keyring {
  private final KmsClient kmsClient;
  private final Configuration conf;
  private final KmsKeyring kmsKeyring;


  public CustomKeyring(Configuration conf) throws IOException {
    this.conf = conf;
    String bucket = S3ATestUtils.getFsName(conf);
    kmsClient = KmsClient.builder()
        .region(Region.of(conf.get(S3_ENCRYPTION_CSE_KMS_REGION, AWS_S3_DEFAULT_REGION)))
        .credentialsProvider(new TemporaryAWSCredentialsProvider(
            new Path(bucket).toUri(), conf))
        .build();
    kmsKeyring = KmsKeyring.builder()
        .kmsClient(kmsClient)
        .wrappingKeyId(S3AUtils.getS3EncryptionKey(bucket, conf))
        .build();
  }

  @Override
  public EncryptionMaterials onEncrypt(EncryptionMaterials encryptionMaterials) {
    return kmsKeyring.onEncrypt(encryptionMaterials);
  }

  @Override
  public DecryptionMaterials onDecrypt(DecryptionMaterials decryptionMaterials,
      List<EncryptedDataKey> list) {
    return kmsKeyring.onDecrypt(decryptionMaterials, list);
  }
}
