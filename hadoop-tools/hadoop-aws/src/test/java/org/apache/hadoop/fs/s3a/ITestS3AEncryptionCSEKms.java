/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfKmsKeyIdIsNotSet;

/**
 * Testing S3A client-side encryption/decryption with KMS method.
 */
public class ITestS3AEncryptionCSEKms extends ITestS3AEncryptionCSE {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.CLIENT_SIDE_ENCRYPTION_METHOD,
            S3AClientEncryptionMethods.KMS.getMethod());
    return conf;
  }

  @Override
  protected void skipTest() {
    skipIfEncryptionTestsDisabled(getConfiguration());
    skipIfKmsKeyIdIsNotSet(getConfiguration());
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);
    assertEquals("kms", md.getUserMetaDataOf("x-amz-wrap-alg"));
    String keyId = getConfiguration()
            .get(Constants.CLIENT_SIDE_ENCRYPTION_KMS_KEY_ID);
    assertTrue("Kms key is same",
            md.getUserMetaDataOf("x-amz-matdesc").contains(keyId));
  }
}
