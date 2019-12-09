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

import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-KMS encryption when no KMS encryption key is provided and AWS
 * uses the default.  Since this resource changes for every account and region,
 * there is no good way to explicitly set this value to do a equality check
 * in the response.
 */
public class ITestS3AEncryptionSSEKMSDefaultKey
    extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, "");
    return conf;
  }

  @Override
  protected S3AEncryptionMethods getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_KMS;
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);
    assertEquals("aws:kms", md.getSSEAlgorithm());
    assertThat(md.getSSEAwsKmsKeyId(), containsString("arn:aws:kms:"));
  }
}
