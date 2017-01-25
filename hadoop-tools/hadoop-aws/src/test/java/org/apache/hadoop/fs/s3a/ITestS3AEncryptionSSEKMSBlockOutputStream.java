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

import org.apache.hadoop.conf.Configuration;

/**
 * Run the encryption tests against the Fast output stream.
 * This verifies that both file writing paths can encrypt their data.
 */
public class ITestS3AEncryptionSSEKMSBlockOutputStream
    extends AbstractTestS3AEncryption {

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(Constants.FAST_UPLOAD, true);
    conf.set(Constants.FAST_UPLOAD_BUFFER,
        Constants.FAST_UPLOAD_BYTEBUFFER);
    return conf;
  }

  @Override
  protected String getSSEAlgorithm() {
    return S3AEncryptionMethods.SSE_KMS.getMethod();
  }
}
