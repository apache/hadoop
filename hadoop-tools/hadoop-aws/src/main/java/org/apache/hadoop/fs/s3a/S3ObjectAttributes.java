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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class is only a holder for bucket, key, SSE Algorithm and SSE key
 * attributes. It is used in {@link S3AInputStream} and the select equivalent.
 * as a way to reduce parameters being passed
 * to the constructor of such class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3ObjectAttributes {
  private final String bucket;
  private final String key;
  private final S3AEncryptionMethods serverSideEncryptionAlgorithm;
  private final String serverSideEncryptionKey;

  public S3ObjectAttributes(
      String bucket,
      String key,
      S3AEncryptionMethods serverSideEncryptionAlgorithm,
      String serverSideEncryptionKey) {
    this.bucket = bucket;
    this.key = key;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;
    this.serverSideEncryptionKey = serverSideEncryptionKey;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return serverSideEncryptionAlgorithm;
  }

  public String getServerSideEncryptionKey() {
    return serverSideEncryptionKey;
  }
}
