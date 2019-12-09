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

/**
 * This class is only a holder for bucket, key, SSE Algorithm and SSE key
 * attributes. It is only used in {@link S3AInputStream}
 * as a way to reduce parameters being passed
 * to the constructor of such class.
 */
class S3ObjectAttributes {
  private String bucket;
  private String key;
  private S3AEncryptionMethods serverSideEncryptionAlgorithm;
  private String serverSideEncryptionKey;

  S3ObjectAttributes(
      String bucket,
      String key,
      S3AEncryptionMethods serverSideEncryptionAlgorithm,
      String serverSideEncryptionKey) {
    this.bucket = bucket;
    this.key = key;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;
    this.serverSideEncryptionKey = serverSideEncryptionKey;
  }

  String getBucket() {
    return bucket;
  }

  String getKey() {
    return key;
  }

  S3AEncryptionMethods getServerSideEncryptionAlgorithm() {
    return serverSideEncryptionAlgorithm;
  }

  String getServerSideEncryptionKey() {
    return serverSideEncryptionKey;
  }
}
