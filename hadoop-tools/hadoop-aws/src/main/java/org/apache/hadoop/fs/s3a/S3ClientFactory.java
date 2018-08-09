/**
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
import java.net.URI;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Factory for creation of {@link AmazonS3} client instances.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface S3ClientFactory {

  /**
   * Creates a new {@link AmazonS3} client.
   *
   * @param name raw input S3A file system URI
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentialSet credentials to use
   * @return S3 client
   * @throws IOException IO problem
   */
  AmazonS3 createS3Client(URI name,
      final String bucket,
      final AWSCredentialsProvider credentialSet) throws IOException;

}
