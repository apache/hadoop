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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * S3 Client factory used for testing with eventual consistency fault injection.
 * This client is for testing <i>only</i>; it is in the production
 * {@code hadoop-aws} module to enable integration tests to use this
 * just by editing the Hadoop configuration used to bring up the client.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InconsistentS3ClientFactory extends DefaultS3ClientFactory {

  /**
   * Create the inconsistent client.
   * Logs a warning that this is being done.
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @return an inconsistent client.
   */
  @Override
  protected AmazonS3 newAmazonS3Client(AWSCredentialsProvider credentials,
      ClientConfiguration awsConf) {
    LOG.warn("** FAILURE INJECTION ENABLED.  Do not run in production! **");
    return new InconsistentAmazonS3Client(credentials, awsConf, getConf());
  }
}
