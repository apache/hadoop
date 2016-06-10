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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * BasicAWSCredentialsProvider supports static configuration of access key ID
 * and secret access key for use with the AWS SDK.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BasicAWSCredentialsProvider implements AWSCredentialsProvider {
  private final String accessKey;
  private final String secretKey;

  public BasicAWSCredentialsProvider(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  public AWSCredentials getCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return new BasicAWSCredentials(accessKey, secretKey);
    }
    throw new CredentialInitializationException(
        "Access key or secret key is null");
  }

  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
