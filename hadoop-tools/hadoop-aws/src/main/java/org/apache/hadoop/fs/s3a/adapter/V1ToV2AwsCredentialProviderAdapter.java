/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.adapter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Adapts a V1 {@link AWSCredentialsProvider} to the V2 {@link AwsCredentialsProvider} interface.
 * Implements both interfaces so can be used with either the V1 or V2 AWS SDK.
 */
final class V1ToV2AwsCredentialProviderAdapter implements V1V2AwsCredentialProviderAdapter {

  private final AWSCredentialsProvider v1CredentialsProvider;

  private V1ToV2AwsCredentialProviderAdapter(AWSCredentialsProvider v1CredentialsProvider) {
    this.v1CredentialsProvider = v1CredentialsProvider;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AWSCredentials toAdapt = v1CredentialsProvider.getCredentials();
    if (toAdapt instanceof AWSSessionCredentials) {
      return AwsSessionCredentials.create(toAdapt.getAWSAccessKeyId(),
          toAdapt.getAWSSecretKey(),
          ((AWSSessionCredentials) toAdapt).getSessionToken());
    } else if (toAdapt instanceof AnonymousAWSCredentials) {
      return AnonymousCredentialsProvider.create().resolveCredentials();
    } else {
      return AwsBasicCredentials.create(toAdapt.getAWSAccessKeyId(), toAdapt.getAWSSecretKey());
    }
  }

  @Override
  public AWSCredentials getCredentials() {
    return v1CredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    v1CredentialsProvider.refresh();
  }

  /**
   * @param v1CredentialsProvider V1 credential provider to adapt.
   * @return A new instance of the credentials provider adapter.
   */
  static V1ToV2AwsCredentialProviderAdapter create(AWSCredentialsProvider v1CredentialsProvider) {
    return new V1ToV2AwsCredentialProviderAdapter(v1CredentialsProvider);
  }
}
