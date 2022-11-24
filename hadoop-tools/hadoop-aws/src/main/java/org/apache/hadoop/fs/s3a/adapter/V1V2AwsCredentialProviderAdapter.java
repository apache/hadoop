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

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public interface V1V2AwsCredentialProviderAdapter extends AWSCredentialsProvider,
    AwsCredentialsProvider {

  /**
   * Creates a two-way adapter from a V1 {@link AWSCredentialsProvider} interface.
   *
   * @param v1CredentialsProvider V1 credentials provider.
   * @return Two-way credential provider adapter.
   */
  static V1V2AwsCredentialProviderAdapter adapt(AWSCredentialsProvider v1CredentialsProvider) {
    return V1ToV2AwsCredentialProviderAdapter.create(v1CredentialsProvider);
  }
}
