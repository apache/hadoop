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
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AnonymousAWSCredentialsProvider supports anonymous access to AWS services
 * through the AWS SDK.  AWS requests will not be signed.  This is not suitable
 * for most cases, because allowing anonymous access to an S3 bucket compromises
 * security.  This can be useful for accessing public data sets without
 * requiring AWS credentials.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class AnonymousAWSCredentialsProvider implements AWSCredentialsProvider {
  public AWSCredentials getCredentials() {
    return new AnonymousAWSCredentials();
  }

  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
