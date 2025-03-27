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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.s3a.S3AUtils.getAWSAccessKeys;

/**
 * Support simple credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SimpleAWSCredentialsProvider implements AwsCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  private final String accessKey;
  private final String secretKey;

  /**
   * Build the credentials from a filesystem URI and configuration.
   * @param uri FS URI
   * @param conf configuration containing secrets/references to.
   * @throws IOException failure
   */
  public SimpleAWSCredentialsProvider(final URI uri, final Configuration conf)
      throws IOException {
    this(getAWSAccessKeys(uri, conf));
  }

  /**
   * Instantiate from a login tuple.
   * For testing, hence package-scoped.
   * @param login login secrets
   * @throws IOException failure
   */
  @VisibleForTesting
  SimpleAWSCredentialsProvider(final S3xLoginHelper.Login login)
      throws IOException {
    this.accessKey = login.getUser();
    this.secretKey = login.getPassword();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return AwsBasicCredentials.create(accessKey, secretKey);
    }
    throw new NoAwsCredentialsException("SimpleAWSCredentialsProvider",
        "No AWS credentials in the Hadoop configuration");
  }

  @Override
  public String toString() {
    return "SimpleAWSCredentialsProvider{" +
        "accessKey.empty=" + accessKey.isEmpty() +
        ", secretKey.empty=" + secretKey.isEmpty() +
        '}';
  }

}
