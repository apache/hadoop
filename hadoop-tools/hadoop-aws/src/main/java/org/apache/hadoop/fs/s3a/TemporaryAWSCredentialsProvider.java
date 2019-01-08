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
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.lookupPassword;

/**
 * Support session credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TemporaryAWSCredentialsProvider implements AWSCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  private String accessKey;
  private String secretKey;
  private String sessionToken;

  public TemporaryAWSCredentialsProvider(Configuration conf)
      throws IOException {
    this(null, conf);
  }

  public TemporaryAWSCredentialsProvider(URI uri, Configuration conf)
      throws IOException {

      // determine the bucket
      String bucket = uri != null ? uri.getHost():  "";
      Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
          conf, S3AFileSystem.class);
      this.accessKey = lookupPassword(bucket, c, ACCESS_KEY);
      this.secretKey = lookupPassword(bucket, c, SECRET_KEY);
      this.sessionToken = lookupPassword(bucket, c, SESSION_TOKEN);
  }

  @Override
  public AWSCredentials getCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)
        && !StringUtils.isEmpty(sessionToken)) {
      return new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    }
    throw new CredentialInitializationException(
        "Access key, secret key or session token is unset");
  }

  @Override
  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
