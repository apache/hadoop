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
import org.apache.commons.lang.StringUtils;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Support session credentials for authenticating with AWS.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class TemporaryAWSCredentialsProvider implements AWSCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;

  public TemporaryAWSCredentialsProvider(URI uri, Configuration conf) {
    this.accessKey = conf.get(ACCESS_KEY, null);
    this.secretKey = conf.get(SECRET_KEY, null);
    this.sessionToken = conf.get(SESSION_TOKEN, null);
  }

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
