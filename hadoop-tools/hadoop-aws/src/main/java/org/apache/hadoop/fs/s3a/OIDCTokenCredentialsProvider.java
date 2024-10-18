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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;

import org.slf4j.Logger;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Support OpenID Connect (OIDC) token for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 * This credential provider must not fail in creation because that will
 * break a chain of credential providers.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OIDCTokenCredentialsProvider implements AWSCredentialsProvider {
  public static final String NAME
          = "org.apache.hadoop.fs.s3a.OIDCTokenCredentialsProvider";

  /** Reuse the S3AFileSystem log. */
  private static final Logger LOG = S3AFileSystem.LOG;

  private String jwtPath;
  private String roleARN;
  private String sessionName;
  private IOException lookupIOE;

  public OIDCTokenCredentialsProvider(Configuration conf) {
    try {
      Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
                  conf, S3AFileSystem.class);
      this.jwtPath = S3AUtils.lookupPassword(c, JWT_PATH, null);
      this.roleARN = S3AUtils.lookupPassword(c, ASSUMED_ROLE_ARN, null);
      this.sessionName = S3AUtils.lookupPassword(c, ASSUMED_ROLE_SESSION_NAME, null);
    } catch (IOException e) {
      lookupIOE = e;
    }
  }

  public AWSCredentials getCredentials() {
      if (lookupIOE != null) {
          // propagate any initialization problem
          throw new CredentialInitializationException(lookupIOE.toString(),
                  lookupIOE);
      }

      LOG.debug("jwtPath {} roleARN {} sessionName {}", jwtPath, roleARN, sessionName);

      if (!StringUtils.isEmpty(jwtPath) && !StringUtils.isEmpty(roleARN)) {
          final AWSCredentialsProvider credentialsProvider =
              WebIdentityTokenCredentialsProvider.builder()
                  .webIdentityTokenFile(jwtPath)
                  .roleArn(roleARN)
                  .roleSessionName(sessionName)
                  .build();
          return credentialsProvider.getCredentials();
      }
      else throw new CredentialInitializationException(
              "OIDC token path or role ARN is null");
  }

  public void refresh() {}

  @Override
  public String toString() {
      return String.format("%s " +
                  "jwtPath {%s} roleARN {%s} sessionName {%s}",
              getClass().getSimpleName(),
              jwtPath, roleARN, sessionName);
  }
}