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

package org.apache.hadoop.fs.s3a.auth;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider;
import static org.apache.hadoop.fs.s3a.S3AUtils.loadAWSProviderClasses;

/**
 * Support IAM Assumed roles by instantiating an instance of
 * {@code STSAssumeRoleSessionCredentialsProvider} from configuration
 * properties, including wiring up the inner authenticator, and,
 * unless overridden, creating a session name from the current user.
 *
 * Classname is used in configuration files; do not move.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AssumedRoleCredentialProvider implements AWSCredentialsProvider,
    Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(AssumedRoleCredentialProvider.class);
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";

  static final String E_FORBIDDEN_PROVIDER =
      "AssumedRoleCredentialProvider cannot be in "
          + ASSUMED_ROLE_CREDENTIALS_PROVIDER;

  public static final String E_NO_ROLE = "Unset property "
      + ASSUMED_ROLE_ARN;

  private final STSAssumeRoleSessionCredentialsProvider stsProvider;

  private final String sessionName;

  private final long duration;

  private final String arn;

  /**
   * Instantiate.
   * This calls {@link #getCredentials()} to fail fast on the inner
   * role credential retrieval.
   * @param conf configuration
   * @throws IOException on IO problems and some parameter checking
   * @throws IllegalArgumentException invalid parameters
   * @throws AWSSecurityTokenServiceException problems getting credentials
   */
  public AssumedRoleCredentialProvider(Configuration conf) throws IOException {

    arn = conf.getTrimmed(ASSUMED_ROLE_ARN, "");
    if (StringUtils.isEmpty(arn)) {
      throw new IOException(E_NO_ROLE);
    }

    // build up the base provider
    Class<?>[] awsClasses = loadAWSProviderClasses(conf,
        ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.class);
    AWSCredentialProviderList credentials = new AWSCredentialProviderList();
    for (Class<?> aClass : awsClasses) {
      if (this.getClass().equals(aClass)) {
        throw new IOException(E_FORBIDDEN_PROVIDER);
      }
      credentials.add(createAWSCredentialProvider(conf, aClass));
    }

    // then the STS binding
    sessionName = conf.getTrimmed(ASSUMED_ROLE_SESSION_NAME,
        buildSessionName());
    duration = conf.getTimeDuration(ASSUMED_ROLE_SESSION_DURATION,
        ASSUMED_ROLE_SESSION_DURATION_DEFAULT, TimeUnit.SECONDS);
    String policy = conf.getTrimmed(ASSUMED_ROLE_POLICY, "");

    LOG.debug("{}", this);
    STSAssumeRoleSessionCredentialsProvider.Builder builder
        = new STSAssumeRoleSessionCredentialsProvider.Builder(arn, sessionName);
    builder.withRoleSessionDurationSeconds((int) duration);
    if (StringUtils.isNotEmpty(policy)) {
      LOG.debug("Scope down policy {}", policy);
      builder.withScopeDownPolicy(policy);
    }
    String epr = conf.get(ASSUMED_ROLE_STS_ENDPOINT, "");
    if (StringUtils.isNotEmpty(epr)) {
      LOG.debug("STS Endpoint: {}", epr);
      builder.withServiceEndpoint(epr);
    }
    LOG.debug("Credentials to obtain role credentials: {}", credentials);
    builder.withLongLivedCredentialsProvider(credentials);
    stsProvider = builder.build();
    // and force in a fail-fast check just to keep the stack traces less
    // convoluted
    getCredentials();
  }

  /**
   * Get credentials.
   * @return the credentials
   * @throws AWSSecurityTokenServiceException if none could be obtained.
   */
  @Override
  public AWSCredentials getCredentials() {
    try {
      return stsProvider.getCredentials();
    } catch (AWSSecurityTokenServiceException e) {
      LOG.error("Failed to get credentials for role {}",
          arn, e);
      throw e;
    }
  }

  @Override
  public void refresh() {
    stsProvider.refresh();
  }

  /**
   * Propagate the close() call to the inner stsProvider.
   */
  @Override
  public void close() {
    stsProvider.close();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AssumedRoleCredentialProvider{");
    sb.append("role='").append(arn).append('\'');
    sb.append(", session'").append(sessionName).append('\'');
    sb.append(", duration=").append(duration);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Build the session name from the current user's shortname.
   * @return a string for the session name.
   * @throws IOException failure to get the current user
   */
  static String buildSessionName() throws IOException {
    return sanitize(UserGroupInformation.getCurrentUser()
        .getShortUserName());
  }

  /**
   * Build a session name from the string, sanitizing it for the permitted
   * characters.
   * @param session source session
   * @return a string for use in role requests.
   */
  @VisibleForTesting
  static String sanitize(String session) {
    StringBuilder r = new StringBuilder(session.length());
    for (char c: session.toCharArray()) {
      if ("abcdefghijklmnopqrstuvwxyz0123456789,.@-".contains(
          Character.toString(c).toLowerCase(Locale.ENGLISH))) {
        r.append(c);
      } else {
        r.append('-');
      }
    }
    return r.toString();
  }

}
