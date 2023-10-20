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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.StsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.CredentialInitializationException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Sets;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory.buildAWSProviderList;

/**
 * Support IAM Assumed roles by instantiating an instance of
 * {@code STSAssumeRoleSessionCredentialsProvider} from configuration
 * properties, including wiring up the inner authenticator, and,
 * unless overridden, creating a session name from the current user.
 *
 * Classname is used in configuration files; do not move.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AssumedRoleCredentialProvider implements AwsCredentialsProvider,
    Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(AssumedRoleCredentialProvider.class);
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";

  public static final String E_NO_ROLE = "Unset property "
      + ASSUMED_ROLE_ARN;

  private final StsAssumeRoleCredentialsProvider stsProvider;

  private final String sessionName;

  private final long duration;

  private final String arn;

  private final AWSCredentialProviderList credentialsToSTS;

  private final Invoker invoker;

  private final StsClient stsClient;

  /**
   * Instantiate.
   * This calls {@link #resolveCredentials()} to fail fast on the inner
   * role credential retrieval.
   * @param fsUri possibly null URI of the filesystem.
   * @param conf configuration
   * @throws IOException on IO problems and some parameter checking
   * @throws IllegalArgumentException invalid parameters
   * @throws StsException problems getting credentials
   */
  public AssumedRoleCredentialProvider(@Nullable URI fsUri, Configuration conf)
      throws IOException {

    arn = conf.getTrimmed(ASSUMED_ROLE_ARN, "");
    if (StringUtils.isEmpty(arn)) {
      throw new PathIOException(String.valueOf(fsUri), E_NO_ROLE);
    }

    // build up the base provider
    credentialsToSTS = buildAWSProviderList(fsUri, conf,
        ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        Arrays.asList(
            SimpleAWSCredentialsProvider.class,
            EnvironmentVariableCredentialsProvider.class),
        Sets.newHashSet(getClass()));
    LOG.debug("Credentials used to obtain role credentials: {}", credentialsToSTS);

    // then the STS binding
    sessionName = conf.getTrimmed(ASSUMED_ROLE_SESSION_NAME,
        buildSessionName());
    duration = conf.getTimeDuration(ASSUMED_ROLE_SESSION_DURATION,
        ASSUMED_ROLE_SESSION_DURATION_DEFAULT, TimeUnit.SECONDS);
    String policy = conf.getTrimmed(ASSUMED_ROLE_POLICY, "");

    LOG.debug("{}", this);

    AssumeRoleRequest.Builder requestBuilder =
        AssumeRoleRequest.builder().roleArn(arn).roleSessionName(sessionName)
            .durationSeconds((int) duration);

    if (StringUtils.isNotEmpty(policy)) {
      LOG.debug("Scope down policy {}", policy);
      requestBuilder.policy(policy);
    }

    String endpoint = conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT, "");
    String region = conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT_REGION,
        ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT);
    stsClient =
        STSClientFactory.builder(
          conf,
          fsUri != null ?  fsUri.getHost() : "",
          credentialsToSTS,
          endpoint,
          region).build();

    //now build the provider
    stsProvider = StsAssumeRoleCredentialsProvider.builder()
        .refreshRequest(requestBuilder.build())
        .stsClient(stsClient).build();

    // to handle STS throttling by the AWS account, we
    // need to retry
    invoker = new Invoker(new S3ARetryPolicy(conf), this::operationRetried);

    // and force in a fail-fast check just to keep the stack traces less
    // convoluted
    resolveCredentials();
  }

  /**
   * Get credentials.
   * @return the credentials
   * @throws StsException if none could be obtained.
   */
  @Override
  @Retries.RetryRaw
  public AwsCredentials resolveCredentials() {
    try {
      return invoker.retryUntranslated("resolveCredentials",
          true,
          stsProvider::resolveCredentials);
    } catch (IOException e) {
      // this is in the signature of retryUntranslated;
      // its hard to see how this could be raised, but for
      // completeness, it is wrapped as an Amazon Client Exception
      // and rethrown.
      throw new CredentialInitializationException(
          "getCredentials failed: " + e,
          e);
    } catch (SdkClientException e) {
      LOG.error("Failed to resolve credentials for role {}",
          arn, e);
      throw e;
    }
  }

  /**
   * Propagate the close() call to the inner stsProvider.
   */
  @Override
  public void close() {
    S3AUtils.closeAutocloseables(LOG, stsProvider, credentialsToSTS, stsClient);
  }

  @Override
  public String toString() {
    String sb = "AssumedRoleCredentialProvider{" + "role='" + arn + '\''
        + ", session'" + sessionName + '\''
        + ", duration=" + duration
        + '}';
    return sb;
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

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  public void operationRetried(
      String text,
      Exception ex,
      int retries,
      boolean idempotent) {
    if (retries == 0) {
      // log on the first retry attempt of the credential access.
      // At worst, this means one log entry every intermittent renewal
      // time.
      LOG.info("Retried {}", text);
    }
  }
}
