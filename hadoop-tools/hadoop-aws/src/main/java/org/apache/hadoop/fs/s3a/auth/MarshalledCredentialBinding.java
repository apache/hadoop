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

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.ProviderUtils;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SESSION_TOKEN;
import static org.apache.hadoop.fs.s3a.S3AUtils.lookupPassword;

/**
 * Class to bridge from the serializable/marshallabled
 * {@link MarshalledCredentialBinding} class to/from AWS classes.
 * This is to keep that class isolated and not dependent on aws-sdk JARs
 * to load.
 */
public final class MarshalledCredentialBinding {

  private MarshalledCredentialBinding() {
  }

  /**
   * Error text on empty credentials: {@value}.
   */
  @VisibleForTesting
  public static final String NO_AWS_CREDENTIALS = "No AWS credentials";

  /**
   * Create a set of marshalled credentials from a set of credentials
   * issued by an STS call.
   * @param credentials AWS-provided session credentials
   * @return a set of marshalled credentials.
   */
  public static MarshalledCredentials fromSTSCredentials(
      final Credentials credentials) {
    MarshalledCredentials marshalled = new MarshalledCredentials(
        credentials.getAccessKeyId(),
        credentials.getSecretAccessKey(),
        credentials.getSessionToken());
    Date date = credentials.getExpiration();
    marshalled.setExpiration(date != null ? date.getTime() : 0);
    return marshalled;
  }

  /**
   * Create from a set of AWS credentials.
   * @param credentials source credential.
   * @return a set of marshalled credentials.
   */
  public static MarshalledCredentials fromAWSCredentials(
      final AWSSessionCredentials credentials) {
    return new MarshalledCredentials(
        credentials.getAWSAccessKeyId(),
        credentials.getAWSSecretKey(),
        credentials.getSessionToken());
  }

  /**
   * Build a set of credentials from the environment.
   * @param env environment.
   * @return a possibly incomplete/invalid set of credentials.
   */
  public static MarshalledCredentials fromEnvironment(
      final Map<String, String> env) {
    return new MarshalledCredentials(
      nullToEmptyString(env.get("AWS_ACCESS_KEY")),
      nullToEmptyString(env.get("AWS_SECRET_KEY")),
      nullToEmptyString(env.get("AWS_SESSION_TOKEN")));
  }

  /**
   * Take a string where a null value is remapped to an empty string.
   * @param src source string.
   * @return the value of the string or ""
   */
  private static String nullToEmptyString(final String src) {
    return src == null ? "" : src;
  }

  /**
   * Loads the credentials from the owning S3A FS, including
   * from Hadoop credential providers.
   * There is no validation.
   * @param uri binding URI
   * @param conf configuration to load from
   * @return the component
   * @throws IOException on any load failure
   */
  public static MarshalledCredentials fromFileSystem(
      final URI uri,
      final Configuration conf) throws IOException {
    // determine the bucket
    final String bucket = uri != null ? uri.getHost() : "";
    final Configuration leanConf =
        ProviderUtils.excludeIncompatibleCredentialProviders(
            conf, S3AFileSystem.class);
    return new MarshalledCredentials(
        lookupPassword(bucket, leanConf, ACCESS_KEY),
        lookupPassword(bucket, leanConf, SECRET_KEY),
        lookupPassword(bucket, leanConf, SESSION_TOKEN));
  }

  /**
   * Create an AWS credential set from a set of marshalled credentials.
   *
   * This code would seem to fit into (@link MarshalledCredentials}, and
   * while it would from a code-hygiene perspective, to keep all AWS
   * SDK references out of that class, the logic is implemented here instead,
   * @param marshalled marshalled credentials
   * @param typeRequired type of credentials required
   * @param component component name for exception messages.
   * @return a new set of credentials
   * @throws NoAuthWithAWSException validation failure
   * @throws NoAwsCredentialsException the credentials are actually empty.
   */
  public static AWSCredentials toAWSCredentials(
      final MarshalledCredentials marshalled,
      final MarshalledCredentials.CredentialTypeRequired typeRequired,
      final String component)
      throws NoAuthWithAWSException, NoAwsCredentialsException {

    if (marshalled.isEmpty()) {
      throw new NoAwsCredentialsException(component, NO_AWS_CREDENTIALS);
    }
    if (!marshalled.isValid(typeRequired)) {
      throw new NoAuthWithAWSException(component + ":" +
          marshalled.buildInvalidCredentialsError(typeRequired));
    }
    final String accessKey = marshalled.getAccessKey();
    final String secretKey = marshalled.getSecretKey();
    if (marshalled.hasSessionToken()) {
      // a session token was supplied, so return session credentials
      return new BasicSessionCredentials(accessKey, secretKey,
          marshalled.getSessionToken());
    } else {
      // these are full credentials
      return new BasicAWSCredentials(accessKey, secretKey);
    }
  }

  /**
   * Request a set of credentials from an STS endpoint.
   * @param parentCredentials the parent credentials needed to talk to STS
   * @param awsConf AWS client configuration
   * @param stsEndpoint an endpoint, use "" for none
   * @param stsRegion region; use if the endpoint isn't the AWS default.
   * @param duration duration of the credentials in seconds. Minimum value: 900.
   * @param invoker invoker to use for retrying the call.
   * @return the credentials
   * @throws IOException on a failure of the request
   */
  @Retries.RetryTranslated
  public static MarshalledCredentials requestSessionCredentials(
      final AWSCredentialsProvider parentCredentials,
      final ClientConfiguration awsConf,
      final String stsEndpoint,
      final String stsRegion,
      final int duration,
      final Invoker invoker) throws IOException {
    final AWSSecurityTokenService tokenService =
        STSClientFactory.builder(parentCredentials,
            awsConf,
            stsEndpoint.isEmpty() ? null : stsEndpoint,
            stsRegion)
            .build();
    return fromSTSCredentials(
        STSClientFactory.createClientConnection(tokenService, invoker)
            .requestSessionCredentials(duration, TimeUnit.SECONDS));
  }

}
