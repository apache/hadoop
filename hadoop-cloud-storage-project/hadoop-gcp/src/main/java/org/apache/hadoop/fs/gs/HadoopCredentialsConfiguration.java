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

package org.apache.hadoop.fs.gs;

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * The Hadoop credentials configuration.
 *
 * <p>When reading configuration this class makes use of a list of key prefixes that are each
 * applied to key suffixes to create a complete configuration key. There is a base prefix of
 * 'google.cloud.' that is included by the builder for each configuration key suffix. When
 * constructing, other prefixes can be specified. Prefixes specified later can be used to override
 * the values of previously set values. In this way a set of global credentials can be specified for
 * most connectors with an override specified for any connectors that need different credentials.
 */
final class HadoopCredentialsConfiguration {

  /**
   * All instances constructed using the builder will use {@code google.cloud} as the first prefix
   * checked. Other prefixes can be added and will override values in the {@code google.cloud}
   * prefix.
   */
  private static final String BASE_KEY_PREFIX = "google.cloud";
  private static   final String CLOUD_PLATFORM_SCOPE =
          "https://www.googleapis.com/auth/cloud-platform";
  /** Key suffix used to configure authentication type. */
  private static final HadoopConfigurationProperty<AuthenticationType> AUTHENTICATION_TYPE_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.type", AuthenticationType.COMPUTE_ENGINE);
  /**
   * Key suffix used to configure the path to a JSON file containing a Service Account key and
   * identifier (email). Technically, this could be a JSON containing a non-service account user,
   * but this setting is only used in the service account flow and is namespaced as such.
   */
  private static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.service.account.json.keyfile");
  /**
   * Key suffix used to configure the path to a JSON file containing a workload identity federation,
   * i.e. external account credential configuration. Technically, this could be a JSON containing an
   * service account impersonation url and credential source. but this setting is only used in the
   * workload identity federation flow and is namespaced as such.
   */
  private static final HadoopConfigurationProperty<String>
          WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX =
          new HadoopConfigurationProperty<>(
                  ".auth.workload.identity.federation.credential.config.file");

  /** Key suffix for setting a token server URL to use to refresh OAuth token. */
  private static final HadoopConfigurationProperty<String> TOKEN_SERVER_URL_SUFFIX =
          new HadoopConfigurationProperty<>(".token.server.url");

  private static final HadoopConfigurationProperty<Long> READ_TIMEOUT_SUFFIX =
          new HadoopConfigurationProperty<>(".http.read-timeout", 5_000L);
  /**
   * Configuration key for defining the OAUth2 client ID. Required when the authentication type is
   * USER_CREDENTIALS
   */
  private static final HadoopConfigurationProperty<String> AUTH_CLIENT_ID_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.client.id");
  /**
   * Configuration key for defining the OAUth2 client secret. Required when the authentication type
   * is USER_CREDENTIALS
   */
  private static final HadoopConfigurationProperty<RedactedString> AUTH_CLIENT_SECRET_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.client.secret");
  /**
   * Configuration key for defining the OAuth2 refresh token. Required when the authentication type
   * is USER_CREDENTIALS
   */
  private static final HadoopConfigurationProperty<RedactedString> AUTH_REFRESH_TOKEN_SUFFIX =
          new HadoopConfigurationProperty<>(".auth.refresh.token");

  private HadoopCredentialsConfiguration() {}

  /**
   * Returns full list of config prefixes that will be resolved based on the order in returned list.
   */
  static List<String> getConfigKeyPrefixes(String... keyPrefixes) {
    return ImmutableList.<String>builder().add(keyPrefixes).add(BASE_KEY_PREFIX).build();
  }

  /**
   * Get the credentials for the configured {@link AuthenticationType}.
   *
   * @throws IllegalStateException if configured {@link AuthenticationType} is not recognized.
   */
  static GoogleCredentials getCredentials(Configuration config, String... keyPrefixesVararg)
          throws IOException {
    List<String> keyPrefixes = getConfigKeyPrefixes(keyPrefixesVararg);
    return getCredentials(config, keyPrefixes);
  }

  @VisibleForTesting
  static GoogleCredentials getCredentials(Configuration config, List<String> keyPrefixes)
          throws IOException {
    GoogleCredentials credentials = getCredentialsInternal(config, keyPrefixes);
    return credentials == null ? null : configureCredentials(config, keyPrefixes, credentials);
  }

  private static GoogleCredentials getCredentialsInternal(
          Configuration config, List<String> keyPrefixes) throws IOException {
    AuthenticationType authenticationType =
            AUTHENTICATION_TYPE_SUFFIX.withPrefixes(keyPrefixes).get(config, config::getEnum);
    switch (authenticationType) {
    case APPLICATION_DEFAULT:
      return GoogleCredentials.getApplicationDefault();
    case COMPUTE_ENGINE:
      return ComputeEngineCredentials.newBuilder().build();
    case SERVICE_ACCOUNT_JSON_KEYFILE:
      String keyFile = SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX
              .withPrefixes(keyPrefixes).get(config, config::get);

      if (Strings.isNullOrEmpty(keyFile)) {
        throw new IllegalArgumentException(String.format(
                "Missing keyfile property ('%s') for authentication type '%s'",
                SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(),
                authenticationType));
      }

      try (FileInputStream fis = new FileInputStream(keyFile)) {
        return ServiceAccountCredentials.fromStream(fis);
      }
    case USER_CREDENTIALS:
      String clientId = AUTH_CLIENT_ID_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get);
      RedactedString clientSecret =
              AUTH_CLIENT_SECRET_SUFFIX.withPrefixes(keyPrefixes).getPassword(config);
      RedactedString refreshToken =
              AUTH_REFRESH_TOKEN_SUFFIX.withPrefixes(keyPrefixes).getPassword(config);

      return UserCredentials.newBuilder()
              .setClientId(clientId)
              .setClientSecret(clientSecret.getValue())
              .setRefreshToken(refreshToken.getValue())
              .build();

    case WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE:
      String configFile =
              WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX
                      .withPrefixes(keyPrefixes)
                      .get(config, config::get);
      try (FileInputStream fis = new FileInputStream(configFile)) {
        return ExternalAccountCredentials.fromStream(fis);
      }
    case UNAUTHENTICATED:
      return null;
    default:
      throw new IllegalArgumentException("Unknown authentication type: " + authenticationType);
    }
  }

  private static GoogleCredentials configureCredentials(
          Configuration config, List<String> keyPrefixes, GoogleCredentials credentials) {
    credentials = credentials.createScoped(CLOUD_PLATFORM_SCOPE);
    String tokenServerUrl =
            TOKEN_SERVER_URL_SUFFIX.withPrefixes(keyPrefixes).get(config, config::get);
    if (tokenServerUrl == null) {
      return credentials;
    }
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
              .toBuilder().setTokenServerUri(URI.create(tokenServerUrl)).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials)
              .toBuilder().setTokenServerUri(URI.create(tokenServerUrl)).build();
    }
    return credentials;
  }

  /** Enumerates all supported authentication types. */
  public enum AuthenticationType {
    /** Configures Application Default Credentials authentication. */
    APPLICATION_DEFAULT,
    /** Configures Google Compute Engine service account authentication. */
    COMPUTE_ENGINE,
    /** Configures JSON keyfile service account authentication. */
    SERVICE_ACCOUNT_JSON_KEYFILE,
    /** Configures workload identity pool key file. */
    WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE,
    /** Configures unauthenticated access. */
    UNAUTHENTICATED,
    /** Configures user credentials authentication. */
    USER_CREDENTIALS,
  }
}
