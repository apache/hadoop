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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.adapter.AwsV1BindingSupport;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.fs.store.LogExactlyOnce;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER_MAPPING;
import static org.apache.hadoop.fs.s3a.adapter.AwsV1BindingSupport.isAwsV1SdkAvailable;

/**
 * This class provides methods to create a {@link AWSCredentialProviderList}
 * list of AWS credential providers.
 */
public final class CredentialProviderListFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderListFactory.class);

  /**
   * A v1 entry has been remapped. warn once about this and then shut up.
   */
  private static final LogExactlyOnce LOG_REMAPPED_ENTRY = new LogExactlyOnce(LOG);

  /**
   * Error message when the AWS provider list built up contains a forbidden
   * entry.
   */
  @VisibleForTesting
  public static final String E_FORBIDDEN_AWS_PROVIDER
      = "AWS provider class cannot be used";

  /**
   * The standard AWS provider list for AWS connections.
   */
  public static final List<Class<?>>
      STANDARD_AWS_PROVIDERS = Collections.unmodifiableList(
      Arrays.asList(
          EnvironmentVariableCredentialsProvider.class,
          IAMInstanceCredentialsProvider.class,
          SimpleAWSCredentialsProvider.class,
          TemporaryAWSCredentialsProvider.class));

  /** V1 credential provider: {@value}. */
  public static final String ANONYMOUS_CREDENTIALS_V1 =
      "com.amazonaws.auth.AnonymousAWSCredentials";

  /** V1 credential provider: {@value}. */
  public static final String EC2_CONTAINER_CREDENTIALS_V1 =
      "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper";

  /** V1 credential provider: {@value}. */
  public static final String EC2_IAM_CREDENTIALS_V1 =
      "com.amazonaws.auth.InstanceProfileCredentialsProvider";

  /** V2 EC2 instance/container credential provider. */
  public static final String EC2_IAM_CREDENTIALS_V2 =
      IAMInstanceCredentialsProvider.class.getName();

  /** V1 env var credential provider: {@value}. */
  public static final String ENVIRONMENT_CREDENTIALS_V1 =
      "com.amazonaws.auth.EnvironmentVariableCredentialsProvider";

  /** V2 environment variables credential provider. */
  public static final String ENVIRONMENT_CREDENTIALS_V2 =
      EnvironmentVariableCredentialsProvider.class.getName();

  /** V1 profile credential provider: {@value}. */
  public static final String PROFILE_CREDENTIALS_V1 =
      "com.amazonaws.auth.profile.ProfileCredentialsProvider";

  /** V2 environment variables credential provider. */
  public static final String PROFILE_CREDENTIALS_V2 =
      ProfileCredentialsProvider.class.getName();

  /**
   * Private map of v1 to v2 credential provider name mapping.
   */
  private static final Map<String, String> V1_V2_CREDENTIAL_PROVIDER_MAP =
      initCredentialProvidersMap();

  private CredentialProviderListFactory() {
  }

  /**
   * Create the AWS credentials from the providers, the URI and
   * the key {@link Constants#AWS_CREDENTIALS_PROVIDER} in the configuration.
   * @param binding Binding URI -may be null
   * @param conf filesystem configuration
   * @return a credentials provider list
   * @throws IOException Problems loading the providers (including reading
   * secrets from credential files).
   */
  public static AWSCredentialProviderList createAWSCredentialProviderList(
      @Nullable URI binding,
      Configuration conf) throws IOException {
    // this will reject any user:secret entries in the URI
    S3xLoginHelper.rejectSecretsInURIs(binding);
    AWSCredentialProviderList credentials =
        buildAWSProviderList(binding,
            conf,
            AWS_CREDENTIALS_PROVIDER,
            STANDARD_AWS_PROVIDERS,
            new HashSet<>());
    // make sure the logging message strips out any auth details
    LOG.debug("For URI {}, using credentials {}",
        binding, credentials);
    return credentials;
  }

  /**
   * Load list of AWS credential provider/credential provider factory classes.
   * @param conf configuration
   * @param key key
   * @param defaultValue list of default values
   * @return the list of classes, empty if the default list is empty and
   * there was no match for the key in the configuration.
   * @throws IOException on a failure to load the list.
   */
  private static Collection<String> loadAWSProviderClasses(Configuration conf,
      String key,
      Class<?>... defaultValue) throws IOException {
    final Collection<String> classnames = conf.getTrimmedStringCollection(key);
    if (classnames.isEmpty()) {
      // empty list; return the defaults
      return Arrays.stream(defaultValue).map(c -> c.getName()).collect(Collectors.toList());
    } else {
      return classnames;
    }
  }

  /**
   * Maps V1 credential providers to either their equivalent SDK V2 class or hadoop provider.
   */
  private static Map<String, String> initCredentialProvidersMap() {
    Map<String, String> v1v2CredentialProviderMap = new HashMap<>();

    v1v2CredentialProviderMap.put(ANONYMOUS_CREDENTIALS_V1,
        AnonymousAWSCredentialsProvider.NAME);
    v1v2CredentialProviderMap.put(EC2_CONTAINER_CREDENTIALS_V1,
        EC2_IAM_CREDENTIALS_V2);
    v1v2CredentialProviderMap.put(EC2_IAM_CREDENTIALS_V1,
        EC2_IAM_CREDENTIALS_V2);
    v1v2CredentialProviderMap.put(ENVIRONMENT_CREDENTIALS_V1,
        ENVIRONMENT_CREDENTIALS_V2);
    v1v2CredentialProviderMap.put(PROFILE_CREDENTIALS_V1,
        PROFILE_CREDENTIALS_V2);

    return v1v2CredentialProviderMap;
  }

  /**
   * Load list of AWS credential provider/credential provider factory classes;
   * support a forbidden list to prevent loops, mandate full secrets, etc.
   * @param binding Binding URI -may be null
   * @param conf configuration
   * @param key configuration key to use
   * @param forbidden a possibly empty set of forbidden classes.
   * @param defaultValues list of default providers.
   * @return the list of classes, possibly empty
   * @throws IOException on a failure to load the list.
   */
  public static AWSCredentialProviderList buildAWSProviderList(
      @Nullable final URI binding,
      final Configuration conf,
      final String key,
      final List<Class<?>> defaultValues,
      final Set<Class<?>> forbidden) throws IOException {

    // build up the base provider
    Collection<String> awsClasses = loadAWSProviderClasses(conf,
        key,
        defaultValues.toArray(new Class[defaultValues.size()]));

    Map<String, String> awsCredsMappedClasses =
        S3AUtils.getTrimmedStringCollectionSplitByEquals(conf,
            AWS_CREDENTIALS_PROVIDER_MAPPING);
    Map<String, String> v1v2CredentialProviderMap = V1_V2_CREDENTIAL_PROVIDER_MAP;
    final Set<String> forbiddenClassnames =
        forbidden.stream().map(c -> c.getName()).collect(Collectors.toSet());


    // iterate through, checking for forbidden values and then instantiating
    // each provider
    AWSCredentialProviderList providers = new AWSCredentialProviderList();
    for (String className : awsClasses) {
      if (v1v2CredentialProviderMap.containsKey(className)) {
        // mapping

        final String mapped = v1v2CredentialProviderMap.get(className);
        LOG_REMAPPED_ENTRY.warn("Credentials option {} contains AWS v1 SDK entry {}; mapping to {}",
            key, className, mapped);
        className = mapped;
      } else if (awsCredsMappedClasses != null && awsCredsMappedClasses.containsKey(className)) {
        final String mapped = awsCredsMappedClasses.get(className);
        LOG_REMAPPED_ENTRY.debug("Credential entry {} is mapped to {}", className, mapped);
        className = mapped;
      }
      // now scan the forbidden list. doing this after any mappings ensures the v1 names
      // are also blocked
      if (forbiddenClassnames.contains(className)) {
        throw new InstantiationIOException(InstantiationIOException.Kind.Forbidden,
            binding, className, key, E_FORBIDDEN_AWS_PROVIDER, null);
      }

      AwsCredentialsProvider provider;
      try {
        provider = createAWSV2CredentialProvider(conf, className, binding, key);
      } catch (InstantiationIOException e) {
        // failed to create a v2; try to see if it is a v1
        if (e.getKind() == InstantiationIOException.Kind.IsNotImplementation) {
          if (isAwsV1SdkAvailable()) {
            // try to create v1
            LOG.debug("Failed to create {} as v2 credentials, trying to instantiate as v1",
                className);
            try {
              provider =
                  AwsV1BindingSupport.createAWSV1CredentialProvider(conf, className, binding, key);
              LOG_REMAPPED_ENTRY.warn("Credentials option {} contains AWS v1 SDK entry {}",
                  key, className);
            } catch (InstantiationIOException ex) {
              // if it is something other than non-implementation, throw.
              // that way, non-impl messages are about v2 not v1 in the error
              if (ex.getKind() != InstantiationIOException.Kind.IsNotImplementation) {
                throw ex;
              } else {
                throw e;
              }
            }
          } else {
            LOG.warn("Failed to instantiate {} as AWS v2 SDK credential provider;"
                + " AWS V1 SDK is not on the classpth so unable to attempt to"
                + " instantiate as a v1 provider", className, e);
            throw e;
          }
        } else {
          // any other problem
          throw e;

        }
        LOG.debug("From provider class {} created Aws provider {}", className, provider);
      }
      providers.add(provider);
    }
    return providers;
  }

  /**
   * Create an AWS v2 credential provider from its class by using reflection.
   * @param conf configuration
   * @param className credential class name
   * @param uri URI of the FS
   * @param key configuration key to use
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   * @see S3AUtils#getInstanceFromReflection
   */
  private static AwsCredentialsProvider createAWSV2CredentialProvider(Configuration conf,
      String className,
      @Nullable URI uri, final String key) throws IOException {
    LOG.debug("Credential provider class is {}", className);
    return S3AUtils.getInstanceFromReflection(className, conf, uri, AwsCredentialsProvider.class,
        "create", key);
  }

}
