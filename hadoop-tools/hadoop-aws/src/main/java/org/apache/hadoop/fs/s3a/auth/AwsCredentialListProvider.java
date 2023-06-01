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
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;

import static org.apache.hadoop.fs.s3a.Constants.AWS_AUTH_CLASS_PREFIX;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;

/**
 * This class provides methods to create the list of AWS credential providers.
 */
public final class AwsCredentialListProvider {

  private AwsCredentialListProvider() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(AwsCredentialListProvider.class);

  public static final String NOT_AWS_PROVIDER =
      "does not implement AWSCredentialsProvider";
  public static final String NOT_AWS_V2_PROVIDER =
      "does not implement AwsCredentialsProvider";
  public static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";

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
          TemporaryAWSCredentialsProvider.class,
          SimpleAWSCredentialsProvider.class,
          EnvironmentVariableCredentialsProvider.class,
          IAMInstanceCredentialsProvider.class));

  /**
   * Create the AWS credentials from the providers, the URI and
   * the key {@link Constants#AWS_CREDENTIALS_PROVIDER} in the configuration.
   * @param binding Binding URI -may be null
   * @param conf filesystem configuration
   * @return a credentials provider list
   * @throws IOException Problems loading the providers (including reading
   * secrets from credential files).
   */
  public static AWSCredentialProviderList createAWSCredentialProviderSet(
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
   * @return the list of classes, possibly empty
   * @throws IOException on a failure to load the list.
   */
  private static List<Class<?>> loadAWSProviderClasses(Configuration conf,
      String key,
      Class<?>... defaultValue) throws IOException {
    try {
      return Arrays.asList(conf.getClasses(key, defaultValue));
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException("From option " + key + ' ' + c, c);
    }
  }

  /**
   * Maps V1 credential providers to either their equivalent SDK V2 class or hadoop provider.
   */
  private static Map<String, Class> initCredentialProvidersMap() {
    Map<String, Class> v1v2CredentialProviderMap = new HashMap<>();

    v1v2CredentialProviderMap.put("EnvironmentVariableCredentialsProvider",
        EnvironmentVariableCredentialsProvider.class);
    v1v2CredentialProviderMap.put("EC2ContainerCredentialsProviderWrapper",
        IAMInstanceCredentialsProvider.class);
    v1v2CredentialProviderMap.put("InstanceProfileCredentialsProvider",
        IAMInstanceCredentialsProvider.class);

    return v1v2CredentialProviderMap;
  }

  /**
   * Load list of AWS credential provider/credential provider factory classes;
   * support a forbidden list to prevent loops, mandate full secrets, etc.
   * @param binding Binding URI -may be null
   * @param conf configuration
   * @param key key
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
    List<Class<?>> awsClasses = loadAWSProviderClasses(conf,
        key,
        defaultValues.toArray(new Class[defaultValues.size()]));

    Map<String, Class> v1v2CredentialProviderMap = initCredentialProvidersMap();
    // and if the list is empty, switch back to the defaults.
    // this is to address the issue that configuration.getClasses()
    // doesn't return the default if the config value is just whitespace.
    if (awsClasses.isEmpty()) {
      awsClasses = defaultValues;
    }
    // iterate through, checking for blacklists and then instantiating
    // each provider
    AWSCredentialProviderList providers = new AWSCredentialProviderList();
    for (Class<?> aClass : awsClasses) {

      if (forbidden.contains(aClass)) {
        throw new IOException(E_FORBIDDEN_AWS_PROVIDER
            + " in option " + key + ": " + aClass);
      }

      if (v1v2CredentialProviderMap.containsKey(aClass.getSimpleName()) &&
          aClass.getName().contains(AWS_AUTH_CLASS_PREFIX)){
        providers.add(createAWSV2CredentialProvider(conf,
            v1v2CredentialProviderMap.get(aClass.getSimpleName()), binding));
      } else if (AWSCredentialsProvider.class.isAssignableFrom(aClass)) {
        providers.add(createAWSV1CredentialProvider(conf,
            aClass, binding));
      } else {
        providers.add(createAWSV2CredentialProvider(conf, aClass, binding));
      }

    }
    return providers;
  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting java.net.URI and
   *     org.apache.hadoop.conf.Configuration</li>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    com.amazonaws.auth.AWSCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param conf configuration
   * @param credClass credential class
   * @param uri URI of the FS
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  private static AWSCredentialsProvider createAWSV1CredentialProvider(Configuration conf,
      Class<?> credClass, @Nullable URI uri) throws IOException {
    AWSCredentialsProvider credentials = null;
    String className = credClass.getName();
    if (!AWSCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IOException("Class " + credClass + " " + NOT_AWS_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IOException("Class " + credClass + " " + ABSTRACT_PROVIDER);
    }
    LOG.debug("Credential provider class is {}", className);

    credentials =
        S3AUtils.getInstanceFromReflection(credClass, conf, uri, AWSCredentialsProvider.class,
            "getInstance", AWS_CREDENTIALS_PROVIDER);
    return credentials;

  }

  /**
   * Create an AWS credential provider from its class by using reflection.  The
   * class must implement one of the following means of construction, which are
   * attempted in order:
   *
   * <ol>
   * <li>a public constructor accepting java.net.URI and
   *     org.apache.hadoop.conf.Configuration</li>
   * <li>a public constructor accepting
   *    org.apache.hadoop.conf.Configuration</li>
   * <li>a public static method named getInstance that accepts no
   *    arguments and returns an instance of
   *    software.amazon.awssdk.auth.credentials.AwsCredentialsProvider, or</li>
   * <li>a public default constructor.</li>
   * </ol>
   *
   * @param conf configuration
   * @param credClass credential class
   * @param uri URI of the FS
   * @return the instantiated class
   * @throws IOException on any instantiation failure.
   */
  private static AwsCredentialsProvider createAWSV2CredentialProvider(Configuration conf,
      Class<?> credClass, @Nullable URI uri) throws IOException {
    AwsCredentialsProvider credentials = null;
    String className = credClass.getName();
    if (!AwsCredentialsProvider.class.isAssignableFrom(credClass)) {
      throw new IOException("Class " + credClass + " " + NOT_AWS_V2_PROVIDER);
    }
    if (Modifier.isAbstract(credClass.getModifiers())) {
      throw new IOException("Class " + credClass + " " + ABSTRACT_PROVIDER);
    }
    LOG.debug("Credential provider class is {}", className);
    credentials =
        S3AUtils.getInstanceFromReflection(credClass, conf, uri, AwsCredentialsProvider.class,
            "create", AWS_CREDENTIALS_PROVIDER);
    return credentials;
  }

}
