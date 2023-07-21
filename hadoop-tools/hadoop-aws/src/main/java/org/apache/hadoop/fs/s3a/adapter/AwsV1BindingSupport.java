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

package org.apache.hadoop.fs.s3a.adapter;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * Binding support; the sole way which the rest of the code should instantiate v1 SDK libraries.
 * Uses this class's Classloader for its analysis/loading.
 */
@SuppressWarnings("StaticNonFinalField")
public class AwsV1BindingSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      AwsV1BindingSupport.class);

  public static final String CREDENTIAL_PROVIDER_CLASSNAME =
      "com.amazonaws.auth.AWSCredentialsProvider";

  public static final String NOT_AWS_PROVIDER =
      "does not implement AWSCredentialsProvider";

  public static final String NOT_AWS_V2_PROVIDER =
      "does not implement AwsCredentialsProvider";

  public static final String ABSTRACT_PROVIDER =
      "is abstract and therefore cannot be created";

  /**
   * Tack availability.
   */
  private static Tristate sdkAvailability = Tristate.UNKNOWN;

  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private static Class<?> credentialProviderClass;

  static {
    isAwsV1SdkAvailable();
  }

  /**
   * Is the AWS v1 SDK available
   * @param cl classloader to look in.
   * @return true if it was found in the classloader
   */
  public static synchronized boolean isAwsV1SdkAvailable() {

    final Optional<Boolean> mapping = sdkAvailability.getMapping();
    if (mapping.isPresent()) {
      return mapping.get();
    }
    // no binding, so calculate it once.
    try {
      ClassLoader cl = AwsV1BindingSupport.class.getClassLoader();
      credentialProviderClass = cl.loadClass(CREDENTIAL_PROVIDER_CLASSNAME);
      LOG.debug("v1 SDK class {} found", CREDENTIAL_PROVIDER_CLASSNAME);
      sdkAvailability = Tristate.TRUE;
    } catch (Exception e) {
      LOG.debug("v1 SDK class {} not found", CREDENTIAL_PROVIDER_CLASSNAME, e);
      sdkAvailability = Tristate.FALSE;
    }
    // guaranteed to be non-empty
    return sdkAvailability.getMapping().get();
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
   * @param conf configuration
   * @param className credential classname
   * @param uri URI of the FS
   * @return the instantiated class
   * @throws IOException on any instantiation failure, including v1 SDK not found.
   */
  public  static AwsCredentialsProvider createAWSV1CredentialProvider(
      Configuration conf,
      String className,
      @Nullable URI uri) throws IOException {
    if (!isAwsV1SdkAvailable()) {
      throw new IOException("No AWS v1 SDK available; unable to load " + className);
    }
    return V1ToV2AwsCredentialProviderAdapter.create(conf, className, uri);

  }
}
