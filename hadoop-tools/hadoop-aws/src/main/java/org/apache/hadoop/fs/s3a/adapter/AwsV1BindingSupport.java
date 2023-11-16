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
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;

import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unavailable;

/**
 * Binding support; the sole way which the rest of the code should instantiate v1 SDK libraries.
 * Uses this class's Classloader for its analysis/loading.
 */
@SuppressWarnings("StaticNonFinalField")
public final class AwsV1BindingSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      AwsV1BindingSupport.class);

  /**
   * V1 credential provider classname: {@code}.
   */
  public static final String CREDENTIAL_PROVIDER_CLASSNAME =
      "com.amazonaws.auth.AWSCredentialsProvider";

  /**
   * SDK availability.
   */
  private static final boolean SDK_V1_FOUND = checkForAwsV1Sdk();

  private AwsV1BindingSupport() {
  }

  /**
   * Probe for the AWS v1 SDK being available by looking for
   * the class {@link #CREDENTIAL_PROVIDER_CLASSNAME}.
   * @return true if it was found in the classloader
   */
  private static boolean checkForAwsV1Sdk() {

    try {
      ClassLoader cl = AwsV1BindingSupport.class.getClassLoader();
      cl.loadClass(CREDENTIAL_PROVIDER_CLASSNAME);
      LOG.debug("v1 SDK class {} found", CREDENTIAL_PROVIDER_CLASSNAME);
      return true;
    } catch (Exception e) {
      LOG.debug("v1 SDK class {} not found", CREDENTIAL_PROVIDER_CLASSNAME, e);
      return false;
    }
  }

  /**
   * Is the AWS v1 SDK available?
   * @return true if it was found in the classloader
   */
  public static synchronized boolean isAwsV1SdkAvailable() {
    return SDK_V1_FOUND;
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
   * @param key configuration key to use
   * @return the instantiated class
   * @throws InstantiationIOException on any instantiation failure, including v1 SDK not found
   * @throws IOException anything else.
   */
  public static AwsCredentialsProvider createAWSV1CredentialProvider(
      Configuration conf,
      String className,
      @Nullable URI uri,
      final String key) throws IOException {
    if (!isAwsV1SdkAvailable()) {
      throw unavailable(uri, className, key, "No AWS v1 SDK available");
    }
    return V1ToV2AwsCredentialProviderAdapter.create(conf, className, uri);
  }
}
