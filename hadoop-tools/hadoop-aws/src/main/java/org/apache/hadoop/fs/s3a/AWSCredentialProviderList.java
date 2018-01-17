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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A list of providers.
 *
 * This is similar to the AWS SDK {@code AWSCredentialsProviderChain},
 * except that:
 * <ol>
 *   <li>Allows extra providers to be added dynamically.</li>
 *   <li>If any provider in the chain throws an exception other than
 *   an {@link AmazonClientException}, that is rethrown, rather than
 *   swallowed.</li>
 *   <li>Has some more diagnostics.</li>
 *   <li>On failure, the last AmazonClientException raised is rethrown.</li>
 *   <li>Special handling of {@link AnonymousAWSCredentials}.</li>
 * </ol>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AWSCredentialProviderList implements AWSCredentialsProvider,
    AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(
      AWSCredentialProviderList.class);
  public static final String NO_AWS_CREDENTIAL_PROVIDERS
      = "No AWS Credential Providers";

  private final List<AWSCredentialsProvider> providers = new ArrayList<>(1);
  private boolean reuseLastProvider = true;
  private AWSCredentialsProvider lastProvider;

  /**
   * Empty instance. This is not ready to be used.
   */
  public AWSCredentialProviderList() {
  }

  /**
   * Create with an initial list of providers.
   * @param providers provider list.
   */
  public AWSCredentialProviderList(
      Collection<AWSCredentialsProvider> providers) {
    this.providers.addAll(providers);
  }

  /**
   * Add a new provider.
   * @param p provider
   */
  public void add(AWSCredentialsProvider p) {
    providers.add(p);
  }

  /**
   * Refresh all child entries.
   */
  @Override
  public void refresh() {
    for (AWSCredentialsProvider provider : providers) {
      provider.refresh();
    }
  }

  /**
   * Iterate through the list of providers, to find one with credentials.
   * If {@link #reuseLastProvider} is true, then it is re-used.
   * @return a set of credentials (possibly anonymous), for authenticating.
   */
  @Override
  public AWSCredentials getCredentials() {
    checkNotEmpty();
    if (reuseLastProvider && lastProvider != null) {
      return lastProvider.getCredentials();
    }

    AmazonClientException lastException = null;
    for (AWSCredentialsProvider provider : providers) {
      try {
        AWSCredentials credentials = provider.getCredentials();
        if ((credentials.getAWSAccessKeyId() != null &&
            credentials.getAWSSecretKey() != null)
            || (credentials instanceof AnonymousAWSCredentials)) {
          lastProvider = provider;
          LOG.debug("Using credentials from {}", provider);
          return credentials;
        }
      } catch (AmazonClientException e) {
        lastException = e;
        LOG.debug("No credentials provided by {}: {}",
            provider, e.toString(), e);
      }
    }

    // no providers had any credentials. Rethrow the last exception
    // or create a new one.
    String message = "No AWS Credentials provided by "
        + listProviderNames();
    if (lastException != null) {
      message += ": " + lastException;
    }
    throw new AmazonClientException(message, lastException);

  }

  /**
   * Returns the underlying list of providers.
   *
   * @return providers
   */
  @VisibleForTesting
  List<AWSCredentialsProvider> getProviders() {
    return providers;
  }

  /**
   * Verify that the provider list is not empty.
   * @throws AmazonClientException if there are no providers.
   */
  public void checkNotEmpty() {
    if (providers.isEmpty()) {
      throw new AmazonClientException(NO_AWS_CREDENTIAL_PROVIDERS);
    }
  }

  /**
   * List all the providers' names.
   * @return a list of names, separated by spaces (with a trailing one).
   * If there are no providers, "" is returned.
   */
  public String listProviderNames() {
    return providers.stream()
        .map(provider -> provider.getClass().getSimpleName() + ' ')
        .collect(Collectors.joining());
  }

  /**
   * The string value is this class name and the string values of nested
   * providers.
   * @return a string value for debugging.
   */
  @Override
  public String toString() {
    return "AWSCredentialProviderList: " +
        StringUtils.join(providers, " ");
  }

  /**
   * Close routine will close all providers in the list which implement
   * {@code Closeable}.
   * This matters because some providers start a background thread to
   * refresh their secrets.
   */
  @Override
  public void close() {
    for(AWSCredentialsProvider p: providers) {
      if (p instanceof Closeable) {
        IOUtils.closeStream((Closeable)p);
      }
    }
  }
}
