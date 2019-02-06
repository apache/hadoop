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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.apache.hadoop.io.IOUtils;

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
 *   <li>On failure, the last "relevant" AmazonClientException raised is
 *   rethrown; exceptions other than 'no credentials' have priority.</li>
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

  static final String
      CREDENTIALS_REQUESTED_WHEN_CLOSED
      = "Credentials requested after provider list was closed";

  private final List<AWSCredentialsProvider> providers = new ArrayList<>(1);
  private boolean reuseLastProvider = true;
  private AWSCredentialsProvider lastProvider;

  private final AtomicInteger refCount = new AtomicInteger(1);

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * The name, which is empty by default.
   * Uses in the code assume if non empty there's a trailing space.
   */
  private String name = "";

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
   * Create with an initial list of providers.
   * @param name name for error messages, may be ""
   * @param providerArgs provider list.
   */
  public AWSCredentialProviderList(final String name,
      final AWSCredentialsProvider... providerArgs) {
    setName(name);
    Collections.addAll(providers, providerArgs);
  }

  /**
   * Set the name; adds a ": " if needed.
   * @param name name to add, or "" for no name.
   */
  public void setName(final String name) {
    if (!name.isEmpty() && !name.endsWith(": ")) {
      this.name = name + ": ";
    } else {
      this.name = name;
    }
  }

  /**
   * Add a new provider.
   * @param p provider
   */
  public void add(AWSCredentialsProvider p) {
    providers.add(p);
  }

  /**
   * Add all providers from another list to this one.
   * @param other the other list.
   */
  public void addAll(AWSCredentialProviderList other) {
    providers.addAll(other.providers);
  }

  /**
   * Refresh all child entries.
   */
  @Override
  public void refresh() {
    if (isClosed()) {
      return;
    }
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
    if (isClosed()) {
      LOG.warn(CREDENTIALS_REQUESTED_WHEN_CLOSED);
      throw new NoAuthWithAWSException(name +
          CREDENTIALS_REQUESTED_WHEN_CLOSED);
    }
    checkNotEmpty();
    if (reuseLastProvider && lastProvider != null) {
      return lastProvider.getCredentials();
    }

    AmazonClientException lastException = null;
    for (AWSCredentialsProvider provider : providers) {
      try {
        AWSCredentials credentials = provider.getCredentials();
        Preconditions.checkNotNull(credentials,
            "Null credentials returned by %s", provider);
        if ((credentials.getAWSAccessKeyId() != null &&
            credentials.getAWSSecretKey() != null)
            || (credentials instanceof AnonymousAWSCredentials)) {
          lastProvider = provider;
          LOG.debug("Using credentials from {}", provider);
          return credentials;
        }
      } catch (NoAwsCredentialsException e) {
        // don't bother with the stack trace here as it is usually a
        // minor detail.

        // only update the last exception if it isn't set.
        // Why so? Stops delegation token issues being lost on the fallback
        // values.
        if (lastException == null) {
          lastException = e;
        }
        LOG.debug("No credentials from {}: {}",
            provider, e.toString());
      } catch (AmazonClientException e) {
        lastException = e;
        LOG.debug("No credentials provided by {}: {}",
            provider, e.toString(), e);
      }
    }

    // no providers had any credentials. Rethrow the last exception
    // or create a new one.
    String message =  name +  "No AWS Credentials provided by "
        + listProviderNames();
    if (lastException != null) {
      message += ": " + lastException;
    }
    if (lastException instanceof CredentialInitializationException) {
      throw lastException;
    } else {
      throw new NoAuthWithAWSException(message, lastException);
    }
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
      throw new NoAuthWithAWSException(name + NO_AWS_CREDENTIAL_PROVIDERS);
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
    return "AWSCredentialProviderList[" +
        name +
        "refcount= " + refCount.get() + ": [" +
        StringUtils.join(providers, ", ") + ']'
        + (lastProvider != null ? (" last provider: " + lastProvider) : "");
  }

  /**
   * Get a reference to this object with an updated reference count.
   *
   * @return a reference to this
   */
  public synchronized AWSCredentialProviderList share() {
    Preconditions.checkState(!closed.get(), "Provider list is closed");
    refCount.incrementAndGet();
    return this;
  }

  /**
   * Get the current reference count.
   * @return the current ref count
   */
  @VisibleForTesting
  public int getRefCount() {
    return refCount.get();
  }

  /**
   * Get the closed flag.
   * @return true iff the list is closed.
   */
  @VisibleForTesting
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Close routine will close all providers in the list which implement
   * {@code Closeable}.
   * This matters because some providers start a background thread to
   * refresh their secrets.
   */
  @Override
  public void close() {
    synchronized (this) {
      if (closed.get()) {
        // already closed: no-op
        return;
      }
      int remainder = refCount.decrementAndGet();
      if (remainder != 0) {
        // still actively used, or somehow things are
        // now negative
        LOG.debug("Not closing {}", this);
        return;
      }
      // at this point, the closing is going to happen
      LOG.debug("Closing {}", this);
      closed.set(true);
    }

    // do this outside the synchronized block.
    for (AWSCredentialsProvider p : providers) {
      if (p instanceof Closeable) {
        IOUtils.closeStream((Closeable) p);
      } else if (p instanceof AutoCloseable) {
        S3AUtils.closeAutocloseables(LOG, (AutoCloseable)p);
      }
    }
  }

  /**
   * Get the size of this list.
   * @return the number of providers in the list.
   */
  public int size() {
    return providers.size();
  }
}
