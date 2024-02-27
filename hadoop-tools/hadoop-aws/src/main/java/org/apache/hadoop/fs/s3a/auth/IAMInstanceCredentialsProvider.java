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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.HttpCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.maybeExtractIOException;

/**
 * This is an IAM credential provider which wraps
 * an {@code ContainerCredentialsProvider}
 * to provide credentials when the S3A connector is instantiated on AWS EC2
 * or the AWS container services.
 * <p>
 * The provider is initialized with async credential refresh enabled to be less
 * brittle against transient network issues.
 * <p>
 * If the ContainerCredentialsProvider fails to authenticate, then an instance of
 * {@link InstanceProfileCredentialsProvider} is created and attemped to
 * be used instead, again with async credential refresh enabled.
 * <p>
 * If both credential providers fail, a {@link NoAwsCredentialsException}
 * is thrown, which can be recognized by retry handlers
 * as a non-recoverable failure.
 * <p>
 * It is implicitly public; marked evolving as we can change its semantics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IAMInstanceCredentialsProvider
    implements AwsCredentialsProvider, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(IAMInstanceCredentialsProvider.class);

  /**
   * The credentials provider.
   * Initially a container credentials provider, but if that fails
   * fall back to the instance profile provider.
   */
  private HttpCredentialsProvider iamCredentialsProvider;

  /**
   * Is the container credentials provider in use?
   */
  private boolean isContainerCredentialsProvider;

  /**
   * Constructor.
   * Build credentials provider with async refresh,
   * mark {@link #isContainerCredentialsProvider} as true.
   */
  public IAMInstanceCredentialsProvider() {
    isContainerCredentialsProvider = true;
    iamCredentialsProvider = ContainerCredentialsProvider.builder()
        .asyncCredentialUpdateEnabled(true)
        .build();
  }

  /**
   * Ask for the credentials.
   * Failure invariably means "you aren't running in an EC2 VM or AWS container".
   * @return the credentials
   * @throws NoAwsCredentialsException on auth failure to indicate non-recoverable.
   */
  @Override
  public AwsCredentials resolveCredentials() {
    try {
      return getCredentials();
    } catch (SdkClientException e) {

      // if the exception contains an IOE, extract it
      // so its type is the immediate cause of this new exception.
      Throwable t = e;
      final IOException ioe = maybeExtractIOException("IAM endpoint", e,
          "resolveCredentials()");
      if (ioe != null) {
        t = ioe;
      }
      throw new NoAwsCredentialsException("IAMInstanceCredentialsProvider",
          e.getMessage(), t);
    }
  }

  /**
   * First try {@link ContainerCredentialsProvider}, which will throw an exception if credentials
   * cannot be retrieved from the container. Then resolve credentials
   * using {@link InstanceProfileCredentialsProvider}.
   *
   * @return credentials
   */
  private synchronized AwsCredentials getCredentials() {
    try {
      return iamCredentialsProvider.resolveCredentials();
    } catch (SdkClientException e) {
      LOG.debug("Failed to get credentials from container provider,", e);
      if (isContainerCredentialsProvider) {
        // create instance profile provider
        LOG.debug("Switching to instance provider", e);

        // close it to shut down any thread
        iamCredentialsProvider.close();
        isContainerCredentialsProvider = false;
        iamCredentialsProvider = InstanceProfileCredentialsProvider.builder()
                .asyncCredentialUpdateEnabled(true)
                .build();
        return iamCredentialsProvider.resolveCredentials();
      } else {
        // already using instance profile provider, so fail
        throw e;
      }

    }
  }

  /**
   * Is this a container credentials provider?
   * @return true if the container credentials provider is in use;
   *         false for InstanceProfileCredentialsProvider
   */
  public boolean isContainerCredentialsProvider() {
    return isContainerCredentialsProvider;
  }

  @Override
  public synchronized void close() throws IOException {
    // this be true but just for safety...
    if (iamCredentialsProvider != null) {
      iamCredentialsProvider.close();
    }
  }

  @Override
  public String toString() {
    return "IAMInstanceCredentialsProvider{" +
        "credentialsProvider=" + iamCredentialsProvider +
        ", isContainerCredentialsProvider=" + isContainerCredentialsProvider +
        '}';
  }
}
