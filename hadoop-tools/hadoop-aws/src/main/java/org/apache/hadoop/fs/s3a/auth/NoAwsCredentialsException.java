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

import javax.annotation.Nonnull;

/**
 * A special exception which declares that no credentials were found;
 * this can be treated specially in logging, handling, etc.
 * As it subclasses {@link NoAuthWithAWSException}, the S3A retry handler
 * knows not to attempt to ask for the credentials again.
 */
public class NoAwsCredentialsException extends
    NoAuthWithAWSException {

  /**
   * The default error message: {@value}.
   */
  public static final String E_NO_AWS_CREDENTIALS = "No AWS Credentials";

  /**
   * Construct.
   * @param credentialProvider name of the credential provider.
   * @param message message.
   */
  public NoAwsCredentialsException(
      @Nonnull final String credentialProvider,
      @Nonnull final String message) {
    this(credentialProvider, message, null);
  }

  /**
   * Construct with the default message of {@link #E_NO_AWS_CREDENTIALS}.
   * @param credentialProvider name of the credential provider.
   */
  public NoAwsCredentialsException(
      @Nonnull final String credentialProvider) {
    this(credentialProvider, E_NO_AWS_CREDENTIALS, null);
  }

  /**
   * Construct with exception.
   * @param credentialProvider name of the credential provider.
   * @param message message.
   * @param thrown inner exception
   */
  public NoAwsCredentialsException(
      @Nonnull final String credentialProvider,
      @Nonnull final String message,
      final Throwable thrown) {
    super(credentialProvider + ": " + message, thrown);
  }
}
