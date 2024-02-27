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

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

/**
 * Custom signer that delegates to the AWS V4 signer.
 * Logs at TRACE the string value of any request.
 * This is in the production code to support testing the signer plugin mechansim.
 * To use
 * <pre>
 *   fs.s3a.http.signer.enabled = true
 *   fs.s3a.http.signer.class = org.apache.hadoop.fs.s3a.auth.CustomHttpSigner
 * </pre>
 */
public class CustomHttpSigner implements HttpSigner<AwsCredentialsIdentity> {
  private static final Logger LOG = LoggerFactory
      .getLogger(CustomHttpSigner.class);

  /**
   * The delegate signer.
   */
  private final HttpSigner<AwsCredentialsIdentity> delegateSigner;

  public CustomHttpSigner() {
    delegateSigner = AwsV4HttpSigner.create();
  }

  @Override
  public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity>
      request) {
    LOG.trace("Signing request:{}", request.request());
    return delegateSigner.sign(request);
  }

  @Override
  public CompletableFuture<AsyncSignedRequest> signAsync(
      final AsyncSignRequest<? extends AwsCredentialsIdentity> request) {

    LOG.trace("Signing async request:{}", request.request());
    return delegateSigner.signAsync(request);
  }
}
