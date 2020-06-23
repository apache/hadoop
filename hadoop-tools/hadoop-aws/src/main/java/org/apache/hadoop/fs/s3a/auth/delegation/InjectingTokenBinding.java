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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.S3AUtils.buildAWSProviderList;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.INJECTING_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.INJECTING_TOKEN_KIND;

/**
 * Token binding for fault injection; can be configured to
 * issue/not issue tokens.
 * The tokens it sends over are simple.
 */
public final class InjectingTokenBinding
    extends AbstractDelegationTokenBinding {

  private static final Logger LOG = LoggerFactory.getLogger(
      InjectingTokenBinding.class);

  /**
   * Wire name of this binding: {@value}.
   */
  private static final String NAME = "InjectingTokens/001";

  private static final AtomicLong ISSUE_COUNT = new AtomicLong();

  public InjectingTokenBinding() {
    super(NAME, INJECTING_TOKEN_KIND);
  }

  @Override
  public AbstractS3ATokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    requireServiceStarted();
    return new InjectingTokenIdentifier(
        getCanonicalUri(),
        getOwnerText(),
        renewer,
        encryptionSecrets,
        ISSUE_COUNT.incrementAndGet());
  }

  @Override
  public AWSCredentialProviderList deployUnbonded() throws IOException {
    requireServiceStarted();
    return loadCredentialProviders();
  }

  private AWSCredentialProviderList loadCredentialProviders()
      throws IOException {
    return buildAWSProviderList(
        getCanonicalUri(),
        getConfig(),
        INJECTING_CREDENTIALS_PROVIDER,
        Collections.emptyList(),
        new HashSet<>());
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier) throws IOException {
    requireServiceStarted();
    convertTokenIdentifier(retrievedIdentifier,
        InjectingTokenIdentifier.class);
    return loadCredentialProviders();
  }

  @Override
  public AbstractS3ATokenIdentifier createEmptyIdentifier() {
    return new InjectingTokenIdentifier();
  }

  /**
   * Get the count of issued tokens.
   * @return issue count.
   */
  public static long getIssueCount() {
    return ISSUE_COUNT.get();
  }
}
