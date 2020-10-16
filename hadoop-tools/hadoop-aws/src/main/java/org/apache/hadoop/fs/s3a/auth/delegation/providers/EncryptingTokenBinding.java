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

package org.apache.hadoop.fs.s3a.auth.delegation.providers;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.delegation.AbstractDelegationTokenBinding;
import org.apache.hadoop.fs.s3a.auth.delegation.AbstractS3ATokenIdentifier;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.auth.delegation.ExtensionBindingData;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.ENCRYPTING_TOKEN_KIND;

/**
 * Token binding for passing encryption settings/secrets only.
 * <p></p>
 * The tokens it sends over contain a counter and encryption
 * secrets but not any AWS credentials.
 */
public final class EncryptingTokenBinding
    extends AbstractDelegationTokenBinding {

  private static final Logger LOG = LoggerFactory.getLogger(
      EncryptingTokenBinding.class);

  /**
   * Wire name of this binding: {@value}.
   */
  private static final String NAME = "EncryptingTokens/001";

  /**
   * Counter of tokens issued.
   */
  private static final AtomicLong ISSUE_COUNT = new AtomicLong();

  public EncryptingTokenBinding() {
    super(NAME, ENCRYPTING_TOKEN_KIND);
  }

  @Override
  public void initializeTokenBinding(final ExtensionBindingData binding)
      throws IOException {
    super.initializeTokenBinding(binding);
    if (binding.isSecondaryBinding()) {
      LOG.warn("Encryption options are only extracted from the primary token"
          + " of a filesystem");
    }
  }

  @Override
  public AbstractS3ATokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    requireServiceStarted();
    return new EncryptingTokenIdentifier(
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

  /**
   * Use the standard provider list.
   * @return the provider list as defined in the standard configuration options.
   * @throws IOException failure
   */
  private AWSCredentialProviderList loadCredentialProviders()
      throws IOException {
    return createAWSCredentialProviderSet(getStoreContext().getFsURI(),
        getConfig());
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier) throws IOException {
    requireServiceStarted();
    convertTokenIdentifier(retrievedIdentifier,
        EncryptingTokenIdentifier.class);
    return loadCredentialProviders();
  }

  @Override
  public AbstractS3ATokenIdentifier createEmptyIdentifier() {
    return new EncryptingTokenIdentifier();
  }

  /**
   * Get the count of issued tokens.
   * @return issue count.
   */
  public static long getIssueCount() {
    return ISSUE_COUNT.get();
  }

}
