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
import java.util.Optional;

import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;

/**
 * A secondary delegation token binding for an S3A bucket.
 * <p></p>
 * These are instantiated as per the primary, but their tokens are issued
 * separately with a different suffix to the S3A URI. This is to make
 * their service Text values different from that of the primary.
 */
class SecondaryDelegationToken extends AbstractDTService implements
    DelegationTokenBinding {

  /**
   * Text value of this token service.
   */
  private final Text service;

  /**
   * Active Delegation token.
   */
  private Optional<Token<AbstractS3ATokenIdentifier>> boundDT
      = Optional.empty();

  /**
   * The DT decoded when this instance is created by bonding
   * to an existing DT.
   */
  private AbstractS3ATokenIdentifier decodedIdentifier;

  /**
   * Token binding; lifecycle matches this object.
   */
  private final DelegationTokenBinding tokenBinding;

  /**
   * Name of the token binding as extracted from token kind; used for
   * logging.
   */
  private final String canonicalServiceName;

  SecondaryDelegationToken(
      final Text service,
      final DelegationTokenBinding tokenBinding) {
    super(tokenBinding.getName());
    this.service = service;
    this.tokenBinding = tokenBinding;
    this.canonicalServiceName = service.toString();
  }

  @Override
  public Text getKind() {
    return tokenBinding.getKind();
  }

  @Override
  public Text getOwnerText() {
    return tokenBinding.getOwnerText();
  }

  @Override
  public S3ADelegationTokens.TokenIssuingPolicy getTokenIssuingPolicy() {
    return tokenBinding.getTokenIssuingPolicy();
  }

  @Override
  public Token<AbstractS3ATokenIdentifier> createDelegationToken(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    return tokenBinding.createDelegationToken(policy, encryptionSecrets,
        renewer);
  }

  @Override
  public AbstractS3ATokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    return tokenBinding.createTokenIdentifier(policy, encryptionSecrets,
        renewer);
  }

  @Override
  public AWSCredentialProviderList deployUnbonded() throws IOException {
    return tokenBinding.deployUnbonded();
  }

  public AWSCredentialProviderList lookupToken() throws IOException {
    return tokenBinding.deployUnbonded();
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier) throws IOException {
    decodedIdentifier = retrievedIdentifier;
    return tokenBinding.bindToTokenIdentifier(retrievedIdentifier);
  }

  @Override
  public AbstractS3ATokenIdentifier createEmptyIdentifier() {
    return tokenBinding.createEmptyIdentifier();
  }

  @Override
  public String getDescription() {
    return tokenBinding.getDescription();
  }

  @Override
  public String getUserAgentField() {
    return tokenBinding.getUserAgentField();
  }

  public String getCanonicalServiceName() {
    return canonicalServiceName;
  }

  @Override
  public void initalizeBindingData(final ExtensionBindingData binding) {
    tokenBinding.initalizeBindingData(binding);
  }

  /**
   * Return any DT from the inner token binding.
   * This does not dynamically create tokens;
   */
  public Token<?> getDelegationToken() throws IOException {
    return boundDT.orElse(null);

  }

  /**
   * Create an issuer for tokens.
   * @param policy role policy.
   * @param encryptionSecrets encryption secrets.
   * @param callbacks callbacks to the DT support.
   * @return the issuer.
   * @throws IOException failure to create.
   */
  public S3ATokenIssuer createTokenIssuer(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final TokenIssueCallbacks callbacks) throws IOException {
    return new S3ATokenIssuer(this, policy, encryptionSecrets, service,
        callbacks);
  }

  /**
   * Look up a token from the credentials, verify it is of the correct
   * kind.
   * @param credentials credentials to look up.
   * @return the token or null if no suitable token was found
   * @throws DelegationTokenIOException wrong token kind found
   */
  public Token<AbstractS3ATokenIdentifier> lookupToken(
      final Credentials credentials) throws DelegationTokenIOException {
    return S3ADelegationTokens.lookupToken(credentials,
        service,
        getKind());
  }

}
