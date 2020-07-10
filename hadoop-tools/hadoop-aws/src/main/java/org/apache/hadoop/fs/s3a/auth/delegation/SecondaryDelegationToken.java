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

import org.apache.hadoop.conf.Configuration;
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
  private Text serviceName;

  /**
   * Active Delegation token.
   */
  private Token<AbstractS3ATokenIdentifier> boundDT;

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
  private String canonicalServiceName;


  /**
   * Instantiate.
   * @param tokenBinding inner token binding.
   */
  SecondaryDelegationToken(
      final DelegationTokenBinding tokenBinding) {
    super(tokenBinding.getName());
    this.tokenBinding = tokenBinding;
  }

  void setServiceName(final Text serviceName) {
    this.serviceName = serviceName;
    this.canonicalServiceName = serviceName.toString();
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    tokenBinding.init(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    tokenBinding.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    tokenBinding.stop();
  }

  /**
   * Get the service text used to register/locate the token.
   *
   * @return service for this token.
   */
  public Text getServiceName() {
    return serviceName;
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

  /**
   * Get any bound DT or create a new one.
   * @return a delegation token.
   * @param callbacks callbacks on token issue
   * @param policy policy for new tokens
   * @param encryptionSecrets encryption secrets for any new token.
   * @param renewer the token renewer.
   * @throws IOException if one cannot be created
   */
  public Token<AbstractS3ATokenIdentifier> getBoundOrNewDT(
      final TokenIssueCallbacks callbacks,
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer)
      throws IOException {
    if (boundDT != null) {
      // the FS was created on startup with a token, so return it.
      return boundDT;
    } else {
      // not bound to a token, so create a new one.
      // issued DTs are not cached so that long-lived filesystems can
      // reliably issue session/role tokens.
      Token<AbstractS3ATokenIdentifier> token = createDelegationToken(
          policy, encryptionSecrets, renewer);
      callbacks.tokenCreated(token);
      return token;
    }
  }

  @Override
  public AWSCredentialProviderList deployUnbonded() throws IOException {
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
  public void initializeTokenBinding(final ExtensionBindingData binding)
      throws IOException {
    super.initializeTokenBinding(binding);
    tokenBinding.initializeTokenBinding(binding);
  }

  @Override
  public Text buildCanonicalNameForSecondaryBinding(final String fsURI) {
    return tokenBinding.buildCanonicalNameForSecondaryBinding(fsURI);
  }

  /**
   * Return any DT from the inner token binding.
   * This does not dynamically create tokens;
   */
  public Token<AbstractS3ATokenIdentifier> getBoundDT() throws IOException {
    return boundDT;
  }

  /**
   * Create an issuer for tokens.
   * @param policy role policy.
   * @param encryptionSecrets encryption secrets.
   * @param callbacks callbacks to the DT support.
   * @return the issuer.
   */
  public S3ATokenIssuer createTokenIssuer(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final TokenIssueCallbacks callbacks) {
    return new S3ATokenIssuer(this, policy, encryptionSecrets, serviceName,
        callbacks);
  }

  /**
   * Look up a token from the credentials, verify it is of the correct
   * kind.
   * This updates the {@link #boundDT} field.
   * @param credentials credentials to look up.
   * @return the token or null if no suitable token was found
   * @throws DelegationTokenIOException wrong token kind found
   */
  public Token<AbstractS3ATokenIdentifier> bindToToken(
      final Credentials credentials) throws DelegationTokenIOException {
    Token<AbstractS3ATokenIdentifier> token
        = S3ADelegationTokens.lookupToken(credentials,
        serviceName,
        getKind());
    boundDT = token;
    return token;
  }

  AbstractS3ATokenIdentifier getDecodedIdentifier() {
    return decodedIdentifier;
  }
}
