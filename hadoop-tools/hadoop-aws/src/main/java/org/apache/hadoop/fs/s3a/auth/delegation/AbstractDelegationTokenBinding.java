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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DURATION_LOG_AT_INFO;

/**
 *  An AbstractDelegationTokenBinding implementation is a class which
 *  handles the binding of its underlying authentication mechanism to the
 *  Hadoop Delegation token mechanism.
 *
 *  See also {@code org.apache.hadoop.fs.azure.security.WasbDelegationTokenManager}
 *  but note that it assumes Kerberos tokens for which the renewal mechanism
 *  is the sole plugin point.
 *  This class is designed to be more generic.
 *
 *  <b>Lifecycle</b>
 *
 *  It is a Hadoop Service, so has a standard lifecycle: once started
 *  its lifecycle will follow that of the {@link S3ADelegationTokens}
 *  instance which created it --which itself follows the lifecycle of the FS.
 *
 *  One big difference is that
 *  {@link AbstractDTService#bindToFileSystem(URI, org.apache.hadoop.fs.s3a.impl.StoreContext, DelegationOperations)}
 *  will be called
 *  before the {@link #init(Configuration)} operation, this is where
 *  the owning FS is passed in.
 *
 *  Implementations are free to start background operations in their
 *  {@code serviceStart()} method, provided they are safely stopped in
 *  {@code serviceStop()}.
 *
 *  <b>When to check for the ability to issue tokens</b>
 *  Implementations MUST start up without actually holding the secrets
 *  needed to issue tokens (config options, credentials to talk to STS etc)
 *  as in server-side deployments they are not expected to have these.
 *
 *  <b>Retry Policy</b>
 *
 *  All methods which talk to AWS services are expected to do translation,
 *  with retries as they see fit.
 */
public abstract class AbstractDelegationTokenBinding extends AbstractDTService
    implements DelegationTokenBinding {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractDelegationTokenBinding.class);

  /** Token kind: must match that of the token identifiers issued. */
  private final Text kind;

  private SecretManager<AbstractS3ATokenIdentifier> secretManager;

  /**
   * Constructor.
   *
   * @param name as passed to superclass for use in log messages.
   * @param kind token kind.
   */
  protected AbstractDelegationTokenBinding(final String name,
      final Text kind) {
    super(name);
    this.kind = requireNonNull(kind);
  }

  @Override
  public Text getKind() {
    return kind;
  }

  @Override
  public Text getOwnerText() {
    return new Text(getOwner().getUserName());
  }

  @Override
  public S3ADelegationTokens.TokenIssuingPolicy getTokenIssuingPolicy() {
    return S3ADelegationTokens.TokenIssuingPolicy.RequestNewToken;
  }

  @Override
  public Token<AbstractS3ATokenIdentifier> createDelegationToken(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets,
      final Text renewer) throws IOException {
    requireServiceStarted();
    final AbstractS3ATokenIdentifier tokenIdentifier =
            createTokenIdentifier(policy, encryptionSecrets, renewer);
    if (tokenIdentifier != null) {
      Token<AbstractS3ATokenIdentifier> token =
          new Token<>(tokenIdentifier, secretManager);
      token.setKind(getKind());
      LOG.debug("Created token {} with token identifier {}",
          token, tokenIdentifier);
      return token;
    } else {
      return null;
    }
  }

  /**
   * Verify that a token identifier is of a specific class.
   * This will reject subclasses (i.e. it is stricter than
   * {@code instanceof}, then cast it to that type.
   * @param <T> type of S3A delegation ttoken identifier.
   * @param identifier identifier to validate
   * @param expectedClass class of the expected token identifier.
   * @return token identifier.
   * @throws DelegationTokenIOException If the wrong class was found.
   */
  protected <T extends AbstractS3ATokenIdentifier> T convertTokenIdentifier(
      final AbstractS3ATokenIdentifier identifier,
      final Class<T> expectedClass) throws DelegationTokenIOException {
    if (!identifier.getClass().equals(expectedClass)) {
      throw new DelegationTokenIOException(
          DelegationTokenIOException.TOKEN_WRONG_CLASS
              + "; expected a token identifier of type "
              + expectedClass
              + " but got "
              + identifier.getClass()
              + " and kind " + identifier.getKind());
    }
    return (T) identifier;
  }

  @Override
  public String toString() {
    return super.toString()
        + " token kind = " + getKind();
  }

  /**
   * Service startup: create the secret manager.
   * @throws Exception failure.
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    secretManager = createSecretMananger();
  }

  @Override
  public String getDescription() {
    return "Token binding " + getKind().toString();
  }

  /**
   * Create a secret manager.
   * @return a secret manager.
   * @throws IOException on failure
   */
  protected SecretManager<AbstractS3ATokenIdentifier> createSecretMananger()
      throws IOException {
    return new TokenSecretManager();
  }

  @Override
  public String getUserAgentField() {
    return "";
  }

  /**
   * Was this DT binding deployed as a secondary instance.
   * @return true if it is.
   */
  protected boolean isSecondaryBinding() {
    return getBindingData().isSecondaryBinding();
  }

  /**
   * Get the password to use in secret managers.
   * This is a constant; its just recalculated every time to stop findbugs
   * highlighting security risks of shared mutable byte arrays.
   * @return a password.
   */
  protected static byte[] getSecretManagerPasssword() {
    return "non-password".getBytes(StandardCharsets.UTF_8);
  }

  /**
   * The secret manager always uses the same secret; the
   * factory for new identifiers is that of the token manager.
   */
  protected class TokenSecretManager
      extends SecretManager<AbstractS3ATokenIdentifier> {

    @Override
    protected byte[] createPassword(AbstractS3ATokenIdentifier identifier) {
      return getSecretManagerPasssword();
    }

    @Override
    public byte[] retrievePassword(AbstractS3ATokenIdentifier identifier)
        throws InvalidToken {
      return getSecretManagerPasssword();
    }

    @Override
    public AbstractS3ATokenIdentifier createIdentifier() {
      try (DurationInfo ignored = new DurationInfo(LOG, DURATION_LOG_AT_INFO,
          "Creating Delegation Token Identifier")) {
        return AbstractDelegationTokenBinding.this.createEmptyIdentifier();
      }
    }
  }
}
