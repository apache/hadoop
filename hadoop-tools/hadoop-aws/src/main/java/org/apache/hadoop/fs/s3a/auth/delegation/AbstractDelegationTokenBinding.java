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
import java.nio.charset.Charset;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
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
 *  {@link #bindToFileSystem(URI, S3AFileSystem)} will be called
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
public abstract class AbstractDelegationTokenBinding extends AbstractDTService {

  /** Token kind: must match that of the token identifiers issued. */
  private final Text kind;

  private SecretManager<AbstractS3ATokenIdentifier> secretManager;

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractDelegationTokenBinding.class);

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

  /**
   * Get the kind of the tokens managed here.
   * @return the token kind.
   */
  public Text getKind() {
    return kind;
  }

  /**
   * Return the name of the owner to be used in tokens.
   * This may be that of the UGI owner, or it could be related to
   * the AWS login.
   * @return a text name of the owner.
   */
  public Text getOwnerText() {
    return new Text(getOwner().getUserName());
  }

  /**
   * Predicate: will this binding issue a DT?
   * That is: should the filesystem declare that it is issuing
   * delegation tokens? If true
   * @return a declaration of what will happen when asked for a token.
   */
  public S3ADelegationTokens.TokenIssuingPolicy getTokenIssuingPolicy() {
    return S3ADelegationTokens.TokenIssuingPolicy.RequestNewToken;
  }

  /**
   * Create a delegation token for the user.
   * This will only be called if a new DT is needed, that is: the
   * filesystem has been deployed unbonded.
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets for the token.
   * @return the token or null if the back end does not want to issue one.
   * @throws IOException if one cannot be created
   */
  public Token<AbstractS3ATokenIdentifier> createDelegationToken(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets) throws IOException {
    requireServiceStarted();
    final AbstractS3ATokenIdentifier tokenIdentifier =
            createTokenIdentifier(policy, encryptionSecrets);
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
   * Create a token identifier with all the information needed
   * to be included in a delegation token.
   * This is where session credentials need to be extracted, etc.
   * This will only be called if a new DT is needed, that is: the
   * filesystem has been deployed unbonded.
   *
   * If {@link #createDelegationToken(Optional, EncryptionSecrets)}
   * is overridden, this method can be replaced with a stub.
   *
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets for the token.
   * @return the token data to include in the token identifier.
   * @throws IOException failure creating the token data.
   */
  public abstract AbstractS3ATokenIdentifier createTokenIdentifier(
      Optional<RoleModel.Policy> policy,
      EncryptionSecrets encryptionSecrets) throws IOException;

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

  /**
   * Perform any actions when deploying unbonded, and return a list
   * of credential providers.
   * @return non-empty list of AWS credential providers to use for
   * authenticating this client with AWS services.
   * @throws IOException any failure.
   */
  public abstract AWSCredentialProviderList deployUnbonded()
      throws IOException;

  /**
   * Bind to the token identifier, returning the credential providers to use
   * for the owner to talk to S3, DDB and related AWS Services.
   * @param retrievedIdentifier the unmarshalled data
   * @return non-empty list of AWS credential providers to use for
   * authenticating this client with AWS services.
   * @throws IOException any failure.
   */
  public abstract AWSCredentialProviderList bindToTokenIdentifier(
      AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException;

  /**
   * Create a new subclass of {@link AbstractS3ATokenIdentifier}.
   * This is used in the secret manager.
   * @return an empty identifier.
   */
  public abstract AbstractS3ATokenIdentifier createEmptyIdentifier();

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

  /**
   * Return a description.
   * This is logged during after service start and binding:
   * it should be as informative as possible.
   * @return a description to log.
   */
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

  /**
   * Return a string for use in building up the User-Agent field, so
   * get into the S3 access logs. Useful for diagnostics.
   * @return a string for the S3 logs or "" for "nothing to add"
   */
  public String getUserAgentField() {
    return "";
  }

  /**
   * Get the password to use in secret managers.
   * This is a constant; its just recalculated every time to stop findbugs
   * highlighting security risks of shared mutable byte arrays.
   * @return a password.
   */
  protected static byte[] getSecretManagerPasssword() {
    return "non-password".getBytes(Charset.forName("UTF-8"));
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
