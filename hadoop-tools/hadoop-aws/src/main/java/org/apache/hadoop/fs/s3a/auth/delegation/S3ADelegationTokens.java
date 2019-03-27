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
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DEFAULT_DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DURATION_LOG_AT_INFO;

/**
 * Support for creating a DT from a filesystem.
 *
 * Isolated from S3A for control and testability.
 *
 * The S3A Delegation Tokens are special in that the tokens are not directly
 * used to authenticate with the AWS services.
 * Instead they can session/role  credentials requested off AWS on demand.
 *
 * The design is extensible in that different back-end bindings can be used
 * to switch to different session creation mechanisms, or indeed, to any
 * other authentication mechanism supported by an S3 service, provided it
 * ultimately accepts some form of AWS credentials for authentication through
 * the AWS SDK. That is, if someone wants to wire this up to Kerberos, or
 * OAuth2, this design should support them.
 *
 * URIs processed must be the canonical URIs for the service.
 */
@InterfaceAudience.Private
public class S3ADelegationTokens extends AbstractDTService {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ADelegationTokens.class);

  @VisibleForTesting
  static final String E_ALREADY_DEPLOYED
      = "S3A Delegation tokens has already been bound/deployed";

  public static final String E_DELEGATION_TOKENS_DISABLED
      = "Delegation tokens are not enabled";

  /**
   * User who owns this FS; fixed at instantiation time, so that
   * in calls to getDelegationToken() and similar, this user is the one whose
   * credentials are involved.
   */
  private final UserGroupInformation user;

  /**
   * Count of number of created tokens.
   * For testing and diagnostics.
   */
  private final AtomicInteger creationCount = new AtomicInteger(0);

  /**
   * Text value of this token service.
   */
  private Text service;

  /**
   * Active Delegation token.
   */
  private Optional<Token<AbstractS3ATokenIdentifier>> boundDT
      = Optional.empty();

  /**
   * The DT decoded when this instance is created by bonding
   * to an existing DT.
   */
  private Optional<AbstractS3ATokenIdentifier> decodedIdentifier
      = Optional.empty();

  /**
   * Dynamically loaded token binding; lifecycle matches this object.
   */
  private AbstractDelegationTokenBinding tokenBinding;

  /**
   * List of cred providers; unset until {@link #bindToDelegationToken(Token)}.
   */
  private Optional<AWSCredentialProviderList> credentialProviders
      = Optional.empty();

  /**
   * The access policies we want for operations.
   * There's no attempt to ask for "admin" permissions here, e.g.
   * those to manipulate S3Guard tables.
   */
  protected static final EnumSet<AWSPolicyProvider.AccessLevel> ACCESS_POLICY
      = EnumSet.of(
          AWSPolicyProvider.AccessLevel.READ,
          AWSPolicyProvider.AccessLevel.WRITE);

  /**
   * Statistics for the owner FS.
   */
  private S3AInstrumentation.DelegationTokenStatistics stats;

  /**
   * Name of the token binding as extracted from token kind; used for
   * logging.
   */
  private String tokenBindingName = "";

  /**
   * Instantiate.
   * @throws IOException if login fails.
   */
  public S3ADelegationTokens() throws IOException {
    super("S3ADelegationTokens");
    user = UserGroupInformation.getCurrentUser();
  }

  @Override
  public void bindToFileSystem(final URI uri, final S3AFileSystem fs)
      throws IOException {
    super.bindToFileSystem(uri, fs);
    service = getTokenService(getCanonicalUri());
    stats = fs.getInstrumentation().newDelegationTokenStatistics();
  }

  /**
   * Init the service.
   * This identifies the token binding class to use and creates, initializes
   * and starts it.
   * Will raise an exception if delegation tokens are not enabled.
   * @param conf configuration
   * @throws Exception any failure to start up
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    checkState(hasDelegationTokenBinding(conf),
        E_DELEGATION_TOKENS_DISABLED);
    Class<? extends AbstractDelegationTokenBinding> binding = conf.getClass(
        DelegationConstants.DELEGATION_TOKEN_BINDING,
        SessionTokenBinding.class,
        AbstractDelegationTokenBinding.class);
    tokenBinding = binding.newInstance();
    tokenBinding.bindToFileSystem(getCanonicalUri(), getFileSystem());
    tokenBinding.init(conf);
    tokenBindingName = tokenBinding.getKind().toString();
    LOG.info("Filesystem {} is using delegation tokens of kind {}",
        getCanonicalUri(), tokenBindingName);
  }

  /**
   * Service startup includes binding to any delegation token, and
   * deploying unbounded if there is none.
   * It is after this that token operations can be used.
   * @throws Exception any failure
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    tokenBinding.start();
    bindToAnyDelegationToken();
    LOG.info("S3A Delegation support token {} with {}",
        identifierToString(),
        tokenBinding.getDescription());
  }

  /**
   * Get the identifier as a string, or "(none)".
   * @return a string value for logs etc.
   */
  private String identifierToString() {
    return decodedIdentifier.map(Objects::toString)
        .orElse("(none)");
  }

  /**
   * Stop the token binding.
   * @throws Exception on any failure
   */
  @SuppressWarnings("ThrowableNotThrown")
  @Override
  protected void serviceStop() throws Exception {
    LOG.debug("Stopping delegation tokens");
    try {
      super.serviceStop();
    } finally {
      ServiceOperations.stopQuietly(LOG, tokenBinding);
    }
  }


  /**
   * Perform the unbonded deployment operations.
   * Create the AWS credential provider chain to use
   * when talking to AWS when there is no delegation token to work with.
   * authenticating this client with AWS services, and saves it
   * to {@link #credentialProviders}
   *
   * @throws IOException any failure.
   */
  private void deployUnbonded()
      throws IOException {
    requireServiceStarted();
    checkState(!isBoundToDT(),
        "Already Bound to a delegation token");
    LOG.info("No delegation tokens present: using direct authentication");
    credentialProviders = Optional.of(tokenBinding.deployUnbonded());
  }

  /**
   * Attempt to bind to any existing DT, including unmarshalling its contents
   * and creating the AWS credential provider used to authenticate
   * the client.
   *
   * If successful:
   * <ol>
   *   <li>{@link #boundDT} is set to the retrieved token.</li>
   *   <li>{@link #decodedIdentifier} is set to the extracted identifier.</li>
   *   <li>{@link #credentialProviders} is set to the credential
   *   provider(s) returned by the token binding.</li>
   * </ol>
   * If unsuccessful, {@link #deployUnbonded()} is called for the
   * unbonded codepath instead, which will set
   * {@link #credentialProviders} to its value.
   *
   * This means after this call (and only after) the token operations
   * can be invoked.
   *
   * This method is called from {@link #serviceStart()}, so a check on
   * the service state can be used to check things; the state model
   * prevents re-entrant calls.
   * @throws IOException selection/extraction/validation failure.
   */
  private void bindToAnyDelegationToken() throws IOException {
    checkState(!credentialProviders.isPresent(), E_ALREADY_DEPLOYED);
    Token<AbstractS3ATokenIdentifier> token = selectTokenFromFSOwner();
    if (token != null) {
      bindToDelegationToken(token);
    } else {
      deployUnbonded();
    }
    if (credentialProviders.get().size() == 0) {
      throw new DelegationTokenIOException("No AWS credential providers"
          + " created by Delegation Token Binding "
          + tokenBinding.getName());
    }
  }

  /**
   * This is a test-only back door which resets the state and binds to
   * a token again.
   * This allows an instance of this class to be bonded to a DT after being
   * started, so avoids the need to have the token in the current user
   * credentials. It is package scoped so as to only be usable in tests
   * in the same package.
   *
   * Yes, this is ugly, but there is no obvious/easy way to test token
   * binding without Kerberos getting involved.
   * @param token token to decode and bind to.
   * @throws IOException selection/extraction/validation failure.
   */
  @VisibleForTesting
  void resetTokenBindingToDT(final Token<AbstractS3ATokenIdentifier> token)
      throws IOException{
    credentialProviders = Optional.empty();
    bindToDelegationToken(token);
  }

  /**
   * Bind to a delegation token retrieved for this filesystem.
   * Extract the secrets from the token and set internal fields
   * to the values.
   * <ol>
   *   <li>{@link #boundDT} is set to {@code token}.</li>
   *   <li>{@link #decodedIdentifier} is set to the extracted identifier.</li>
   *   <li>{@link #credentialProviders} is set to the credential
   *   provider(s) returned by the token binding.</li>
   * </ol>
   * @param token token to decode and bind to.
   * @throws IOException selection/extraction/validation failure.
   */
  @VisibleForTesting
  public void bindToDelegationToken(
      final Token<AbstractS3ATokenIdentifier> token)
      throws IOException {
    checkState(!credentialProviders.isPresent(), E_ALREADY_DEPLOYED);
    boundDT = Optional.of(token);
    AbstractS3ATokenIdentifier dti = extractIdentifier(token);
    LOG.info("Using delegation token {}", dti);
    decodedIdentifier = Optional.of(dti);
    try (DurationInfo ignored = new DurationInfo(LOG, DURATION_LOG_AT_INFO,
        "Creating Delegation Token")) {
      // extract the credential providers.
      credentialProviders = Optional.of(
          tokenBinding.bindToTokenIdentifier(dti));
    }
  }

  /**
   * Predicate: is there a bound DT?
   * @return true if there's a value in {@link #boundDT}.
   */
  public boolean isBoundToDT() {
    return boundDT.isPresent();
  }

  /**
   * Get any bound DT.
   * @return a delegation token if this instance was bound to it.
   */
  public Optional<Token<AbstractS3ATokenIdentifier>> getBoundDT() {
    return boundDT;
  }

  /**
   * Predicate: will this binding issue a DT if requested
   * in a call to {@link #getBoundOrNewDT(EncryptionSecrets)}?
   * That is: should the filesystem declare that it is issuing
   * delegation tokens?
   * @return a declaration of what will happen when asked for a token.
   */
  public TokenIssuingPolicy getTokenIssuingPolicy() {
    return isBoundToDT()
        ? TokenIssuingPolicy.ReturnExistingToken
        : tokenBinding.getTokenIssuingPolicy();
  }

  /**
   * Get any bound DT or create a new one.
   * @return a delegation token.
   * @throws IOException if one cannot be created
   * @param encryptionSecrets encryption secrets for any new token.
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public Token<AbstractS3ATokenIdentifier> getBoundOrNewDT(
      final EncryptionSecrets encryptionSecrets)
      throws IOException {
    LOG.debug("Delegation token requested");
    if (isBoundToDT()) {
      // the FS was created on startup with a token, so return it.
      LOG.debug("Returning current token");
      return getBoundDT().get();
    } else {
      // not bound to a token, so create a new one.
      // issued DTs are not cached so that long-lived filesystems can
      // reliably issue session/role tokens.
      return createDelegationToken(encryptionSecrets);
    }
  }

  /**
   * How many delegation tokens have been issued?
   * @return the number times {@link #createDelegationToken(EncryptionSecrets)}
   * returned a token.
   */
  public int getCreationCount() {
    return creationCount.get();
  }

  /**
   * Create a delegation token for the user.
   * This will only be called if a new DT is needed, that is: the
   * filesystem has been deployed unbonded.
   * @param encryptionSecrets encryption secrets for the token.
   * @return the token
   * @throws IOException if one cannot be created
   */
  @VisibleForTesting
  public Token<AbstractS3ATokenIdentifier> createDelegationToken(
      final EncryptionSecrets encryptionSecrets) throws IOException {
    requireServiceStarted();
    checkArgument(encryptionSecrets != null,
        "Null encryption secrets");
    // this isn't done in in advance as it needs S3Guard initialized in the
    // filesystem before it can generate complete policies.
    List<RoleModel.Statement> statements = getFileSystem()
        .listAWSPolicyRules(ACCESS_POLICY);
    Optional<RoleModel.Policy> rolePolicy =
        statements.isEmpty() ?
            Optional.empty() : Optional.of(new RoleModel.Policy(statements));

    try(DurationInfo ignored = new DurationInfo(LOG, DURATION_LOG_AT_INFO,
        "Creating New Delegation Token", tokenBinding.getKind())) {
      Token<AbstractS3ATokenIdentifier> token
          = tokenBinding.createDelegationToken(rolePolicy, encryptionSecrets);
      if (token != null) {
        token.setService(service);
        noteTokenCreated(token);
      }
      return token;
    }
  }

  /**
   * Note that a token has been created; increment counters and statistics.
   * @param token token created
   */
  private void noteTokenCreated(final Token<AbstractS3ATokenIdentifier> token) {
    LOG.info("Created S3A Delegation Token: {}", token);
    creationCount.incrementAndGet();
    stats.tokenIssued();
  }

  /**
   * Get the AWS credential provider.
   * @return the DT credential provider
   * @throws IOException failure to parse the DT
   * @throws IllegalStateException if this instance is not bound to a DT
   */
  public AWSCredentialProviderList getCredentialProviders()
      throws IOException {
    return credentialProviders.orElseThrow(
        () -> new DelegationTokenIOException("Not yet bonded"));
  }

  /**
   * Get the encryption secrets of the DT.
   * non-empty iff service is started and was bound to a DT.
   * @return any encryption settings propagated with the DT.
   */
  public Optional<EncryptionSecrets> getEncryptionSecrets() {
    return decodedIdentifier.map(
        AbstractS3ATokenIdentifier::getEncryptionSecrets);
  }

  /**
   * Get any decoded identifier from the bound DT; empty if not bound.
   * @return the decoded identifier.
   */
  public Optional<AbstractS3ATokenIdentifier> getDecodedIdentifier() {
    return decodedIdentifier;
  }

  /**
   * Get the service identifier of the owning FS.
   * @return a service identifier to use when registering tokens
   */
  public Text getService() {
    return service;
  }

  /**
   * The canonical name of the service.
   * This can be used as the canonical service name for the FS.
   * @return the canonicalized FS URI.
   */
  public String getCanonicalServiceName() {
    return getCanonicalUri().toString();
  }

  /**
   * Find a token for the FS user and canonical filesystem URI.
   * @return the token, or null if one cannot be found.
   * @throws IOException on a failure to unmarshall the token.
   */
  @VisibleForTesting
  public Token<AbstractS3ATokenIdentifier> selectTokenFromFSOwner()
      throws IOException {
    return lookupToken(user.getCredentials(),
        service,
        tokenBinding.getKind());
  }

  /**
   * Get the service identifier of a filesystem.
   * This must be unique for (S3A, the FS URI)
   * @param fsURI filesystem URI
   * @return identifier to use.
   */
  private static Text getTokenService(final URI fsURI) {
    return getTokenService(fsURI.toString());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3ADelegationTokens{");
    sb.append("canonicalServiceURI=").append(getCanonicalUri());
    sb.append("; owner=").append(user.getShortUserName());
    sb.append("; isBoundToDT=").append(isBoundToDT());
    sb.append("; token creation count=").append(getCreationCount());
    sb.append("; tokenManager=").append(tokenBinding);
    sb.append("; token=").append(identifierToString());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the kind of the issued tokens.
   * @return token kind.
   */
  public Text getTokenKind() {
    return tokenBinding.getKind();
  }

  /**
   * Get the service identifier of a filesystem URI.
   * This must be unique for (S3a, the FS URI)
   * @param fsURI filesystem URI as a string
   * @return identifier to use.
   */
  @VisibleForTesting
  static Text getTokenService(final String fsURI) {
    return new Text(fsURI);
  }

  /**
   * From a token, get the session token identifier.
   * @param token token to process
   * @return the session token identifier
   * @throws IOException failure to validate/read data encoded in identifier.
   * @throws IllegalArgumentException if the token isn't an S3A session token
   */
  public AbstractS3ATokenIdentifier extractIdentifier(
      final Token<? extends AbstractS3ATokenIdentifier> token)
      throws IOException {

    checkArgument(token != null, "null token");
    AbstractS3ATokenIdentifier identifier;
    // harden up decode beyond that Token does itself
    try {
      identifier = token.decodeIdentifier();
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        // its a wrapping around class instantiation.
        throw new DelegationTokenIOException("Decoding S3A token " + cause,
            cause);
      } else {
        throw e;
      }
    }
    if (identifier == null) {
      throw new DelegationTokenIOException("Failed to unmarshall token for "
          + getCanonicalUri());
    }
    identifier.validate();
    return identifier;
  }

  /**
   * Return a string for use in building up the User-Agent field, so
   * get into the S3 access logs. Useful for diagnostics.
   * Delegates to {{@link AbstractDelegationTokenBinding#getUserAgentField()}}
   * for the current binding.
   * @return a string for the S3 logs or "" for "nothing to add"
   */
  public String getUserAgentField() {
    return tokenBinding.getUserAgentField();
  }

  /**
   * Look up a token from the credentials, verify it is of the correct
   * kind.
   * @param credentials credentials to look up.
   * @param service service name
   * @param kind token kind to look for
   * @return the token or null if no suitable token was found
   * @throws DelegationTokenIOException wrong token kind found
   */
  @VisibleForTesting
  public static Token<AbstractS3ATokenIdentifier> lookupToken(
      final Credentials credentials,
      final Text service,
      final Text kind)
      throws DelegationTokenIOException {

    LOG.debug("Looking for token for service {} in credentials", service);
    Token<?> token = credentials.getToken(service);
    if (token != null) {
      Text tokenKind = token.getKind();
      LOG.debug("Found token of kind {}", tokenKind);
      if (kind.equals(tokenKind)) {
        // the Oauth implementation catches and logs here; this one
        // throws the failure up.
        return (Token<AbstractS3ATokenIdentifier>) token;
      } else {

        // there's a token for this URI, but its not the right DT kind
        throw new DelegationTokenIOException(
            DelegationTokenIOException.TOKEN_MISMATCH + ": expected token"
            + " for " + service
            + " of type " + kind
            + " but got a token of type " + tokenKind);
      }
    }
    // A token for the service was not found
    LOG.debug("No token for {} found", service);
    return null;
  }

  /**
   * Look up any token from the service; cast it to one of ours.
   * @param credentials credentials
   * @param service service to look up
   * @return any token found or null if none was
   * @throws ClassCastException if the token is of a wrong type.
   */
  public static Token<AbstractS3ATokenIdentifier> lookupToken(
      final Credentials credentials,
      final Text service) {
    return (Token<AbstractS3ATokenIdentifier>) credentials.getToken(service);
  }

  /**
   * Look for any S3A token for the given FS service.
   * @param credentials credentials to scan.
   * @param uri the URI of the FS to look for
   * @return the token or null if none was found
   */
  public static Token<AbstractS3ATokenIdentifier> lookupS3ADelegationToken(
      final Credentials credentials,
      final URI uri) {
    return lookupToken(credentials, getTokenService(uri.toString()));
  }

  /**
   * Predicate: does this configuration enable delegation tokens?
   * That is: is there any text in the option
   * {@link DelegationConstants#DELEGATION_TOKEN_BINDING} ?
   * @param conf configuration to examine
   * @return true iff the trimmed configuration option is not empty.
   */
  public static boolean hasDelegationTokenBinding(Configuration conf) {
    return StringUtils.isNotEmpty(
        conf.getTrimmed(DELEGATION_TOKEN_BINDING,
            DEFAULT_DELEGATION_TOKEN_BINDING));
  }

  /**
   * How will tokens be issued on request?
   *
   * The {@link #RequestNewToken} policy does not guarantee that a tokens
   * can be created, only that an attempt will be made to request one.
   * It may fail (wrong credential types, wrong role, etc).
   */
  public enum TokenIssuingPolicy {

    /** The existing token will be returned. */
    ReturnExistingToken,

    /** No tokens will be issued. */
    NoTokensAvailable,

    /** An attempt will be made to request a new DT. */
    RequestNewToken
  }
}
