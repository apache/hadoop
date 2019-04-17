/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.S3SecretManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.OzoneSecretStore.OzoneManagerSecretState;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier.TokenInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3TOKEN;

/**
 * SecretManager for Ozone Master. Responsible for signing identifiers with
 * private key,
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneDelegationTokenSecretManager
    extends OzoneSecretManager<OzoneTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneDelegationTokenSecretManager.class);
  private final Map<OzoneTokenIdentifier, TokenInfo> currentTokens;
  private final OzoneSecretStore store;
  private final S3SecretManagerImpl s3SecretManager;
  private Thread tokenRemoverThread;
  private final long tokenRemoverScanInterval;
  private String omCertificateSerialId;
  /**
   * If the delegation token update thread holds this lock, it will not get
   * interrupted.
   */
  private Object noInterruptsLock = new Object();

  /**
   * Create a secret manager.
   *
   * @param conf configuration.
   * @param tokenMaxLifetime the maximum lifetime of the delegation tokens in
   * milliseconds
   * @param tokenRenewInterval how often the tokens must be renewed in
   * milliseconds
   * @param dtRemoverScanInterval how often the tokens are scanned for expired
   * tokens in milliseconds
   */
  public OzoneDelegationTokenSecretManager(OzoneConfiguration conf,
      long tokenMaxLifetime, long tokenRenewInterval,
      long dtRemoverScanInterval, Text service,
      S3SecretManager s3SecretManager) throws IOException {
    super(new SecurityConfig(conf), tokenMaxLifetime, tokenRenewInterval,
        service, LOG);
    currentTokens = new ConcurrentHashMap();
    this.tokenRemoverScanInterval = dtRemoverScanInterval;
    this.s3SecretManager = (S3SecretManagerImpl) s3SecretManager;
    this.store = new OzoneSecretStore(conf,
        this.s3SecretManager.getOmMetadataManager());
    loadTokenSecretState(store.loadState());
  }

  @Override
  public OzoneTokenIdentifier createIdentifier() {
    return OzoneTokenIdentifier.newInstance();
  }

  /**
   * Create new Identifier with given,owner,renwer and realUser.
   *
   * @return T
   */
  public OzoneTokenIdentifier createIdentifier(Text owner, Text renewer,
      Text realUser) {
    return OzoneTokenIdentifier.newInstance(owner, renewer, realUser);
  }

  /**
   * Returns {@link Token} for given identifier.
   *
   * @param owner
   * @param renewer
   * @param realUser
   * @return Token
   * @throws IOException to allow future exceptions to be added without breaking
   * compatibility
   */
  public Token<OzoneTokenIdentifier> createToken(Text owner, Text renewer,
      Text realUser)
      throws IOException {
    OzoneTokenIdentifier identifier = createIdentifier(owner, renewer,
        realUser);
    updateIdentifierDetails(identifier);

    byte[] password = createPassword(identifier.getBytes(),
        getCurrentKey().getPrivateKey());
    long expiryTime = identifier.getIssueDate() + getTokenRenewInterval();
    addToTokenStore(identifier, password, expiryTime);
    Token<OzoneTokenIdentifier> token = new Token<>(identifier.getBytes(),
        password, identifier.getKind(), getService());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created delegation token: {}", token);
    }
    return token;
  }

  /**
   * Stores given identifier in token store.
   *
   * @param identifier
   * @param password
   * @throws IOException
   */
  private void addToTokenStore(OzoneTokenIdentifier identifier,
      byte[] password, long renewTime)
      throws IOException {
    TokenInfo tokenInfo = new TokenInfo(renewTime, password,
        identifier.getTrackingId());
    currentTokens.put(identifier, tokenInfo);
    store.storeToken(identifier, tokenInfo.getRenewDate());
  }

  /**
   * Updates issue date, master key id and sequence number for identifier.
   *
   * @param identifier the identifier to validate
   */
  private void updateIdentifierDetails(OzoneTokenIdentifier identifier) {
    int sequenceNum;
    long now = Time.now();
    sequenceNum = incrementDelegationTokenSeqNum();
    identifier.setIssueDate(now);
    identifier.setMasterKeyId(getCurrentKey().getKeyId());
    identifier.setSequenceNumber(sequenceNum);
    identifier.setMaxDate(now + getTokenMaxLifetime());
    identifier.setOmCertSerialId(getOmCertificateSerialId());
  }

  /**
   * Get OM certificate serial id.
   * */
  private String getOmCertificateSerialId() {
    if (omCertificateSerialId == null) {
      omCertificateSerialId =
          getCertClient().getCertificate().getSerialNumber().toString();
    }
    return omCertificateSerialId;
  }

  /**
   * Renew a delegation token.
   *
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  @Override
  public synchronized long renewToken(Token<OzoneTokenIdentifier> token,
      String renewer) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    OzoneTokenIdentifier id = OzoneTokenIdentifier.readProtoBuf(in);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Token renewal for identifier: {}, total currentTokens: {}",
          formatTokenId(id), currentTokens.size());
    }

    long now = Time.now();
    if (id.getMaxDate() < now) {
      throw new OMException(renewer + " tried to renew an expired token "
          + formatTokenId(id) + " max expiration date: "
          + Time.formatTime(id.getMaxDate())
          + " currentTime: " + Time.formatTime(now), TOKEN_EXPIRED);
    }
    validateToken(id);
    if ((id.getRenewer() == null) || (id.getRenewer().toString().isEmpty())) {
      throw new AccessControlException(renewer +
          " tried to renew a token " + formatTokenId(id)
          + " without a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new AccessControlException(renewer
          + " tries to renew a token " + formatTokenId(id)
          + " with non-matching renewer " + id.getRenewer());
    }

    long renewTime = Math.min(id.getMaxDate(), now + getTokenRenewInterval());
    try {
      addToTokenStore(id, token.getPassword(),  renewTime);
    } catch (IOException e) {
      LOG.error("Unable to update token " + id.getSequenceNumber(), e);
    }
    return renewTime;
  }

  /**
   * Cancel a token by removing it from store and cache.
   *
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public OzoneTokenIdentifier cancelToken(Token<OzoneTokenIdentifier> token,
      String canceller) throws IOException {
    OzoneTokenIdentifier id = OzoneTokenIdentifier.readProtoBuf(
        token.getIdentifier());
    LOG.debug("Token cancellation requested for identifier: {}",
        formatTokenId(id));

    if (id.getUser() == null) {
      throw new InvalidToken("Token with no owner " + formatTokenId(id));
    }
    String owner = id.getUser().getUserName();
    Text renewer = id.getRenewer();
    HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty()
        || !cancelerShortName
        .equals(renewer.toString()))) {
      throw new AccessControlException(canceller
          + " is not authorized to cancel the token " + formatTokenId(id));
    }
    try {
      store.removeToken(id);
    } catch (IOException e) {
      LOG.error("Unable to remove token " + id.getSequenceNumber(), e);
    }
    TokenInfo info = currentTokens.remove(id);
    if (info == null) {
      throw new InvalidToken("Token not found " + formatTokenId(id));
    }
    return id;
  }

  @Override
  public byte[] retrievePassword(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    if(identifier.getTokenType().equals(S3TOKEN)) {
      return validateS3Token(identifier);
    }
    return validateToken(identifier).getPassword();
  }

  /**
   * Checks if TokenInfo for the given identifier exists in database and if the
   * token is expired.
   */
  private TokenInfo validateToken(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    TokenInfo info = currentTokens.get(identifier);
    if (info == null) {
      throw new InvalidToken("token " + formatTokenId(identifier)
          + " can't be found in cache");
    }
    long now = Time.now();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + Time.formatTime(now) +
          " expected renewal time: " + Time.formatTime(info.getRenewDate()));
    }
    if (!verifySignature(identifier, info.getPassword())) {
      throw new InvalidToken("Tampered/Invalid token.");
    }
    return info;
  }

  /**
   * Validates if given hash is valid.
   *
   * @param identifier
   * @param password
   */
  public boolean verifySignature(OzoneTokenIdentifier identifier,
      byte[] password) {
    try {
      return getCertClient().verifySignature(identifier.getBytes(), password,
          getCertClient().getCertificate(identifier.getOmCertSerialId()));
    } catch (CertificateException e) {
      return false;
    }
  }

  /**
   * Validates if a S3 identifier is valid or not.
   * */
  private byte[] validateS3Token(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    LOG.trace("Validating S3Token for identifier:{}", identifier);
    String awsSecret;
    try {
      awsSecret = s3SecretManager.getS3UserSecretString(identifier
          .getAwsAccessId());
    } catch (IOException e) {
      LOG.error("Error while validating S3 identifier:{}",
          identifier, e);
      throw new InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (awsSecret == null) {
      throw new InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (AWSV4AuthValidator.validateRequest(identifier.getStrToSign(),
        identifier.getSignature(), awsSecret)) {
      return identifier.getSignature().getBytes(UTF_8);
    }
    throw new InvalidToken("Invalid S3 identifier:"
        + identifier);

  }

  private void loadTokenSecretState(
      OzoneManagerSecretState<OzoneTokenIdentifier> state) throws IOException {
    LOG.info("Loading token state into token manager.");
    for (Map.Entry<OzoneTokenIdentifier, Long> entry :
        state.getTokenState().entrySet()) {
      addPersistedDelegationToken(entry.getKey(), entry.getValue());
    }
  }

  private void addPersistedDelegationToken(OzoneTokenIdentifier identifier,
      long renewDate) throws IOException {
    if (isRunning()) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }

    byte[] password = createPassword(identifier.getBytes(),
        getCertClient().getPrivateKey());
    if (identifier.getSequenceNumber() > getDelegationTokenSeqNum()) {
      setDelegationTokenSeqNum(identifier.getSequenceNumber());
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new TokenInfo(renewDate,
          password, identifier.getTrackingId()));
    } else {
      throw new IOException("Same delegation token being added twice: "
          + formatTokenId(identifier));
    }
  }

  /**
   * Should be called before this object is used.
   */
  @Override
  public synchronized void start(CertificateClient certClient)
      throws IOException {
    super.start(certClient);
    tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
    tokenRemoverThread.start();
  }

  public void stopThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping expired delegation token remover thread");
    }
    setIsRunning(false);

    if (tokenRemoverThread != null) {
      synchronized (noInterruptsLock) {
        tokenRemoverThread.interrupt();
      }
      try {
        tokenRemoverThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Unable to join on token removal thread", e);
      }
    }
  }

  /**
   * Stops the OzoneDelegationTokenSecretManager.
   *
   * @throws IOException
   */
  @Override
  public void stop() throws IOException {
    super.stop();
    stopThreads();
    if (this.store != null) {
      this.store.close();
    }
  }

  /**
   * Remove expired delegation tokens from cache and persisted store.
   */
  private void removeExpiredToken() {
    long now = Time.now();
    synchronized (this) {
      Iterator<Map.Entry<OzoneTokenIdentifier,
          TokenInfo>> i = currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<OzoneTokenIdentifier,
            TokenInfo> entry = i.next();
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          i.remove();
          try {
            store.removeToken(entry.getKey());
          } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to remove expired token {}", entry.getValue());
            }
          }
        }
      }
    }
  }

  private class ExpiredTokenRemover extends Thread {

    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + getTokenRemoverScanInterval()
          / (60 * 1000) + " min(s)");
      try {
        while (isRunning()) {
          long now = Time.now();
          if (lastTokenCacheCleanup + getTokenRemoverScanInterval()
              < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(Math.min(5000,
                getTokenRemoverScanInterval())); // 5 seconds
          } catch (InterruptedException ie) {
            LOG.error("ExpiredTokenRemover received " + ie);
          }
        }
      } catch (Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception",
            t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  public long getTokenRemoverScanInterval() {
    return tokenRemoverScanInterval;
  }
}
