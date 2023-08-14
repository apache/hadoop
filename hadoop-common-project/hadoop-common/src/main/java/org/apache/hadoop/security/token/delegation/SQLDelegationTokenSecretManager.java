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

package org.apache.hadoop.security.token.delegation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link AbstractDelegationTokenSecretManager} that
 * persists TokenIdentifiers and DelegationKeys in an existing SQL database.
 */
public abstract class SQLDelegationTokenSecretManager<TokenIdent
    extends AbstractDelegationTokenIdentifier>
    extends AbstractDelegationTokenSecretManager<TokenIdent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLDelegationTokenSecretManager.class);

  public static final String SQL_DTSM_CONF_PREFIX = "sql-dt-secret-manager.";
  private static final String SQL_DTSM_TOKEN_SEQNUM_BATCH_SIZE = SQL_DTSM_CONF_PREFIX
      + "token.seqnum.batch.size";
  public static final int DEFAULT_SEQ_NUM_BATCH_SIZE = 10;
  public static final String SQL_DTSM_TOKEN_MAX_CLEANUP_RESULTS = SQL_DTSM_CONF_PREFIX
      + "token.max.cleanup.results";
  public static final int SQL_DTSM_TOKEN_MAX_CLEANUP_RESULTS_DEFAULT = 1000;
  public static final String SQL_DTSM_TOKEN_LOADING_CACHE_EXPIRATION = SQL_DTSM_CONF_PREFIX
      + "token.loading.cache.expiration";
  public static final long SQL_DTSM_TOKEN_LOADING_CACHE_EXPIRATION_DEFAULT =
      TimeUnit.SECONDS.toMillis(10);
  public static final String SQL_DTSM_TOKEN_LOADING_CACHE_MAX_SIZE = SQL_DTSM_CONF_PREFIX
      + "token.loading.cache.max.size";
  public static final long SQL_DTSM_TOKEN_LOADING_CACHE_MAX_SIZE_DEFAULT = 100000;

  // Batch of sequence numbers that will be requested by the sequenceNumCounter.
  // A new batch is requested once the sequenceNums available to a secret manager are
  // exhausted, including during initialization.
  private final int seqNumBatchSize;

  // Number of tokens to obtain from SQL during the cleanup process.
  private final int maxTokenCleanupResults;

  // Last sequenceNum in the current batch that has been allocated to a token.
  private int currentSeqNum;

  // Max sequenceNum in the current batch that can be allocated to a token.
  // Unused sequenceNums in the current batch cannot be reused by other routers.
  private int currentMaxSeqNum;

  public SQLDelegationTokenSecretManager(Configuration conf) {
    super(conf.getLong(DelegationTokenManager.UPDATE_INTERVAL,
            DelegationTokenManager.UPDATE_INTERVAL_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.MAX_LIFETIME,
            DelegationTokenManager.MAX_LIFETIME_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.RENEW_INTERVAL,
            DelegationTokenManager.RENEW_INTERVAL_DEFAULT) * 1000,
        conf.getLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL,
            DelegationTokenManager.REMOVAL_SCAN_INTERVAL_DEFAULT) * 1000);

    this.seqNumBatchSize = conf.getInt(SQL_DTSM_TOKEN_SEQNUM_BATCH_SIZE,
        DEFAULT_SEQ_NUM_BATCH_SIZE);
    this.maxTokenCleanupResults = conf.getInt(SQL_DTSM_TOKEN_MAX_CLEANUP_RESULTS,
        SQL_DTSM_TOKEN_MAX_CLEANUP_RESULTS_DEFAULT);

    long cacheExpirationMs = conf.getTimeDuration(SQL_DTSM_TOKEN_LOADING_CACHE_EXPIRATION,
        SQL_DTSM_TOKEN_LOADING_CACHE_EXPIRATION_DEFAULT, TimeUnit.MILLISECONDS);
    long maximumCacheSize = conf.getLong(SQL_DTSM_TOKEN_LOADING_CACHE_MAX_SIZE,
        SQL_DTSM_TOKEN_LOADING_CACHE_MAX_SIZE_DEFAULT);
    this.currentTokens = new DelegationTokenLoadingCache<>(cacheExpirationMs, maximumCacheSize,
        this::getTokenInfoFromSQL);
  }

  /**
   * Persists a TokenIdentifier and its corresponding TokenInformation into
   * the SQL database. The TokenIdentifier is expected to be unique and any
   * duplicate token attempts will result in an IOException.
   * @param ident TokenIdentifier to persist.
   * @param tokenInfo DelegationTokenInformation associated with the TokenIdentifier.
   */
  @Override
  protected void storeToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      tokenInfo.write(dos);
      // Add token to SQL database
      insertToken(ident.getSequenceNumber(), ident.getBytes(), bos.toByteArray());
      // Add token to local cache
      super.storeToken(ident, tokenInfo);
    } catch (SQLException e) {
      throw new IOException("Failed to store token in SQL secret manager", e);
    }
  }

  /**
   * Updates the TokenInformation of an existing TokenIdentifier in
   * the SQL database.
   * @param ident Existing TokenIdentifier in the SQL database.
   * @param tokenInfo Updated DelegationTokenInformation associated with the TokenIdentifier.
   */
  @Override
  protected void updateToken(TokenIdent ident,
      DelegationTokenInformation tokenInfo) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        tokenInfo.write(dos);
        // Update token in SQL database
        updateToken(ident.getSequenceNumber(), ident.getBytes(), bos.toByteArray());
        // Update token in local cache
        super.updateToken(ident, tokenInfo);
      }
    } catch (SQLException e) {
      throw new IOException("Failed to update token in SQL secret manager", e);
    }
  }

  /**
   * Cancels a token by removing it from the SQL database. This will
   * call the corresponding method in {@link AbstractDelegationTokenSecretManager}
   * to perform validation and remove the token from the cache.
   * @return Identifier of the canceled token
   */
  @Override
  public synchronized TokenIdent cancelToken(Token<TokenIdent> token,
      String canceller) throws IOException {
    TokenIdent id = createTokenIdent(token.getIdentifier());

    // Calling getTokenInfo to load token into local cache if not present.
    // super.cancelToken() requires token to be present in local cache.
    getTokenInfo(id);

    return super.cancelToken(token, canceller);
  }

  /**
   * Obtain a list of tokens that will be considered for cleanup, based on the last
   * time the token was updated in SQL. This list may include tokens that are not
   * expired and should not be deleted (e.g. if the token was last renewed using a
   * higher renewal interval).
   * The number of results is limited to reduce performance impact. Some level of
   * contention is expected when multiple routers run cleanup simultaneously.
   * @return Map of tokens that have not been updated in SQL after the token renewal
   *         period.
   */
  @Override
  protected Map<TokenIdent, DelegationTokenInformation> getCandidateTokensForCleanup() {
    Map<TokenIdent, DelegationTokenInformation> tokens = new HashMap<>();
    try {
      // Query SQL for tokens that haven't been updated after
      // the last token renewal period.
      long maxModifiedTime = Time.now() - getTokenRenewInterval();
      Map<byte[], byte[]> tokenInfoBytesList = selectStaleTokenInfos(maxModifiedTime,
          this.maxTokenCleanupResults);

      LOG.info("Found {} tokens for cleanup", tokenInfoBytesList.size());
      for (Map.Entry<byte[], byte[]> tokenInfoBytes : tokenInfoBytesList.entrySet()) {
        TokenIdent tokenIdent = createTokenIdent(tokenInfoBytes.getKey());
        DelegationTokenInformation tokenInfo = createTokenInfo(tokenInfoBytes.getValue());
        tokens.put(tokenIdent, tokenInfo);
      }
    } catch (IOException | SQLException e) {
      LOG.error("Failed to get candidate tokens for cleanup in SQL secret manager", e);
    }

    return tokens;
  }

  /**
   * Removes the existing TokenInformation from the SQL database to
   * invalidate it.
   * @param ident TokenInformation to remove from the SQL database.
   */
  @Override
  protected void removeStoredToken(TokenIdent ident) throws IOException {
    try {
      deleteToken(ident.getSequenceNumber(), ident.getBytes());
    } catch (SQLException e) {
      LOG.warn("Failed to remove token in SQL secret manager", e);
    }
  }

  @Override
  protected void removeExpiredStoredToken(TokenIdent ident) {
    try {
      // Ensure that the token has not been renewed in SQL by
      // another secret manager
      DelegationTokenInformation tokenInfo = getTokenInfoFromSQL(ident);
      if (tokenInfo.getRenewDate() >= Time.now()) {
        LOG.info("Token was renewed by a different router and has not been deleted: {}", ident);
        return;
      }
      removeStoredToken(ident);
    } catch (NoSuchElementException e) {
      LOG.info("Token has already been deleted by a different router: {}", ident);
    } catch (Exception e) {
      LOG.warn("Could not remove token {}", ident, e);
    }
  }

  /**
   * Obtains the DelegationTokenInformation associated with the given
   * TokenIdentifier in the SQL database.
   * @param ident Existing TokenIdentifier in the SQL database.
   * @return DelegationTokenInformation that matches the given TokenIdentifier or
   *         null if it doesn't exist in the database.
   */
  @VisibleForTesting
  protected DelegationTokenInformation getTokenInfoFromSQL(TokenIdent ident) {
    try {
      byte[] tokenInfoBytes = selectTokenInfo(ident.getSequenceNumber(), ident.getBytes());
      if (tokenInfoBytes == null) {
        // Throw exception so value is not added to cache
        throw new NoSuchElementException("Token not found in SQL secret manager: " + ident);
      }
      return createTokenInfo(tokenInfoBytes);
    } catch (SQLException | IOException e) {
      LOG.error("Failed to get token in SQL secret manager", e);
      throw new RuntimeException(e);
    }
  }

  private TokenIdent createTokenIdent(byte[] tokenIdentBytes) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(tokenIdentBytes);
        DataInputStream din = new DataInputStream(bis)) {
      TokenIdent id = createIdentifier();
      id.readFields(din);
      return id;
    }
  }

  private DelegationTokenInformation createTokenInfo(byte[] tokenInfoBytes) throws IOException {
    DelegationTokenInformation tokenInfo = new DelegationTokenInformation();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(tokenInfoBytes)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        tokenInfo.readFields(dis);
      }
    }

    return tokenInfo;
  }

  /**
   * Obtains the value of the last reserved sequence number.
   * @return Last reserved sequence number.
   */
  @Override
  public int getDelegationTokenSeqNum() {
    try {
      return selectSequenceNum();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to get token sequence number in SQL secret manager", e);
    }
  }

  /**
   * Updates the value of the last reserved sequence number.
   * @param seqNum Value to update the sequence number to.
   */
  @Override
  public void setDelegationTokenSeqNum(int seqNum) {
    try {
      updateSequenceNum(seqNum);
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to update token sequence number in SQL secret manager", e);
    }
  }

  /**
   * Obtains the next available sequence number that can be allocated to a Token.
   * Sequence numbers need to be reserved using the shared sequenceNumberCounter once
   * the local batch has been exhausted, which handles sequenceNumber allocation
   * concurrently with other secret managers.
   * This method ensures that sequence numbers are incremental in a single secret manager,
   * but not across secret managers.
   * @return Next available sequence number.
   */
  @Override
  public synchronized int incrementDelegationTokenSeqNum() {
    if (currentSeqNum >= currentMaxSeqNum) {
      try {
        // Request a new batch of sequence numbers and use the
        // lowest one available.
        currentSeqNum = incrementSequenceNum(seqNumBatchSize);
        currentMaxSeqNum = currentSeqNum + seqNumBatchSize;
      } catch (SQLException e) {
        throw new RuntimeException(
            "Failed to increment token sequence number in SQL secret manager", e);
      }
    }

    return ++currentSeqNum;
  }

  /**
   * Persists a DelegationKey into the SQL database. The delegation keyId
   * is expected to be unique and any duplicate key attempts will result
   * in an IOException.
   * @param key DelegationKey to persist into the SQL database.
   */
  @Override
  protected void storeDelegationKey(DelegationKey key) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      key.write(dos);
      // Add delegation key to SQL database
      insertDelegationKey(key.getKeyId(), bos.toByteArray());
      // Add delegation key to local cache
      super.storeDelegationKey(key);
    } catch (SQLException e) {
      throw new IOException("Failed to store delegation key in SQL secret manager", e);
    }
  }

  /**
   * Updates an existing DelegationKey in the SQL database.
   * @param key Updated DelegationKey.
   */
  @Override
  protected void updateDelegationKey(DelegationKey key) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      key.write(dos);
      // Update delegation key in SQL database
      updateDelegationKey(key.getKeyId(), bos.toByteArray());
      // Update delegation key in local cache
      super.updateDelegationKey(key);
    } catch (SQLException e) {
      throw new IOException("Failed to update delegation key in SQL secret manager", e);
    }
  }

  /**
   * Removes the existing DelegationKey from the SQL database to
   * invalidate it.
   * @param key DelegationKey to remove from the SQL database.
   */
  @Override
  protected void removeStoredMasterKey(DelegationKey key) {
    try {
      deleteDelegationKey(key.getKeyId());
    } catch (SQLException e) {
      LOG.warn("Failed to remove delegation key in SQL secret manager", e);
    }
  }

  /**
   * Obtains the DelegationKey from the SQL database.
   * @param keyId KeyId of the DelegationKey to obtain.
   * @return DelegationKey that matches the given keyId or null
   *         if it doesn't exist in the database.
   */
  @Override
  protected DelegationKey getDelegationKey(int keyId) {
    // Look for delegation key in local cache
    DelegationKey delegationKey = super.getDelegationKey(keyId);

    if (delegationKey == null) {
      try {
        // Look for delegation key in SQL database
        byte[] delegationKeyBytes = selectDelegationKey(keyId);

        if (delegationKeyBytes != null) {
          delegationKey = new DelegationKey();
          try (ByteArrayInputStream bis = new ByteArrayInputStream(delegationKeyBytes)) {
            try (DataInputStream dis = new DataInputStream(bis)) {
              delegationKey.readFields(dis);
            }
          }

          // Update delegation key in local cache
          allKeys.put(keyId, delegationKey);
        }
      } catch (IOException | SQLException e) {
        LOG.error("Failed to get delegation key in SQL secret manager", e);
      }
    }

    return delegationKey;
  }

  /**
   * Obtains the value of the last delegation key id.
   * @return Last delegation key id.
   */
  @Override
  public int getCurrentKeyId() {
    try {
      return selectKeyId();
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to get delegation key id in SQL secret manager", e);
    }
  }

  /**
   * Updates the value of the last delegation key id.
   * @param keyId Value to update the delegation key id to.
   */
  @Override
  public void setCurrentKeyId(int keyId) {
    try {
      updateKeyId(keyId);
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to set delegation key id in SQL secret manager", e);
    }
  }

  /**
   * Obtains the next available delegation key id that can be allocated to a DelegationKey.
   * Delegation key id need to be reserved using the shared delegationKeyIdCounter,
   * which handles keyId allocation concurrently with other secret managers.
   * @return Next available delegation key id.
   */
  @Override
  public int incrementCurrentKeyId() {
    try {
      return incrementKeyId(1) + 1;
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to increment delegation key id in SQL secret manager", e);
    }
  }

  // Token operations in SQL database
  protected abstract byte[] selectTokenInfo(int sequenceNum, byte[] tokenIdentifier)
      throws SQLException;
  protected abstract Map<byte[], byte[]> selectStaleTokenInfos(long maxModifiedTime,
      int maxResults) throws SQLException;
  protected abstract void insertToken(int sequenceNum, byte[] tokenIdentifier, byte[] tokenInfo)
      throws SQLException;
  protected abstract void updateToken(int sequenceNum, byte[] tokenIdentifier, byte[] tokenInfo)
      throws SQLException;
  protected abstract void deleteToken(int sequenceNum, byte[] tokenIdentifier)
      throws SQLException;
  // Delegation key operations in SQL database
  protected abstract byte[] selectDelegationKey(int keyId) throws SQLException;
  protected abstract void insertDelegationKey(int keyId, byte[] delegationKey)
      throws SQLException;
  protected abstract void updateDelegationKey(int keyId, byte[] delegationKey)
      throws SQLException;
  protected abstract void deleteDelegationKey(int keyId) throws SQLException;
  // Counter operations in SQL database
  protected abstract int selectSequenceNum() throws SQLException;
  protected abstract void updateSequenceNum(int value) throws SQLException;
  protected abstract int incrementSequenceNum(int amount) throws SQLException;
  protected abstract int selectKeyId() throws SQLException;
  protected abstract void updateKeyId(int value) throws SQLException;
  protected abstract int incrementKeyId(int amount) throws SQLException;
}
