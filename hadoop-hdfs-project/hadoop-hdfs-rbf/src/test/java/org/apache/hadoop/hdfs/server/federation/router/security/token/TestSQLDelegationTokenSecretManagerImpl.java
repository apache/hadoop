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

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.SQLDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestSQLDelegationTokenSecretManagerImpl {
  private static final String CONNECTION_URL = "jdbc:derby:memory:TokenStore";
  private static final int TEST_MAX_RETRIES = 3;
  private static final int TOKEN_EXPIRATION_SECONDS = 1;
  private static final int TOKEN_EXPIRATION_SCAN_SECONDS = 1;
  private static Configuration conf;

  @Before
  public void init() throws SQLException {
    createTestDBTables();
  }

  @After
  public void cleanup() throws SQLException {
    dropTestDBTables();
  }

  @BeforeClass
  public static void initDatabase() throws SQLException {
    DriverManager.getConnection(CONNECTION_URL + ";create=true");

    conf = new Configuration();
    conf.set(SQLConnectionFactory.CONNECTION_URL, CONNECTION_URL);
    conf.set(SQLConnectionFactory.CONNECTION_USERNAME, "testuser");
    conf.set(SQLConnectionFactory.CONNECTION_PASSWORD, "testpassword");
    conf.set(SQLConnectionFactory.CONNECTION_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
    conf.setInt(SQLSecretManagerRetriableHandlerImpl.MAX_RETRIES, TEST_MAX_RETRIES);
    conf.setInt(SQLSecretManagerRetriableHandlerImpl.RETRY_SLEEP_TIME_MS, 10);
    conf.setInt(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, TOKEN_EXPIRATION_SCAN_SECONDS);
  }

  @AfterClass
  public static void cleanupDatabase() {
    try {
      DriverManager.getConnection(CONNECTION_URL + ";drop=true");
    } catch (SQLException e) {
      // SQLException expected when database is dropped
      if (!e.getMessage().contains("dropped")) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testSingleSecretManager() throws Exception {
    DelegationTokenManager tokenManager = createTokenManager();
    try {
      Token<? extends AbstractDelegationTokenIdentifier> token =
          tokenManager.createToken(UserGroupInformation.getCurrentUser(), "foo");
      validateToken(tokenManager, token);
    } finally {
      stopTokenManager(tokenManager);
    }
  }

  @Test
  public void testMultipleSecretManagers() throws Exception {
    DelegationTokenManager tokenManager1 = createTokenManager();
    DelegationTokenManager tokenManager2 = createTokenManager();

    try {
      Token<? extends AbstractDelegationTokenIdentifier> token1 =
          tokenManager1.createToken(UserGroupInformation.getCurrentUser(), "foo");
      Token<? extends AbstractDelegationTokenIdentifier> token2 =
          tokenManager2.createToken(UserGroupInformation.getCurrentUser(), "foo");

      validateToken(tokenManager1, token2);
      validateToken(tokenManager2, token1);
    } finally {
      stopTokenManager(tokenManager1);
      stopTokenManager(tokenManager2);
    }
  }

  @Test
  public void testCancelToken() throws Exception {
    DelegationTokenManager tokenManager1 = createTokenManager(getShortLivedTokenConf());
    DelegationTokenManager tokenManager2 = createTokenManager(getShortLivedTokenConf());

    TestDelegationTokenSecretManager secretManager2 =
        (TestDelegationTokenSecretManager) tokenManager2.getDelegationTokenSecretManager();

    try {
      // Create token on token manager 1
      Token token1 = tokenManager1.createToken(UserGroupInformation.getCurrentUser(), "foo");

      // Load token on token manager 2 to test it doesn't get stale
      tokenManager2.verifyToken(token1);

      // Cancel token on token manager 1
      tokenManager1.cancelToken(token1, "foo");

      // Validate that token cancellation is propagated to token manager 2
      secretManager2.waitForTokenEviction(token1.decodeIdentifier());
      LambdaTestUtils.intercept(SecretManager.InvalidToken.class,
          () -> tokenManager2.verifyToken(token1));
    } finally {
      stopTokenManager(tokenManager1);
      stopTokenManager(tokenManager2);
    }
  }

  @Test
  public void testRenewToken() throws Exception {
    DelegationTokenManager tokenManager1 = createTokenManager(getShortLivedTokenConf());
    DelegationTokenManager tokenManager2 = createTokenManager(getShortLivedTokenConf());

    TestDelegationTokenSecretManager secretManager2 =
        (TestDelegationTokenSecretManager) tokenManager2.getDelegationTokenSecretManager();

    try {
      // Create token on token manager 1
      Token token1 = tokenManager1.createToken(UserGroupInformation.getCurrentUser(), "foo");
      long expirationTime = Time.monotonicNow() +
          TimeUnit.SECONDS.toMillis(TOKEN_EXPIRATION_SECONDS) * 2;

      // Load token on token manager 2 to test it doesn't get stale
      tokenManager2.verifyToken(token1);

      // Renew token on token manager 1 and verify token is updated on token manager 2
      // Do this for long enough that the token should be expired if not renewed
      AbstractDelegationTokenIdentifier token1Id =
          (AbstractDelegationTokenIdentifier) token1.decodeIdentifier();
      while (Time.monotonicNow() < expirationTime) {
        tokenManager1.renewToken(token1, "foo");
        callRemoveExpiredTokensAndValidateSQL(secretManager2, token1Id, true);
        secretManager2.waitForTokenEviction(token1Id);
        tokenManager2.verifyToken(token1);
      }

      // Stop renewing token and validate it's no longer valid and removed
      // from SQL
      Thread.sleep(TimeUnit.SECONDS.toMillis(TOKEN_EXPIRATION_SECONDS) * 2);
      LambdaTestUtils.intercept(SecretManager.InvalidToken.class,
          () -> tokenManager2.verifyToken(token1));
      callRemoveExpiredTokensAndValidateSQL(secretManager2, token1Id, false);
    } finally {
      stopTokenManager(tokenManager1);
      stopTokenManager(tokenManager2);
    }
  }

  @Test
  public void testRemoveExpiredTokens() throws Exception {
    DelegationTokenManager tokenManager = createTokenManager(getShortLivedTokenConf());

    try {
      TestDelegationTokenSecretManager secretManager =
          (TestDelegationTokenSecretManager) tokenManager.getDelegationTokenSecretManager();

      // Create token to be constantly renewed.
      Token token1 = tokenManager.createToken(UserGroupInformation.getCurrentUser(), "foo");
      AbstractDelegationTokenIdentifier tokenId1 =
          (AbstractDelegationTokenIdentifier) token1.decodeIdentifier();

      // Create token expected to expire soon.
      long expirationTime2 = Time.now();
      AbstractDelegationTokenIdentifier tokenId2 = storeToken(secretManager, 2, expirationTime2);

      // Create token not expected to expire soon.
      long expirationTime3 = Time.now() + TimeUnit.SECONDS.toMillis(TOKEN_EXPIRATION_SECONDS) * 10;
      AbstractDelegationTokenIdentifier tokenId3 = storeToken(secretManager, 3, expirationTime3);

      GenericTestUtils.waitFor(() -> {
        try {
          // Constantly renew token so it doesn't expire.
          tokenManager.renewToken(token1, "foo");

          // Wait for cleanup to happen so expired token is deleted from SQL.
          return !isTokenInSQL(secretManager, tokenId2);
        } catch (IOException | SQLException e) {
          throw new RuntimeException(e);
        }
      }, 100, 6000);

      Assert.assertTrue("Renewed token must not be cleaned up",
          isTokenInSQL(secretManager, tokenId1));
      Assert.assertTrue("Token with future expiration must not be cleaned up",
          isTokenInSQL(secretManager, tokenId3));
    } finally {
      stopTokenManager(tokenManager);
    }
  }

  private AbstractDelegationTokenIdentifier storeToken(
      TestDelegationTokenSecretManager secretManager, int sequenceNum, long expirationTime)
      throws IOException {
    AbstractDelegationTokenIdentifier tokenId = new DelegationTokenIdentifier(new Text("Test"));
    tokenId.setOwner(new Text("foo"));
    tokenId.setSequenceNumber(sequenceNum);

    AbstractDelegationTokenSecretManager.DelegationTokenInformation tokenInfo =
        new AbstractDelegationTokenSecretManager.DelegationTokenInformation(expirationTime, null);
    secretManager.storeToken(tokenId, tokenInfo);

    return tokenId;
  }

  private Configuration getShortLivedTokenConf() {
    Configuration shortLivedConf = new Configuration(conf);
    shortLivedConf.setTimeDuration(
        SQLDelegationTokenSecretManager.SQL_DTSM_TOKEN_LOADING_CACHE_EXPIRATION,
        200, TimeUnit.MILLISECONDS);
    shortLivedConf.setInt(DelegationTokenManager.RENEW_INTERVAL, TOKEN_EXPIRATION_SECONDS);
    return shortLivedConf;
  }

  private void callRemoveExpiredTokensAndValidateSQL(
      TestDelegationTokenSecretManager secretManager, AbstractDelegationTokenIdentifier tokenId,
      boolean expectedInSQL) throws SQLException {
    secretManager.removeExpiredStoredToken(tokenId);
    Assert.assertEquals(expectedInSQL, isTokenInSQL(secretManager, tokenId));
  }

  private boolean isTokenInSQL(TestDelegationTokenSecretManager secretManager,
      AbstractDelegationTokenIdentifier tokenId) throws SQLException {
    return secretManager.selectTokenInfo(tokenId.getSequenceNumber(), tokenId.getBytes()) != null;
  }

  @Test
  public void testSequenceNumAllocation() throws Exception {
    int tokensPerManager = SQLDelegationTokenSecretManagerImpl.DEFAULT_SEQ_NUM_BATCH_SIZE * 5;
    Set<Integer> sequenceNums1 = new HashSet<>();
    Set<Integer> sequenceNums2 = new HashSet<>();
    Set<Integer> sequenceNums3 = new HashSet<>();
    Set<Integer> sequenceNums = new HashSet<>();
    DelegationTokenManager tokenManager1 = createTokenManager();
    DelegationTokenManager tokenManager2 = createTokenManager();
    DelegationTokenManager tokenManager3 = createTokenManager();

    try {
      for (int i = 0; i < tokensPerManager; i++) {
        allocateSequenceNum(tokenManager1, sequenceNums1);
        allocateSequenceNum(tokenManager2, sequenceNums2);
        allocateSequenceNum(tokenManager3, sequenceNums3);
        sequenceNums.addAll(sequenceNums1);
        sequenceNums.addAll(sequenceNums2);
        sequenceNums.addAll(sequenceNums3);
      }

      Assert.assertEquals("Verify that all tokens were created with unique sequence numbers",
          tokensPerManager * 3, sequenceNums.size());
      Assert.assertEquals("Verify that tokenManager1 generated unique sequence numbers",
          tokensPerManager, sequenceNums1.size());
      Assert.assertEquals("Verify that tokenManager2 generated unique sequence number",
          tokensPerManager, sequenceNums2.size());
      Assert.assertEquals("Verify that tokenManager3 generated unique sequence numbers",
          tokensPerManager, sequenceNums3.size());

      // Validate sequence number batches allocated in order to each token manager
      int batchSize = SQLDelegationTokenSecretManagerImpl.DEFAULT_SEQ_NUM_BATCH_SIZE;
      for (int seqNum = 1; seqNum < tokensPerManager;) {
        // First batch allocated tokenManager1
        for (int i = 0; i < batchSize; i++, seqNum++) {
          Assert.assertTrue(sequenceNums1.contains(seqNum));
        }
        // Second batch allocated tokenManager2
        for (int i = 0; i < batchSize; i++, seqNum++) {
          Assert.assertTrue(sequenceNums2.contains(seqNum));
        }
        // Third batch allocated tokenManager3
        for (int i = 0; i < batchSize; i++, seqNum++) {
          Assert.assertTrue(sequenceNums3.contains(seqNum));
        }
      }

      SQLDelegationTokenSecretManagerImpl secretManager =
          (SQLDelegationTokenSecretManagerImpl) tokenManager1.getDelegationTokenSecretManager();
      Assert.assertEquals("Verify that the counter is set to the highest sequence number",
          tokensPerManager * 3, secretManager.getDelegationTokenSeqNum());
    } finally {
      stopTokenManager(tokenManager1);
      stopTokenManager(tokenManager2);
      stopTokenManager(tokenManager3);
    }
  }

  @Test
  public void testSequenceNumRollover() throws Exception {
    int tokenBatch = SQLDelegationTokenSecretManagerImpl.DEFAULT_SEQ_NUM_BATCH_SIZE;
    Set<Integer> sequenceNums = new HashSet<>();

    DelegationTokenManager tokenManager = createTokenManager();

    try {
      SQLDelegationTokenSecretManagerImpl secretManager =
          (SQLDelegationTokenSecretManagerImpl) tokenManager.getDelegationTokenSecretManager();
      secretManager.setDelegationTokenSeqNum(Integer.MAX_VALUE - tokenBatch);

      // Allocate sequence numbers before they are rolled over
      for (int seqNum = Integer.MAX_VALUE - tokenBatch; seqNum < Integer.MAX_VALUE; seqNum++) {
        allocateSequenceNum(tokenManager, sequenceNums);
        Assert.assertTrue(sequenceNums.contains(seqNum + 1));
      }

      // Allocate sequence numbers after they are rolled over
      for (int seqNum = 0; seqNum < tokenBatch; seqNum++) {
        allocateSequenceNum(tokenManager, sequenceNums);
        Assert.assertTrue(sequenceNums.contains(seqNum + 1));
      }
    } finally {
      stopTokenManager(tokenManager);
    }
  }

  @Test
  public void testDelegationKeyAllocation() throws Exception {
    DelegationTokenManager tokenManager1 = createTokenManager();

    try {
      SQLDelegationTokenSecretManagerImpl secretManager1 =
          (SQLDelegationTokenSecretManagerImpl) tokenManager1.getDelegationTokenSecretManager();
      // Prevent delegation keys to roll for the rest of the test to avoid race conditions
      // between the keys generated and the active keys in the database.
      ((TestDelegationTokenSecretManager) secretManager1).lockKeyRoll();
      int keyId1 = secretManager1.getCurrentKeyId();

      // Validate that latest key1 is assigned to tokenManager1 tokens
      Token<? extends AbstractDelegationTokenIdentifier> token1 =
          tokenManager1.createToken(UserGroupInformation.getCurrentUser(), "foo");
      validateKeyId(token1, keyId1);

      DelegationTokenManager tokenManager2 = createTokenManager();
      try {
        SQLDelegationTokenSecretManagerImpl secretManager2 =
            (SQLDelegationTokenSecretManagerImpl) tokenManager2.getDelegationTokenSecretManager();
        // Prevent delegation keys to roll for the rest of the test
        ((TestDelegationTokenSecretManager) secretManager2).lockKeyRoll();
        int keyId2 = secretManager2.getCurrentKeyId();

        Assert.assertNotEquals("Each secret manager has its own key", keyId1, keyId2);

        // Validate that latest key2 is assigned to tokenManager2 tokens
        Token<? extends AbstractDelegationTokenIdentifier> token2 =
            tokenManager2.createToken(UserGroupInformation.getCurrentUser(), "foo");
        validateKeyId(token2, keyId2);

        // Validate that key1 is still assigned to tokenManager1 tokens
        token1 = tokenManager1.createToken(UserGroupInformation.getCurrentUser(), "foo");
        validateKeyId(token1, keyId1);

        // Validate that key2 is still assigned to tokenManager2 tokens
        token2 = tokenManager2.createToken(UserGroupInformation.getCurrentUser(), "foo");
        validateKeyId(token2, keyId2);
      } finally {
        stopTokenManager(tokenManager2);
      }
    } finally {
      stopTokenManager(tokenManager1);
    }
  }

  @Test
  public void testHikariConfigs() {
    HikariDataSourceConnectionFactory factory1 = new HikariDataSourceConnectionFactory(conf);
    int defaultMaximumPoolSize = factory1.getDataSource().getMaximumPoolSize();
    factory1.shutdown();

    // Changing default maximumPoolSize
    Configuration hikariConf = new Configuration(conf);
    hikariConf.setInt(HikariDataSourceConnectionFactory.HIKARI_PROPS + "maximumPoolSize",
        defaultMaximumPoolSize + 1);

    // Verifying property is correctly set in datasource
    HikariDataSourceConnectionFactory factory2 = new HikariDataSourceConnectionFactory(hikariConf);
    Assert.assertEquals(factory2.getDataSource().getMaximumPoolSize(),
        defaultMaximumPoolSize + 1);
    factory2.shutdown();
  }

  @Test
  public void testRetries() throws Exception {
    DelegationTokenManager tokenManager = createTokenManager();
    TestDelegationTokenSecretManager secretManager =
        (TestDelegationTokenSecretManager) tokenManager.getDelegationTokenSecretManager();

    try {
      // Prevent delegation keys to roll for the rest of the test
      secretManager.lockKeyRoll();

      // Reset counter and expect a single request when inserting a token
      TestRetryHandler.resetExecutionAttemptCounter();
      tokenManager.createToken(UserGroupInformation.getCurrentUser(), "foo");
      Assert.assertEquals(1, TestRetryHandler.getExecutionAttempts());

      // Breaking database connections to cause retries
      secretManager.setReadOnly(true);

      // Reset counter and expect a multiple retries when failing to insert a token
      TestRetryHandler.resetExecutionAttemptCounter();
      tokenManager.createToken(UserGroupInformation.getCurrentUser(), "foo");
      Assert.assertEquals(TEST_MAX_RETRIES + 1, TestRetryHandler.getExecutionAttempts());
    } finally {
      // Fix database connections
      secretManager.setReadOnly(false);
      stopTokenManager(tokenManager);
    }
  }

  private DelegationTokenManager createTokenManager() {
    return createTokenManager(conf);
  }

  private DelegationTokenManager createTokenManager(Configuration config) {
    DelegationTokenManager tokenManager = new DelegationTokenManager(new Configuration(), null);
    tokenManager.setExternalDelegationTokenSecretManager(
        new TestDelegationTokenSecretManager(config));
    return tokenManager;
  }

  private void allocateSequenceNum(DelegationTokenManager tokenManager, Set<Integer> sequenceNums)
      throws IOException {
    Token<? extends AbstractDelegationTokenIdentifier> token =
        tokenManager.createToken(UserGroupInformation.getCurrentUser(), "foo");
    AbstractDelegationTokenIdentifier tokenIdentifier = token.decodeIdentifier();
    Assert.assertFalse("Verify sequence number is unique",
        sequenceNums.contains(tokenIdentifier.getSequenceNumber()));

    sequenceNums.add(tokenIdentifier.getSequenceNumber());
  }

  private void validateToken(DelegationTokenManager tokenManager,
      Token<? extends AbstractDelegationTokenIdentifier> token)
      throws Exception {
    SQLDelegationTokenSecretManagerImpl secretManager =
        (SQLDelegationTokenSecretManagerImpl) tokenManager.getDelegationTokenSecretManager();
    AbstractDelegationTokenIdentifier tokenIdentifier = token.decodeIdentifier();

    // Verify token using token manager
    tokenManager.verifyToken(token);

    byte[] tokenInfo1 = secretManager.selectTokenInfo(tokenIdentifier.getSequenceNumber(),
        tokenIdentifier.getBytes());
    Assert.assertNotNull("Verify token exists in database", tokenInfo1);

    // Renew token using token manager
    tokenManager.renewToken(token, "foo");

    byte[] tokenInfo2 = secretManager.selectTokenInfo(tokenIdentifier.getSequenceNumber(),
        tokenIdentifier.getBytes());
    Assert.assertNotNull("Verify token exists in database", tokenInfo2);
    Assert.assertFalse("Verify token has been updated in database",
        Arrays.equals(tokenInfo1, tokenInfo2));

    // Cancel token using token manager
    tokenManager.cancelToken(token, "foo");
    byte[] tokenInfo3 = secretManager.selectTokenInfo(tokenIdentifier.getSequenceNumber(),
        tokenIdentifier.getBytes());
    Assert.assertNull("Verify token was removed from database", tokenInfo3);
  }

  private void validateKeyId(Token<? extends AbstractDelegationTokenIdentifier> token,
      int expectedKeyiD) throws IOException {
    AbstractDelegationTokenIdentifier tokenIdentifier = token.decodeIdentifier();
    Assert.assertEquals("Verify that keyId is assigned to token",
        tokenIdentifier.getMasterKeyId(), expectedKeyiD);
  }

  private static Connection getTestDBConnection() {
    try {
      return DriverManager.getConnection(CONNECTION_URL);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void createTestDBTables() throws SQLException {
    execute("CREATE TABLE Tokens (sequenceNum INT NOT NULL, "
        + "tokenIdentifier VARCHAR (255) FOR BIT DATA NOT NULL, "
        + "tokenInfo VARCHAR (255) FOR BIT DATA NOT NULL, "
        + "modifiedTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        + "PRIMARY KEY(sequenceNum))");
    execute("CREATE TABLE DelegationKeys (keyId INT NOT NULL, "
        + "delegationKey VARCHAR (255) FOR BIT DATA NOT NULL, "
        + "modifiedTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
        + "PRIMARY KEY(keyId))");
    execute("CREATE TABLE LastSequenceNum (sequenceNum INT NOT NULL)");
    execute("INSERT INTO LastSequenceNum VALUES (0)");
    execute("CREATE TABLE LastDelegationKeyId (keyId INT NOT NULL)");
    execute("INSERT INTO LastDelegationKeyId VALUES (0)");
  }

  private static void dropTestDBTables() throws SQLException {
    execute("DROP TABLE Tokens");
    execute("DROP TABLE DelegationKeys");
    execute("DROP TABLE LastSequenceNum");
    execute("DROP TABLE LastDelegationKeyId");
  }

  private static void execute(String statement) throws SQLException {
    try (Connection connection = getTestDBConnection()) {
      connection.createStatement().execute(statement);
    }
  }

  private void stopTokenManager(DelegationTokenManager tokenManager) {
    TestDelegationTokenSecretManager secretManager =
        (TestDelegationTokenSecretManager) tokenManager.getDelegationTokenSecretManager();
    // Release any locks on tables
    secretManager.unlockKeyRoll();
    // Stop threads to close database connections
    secretManager.stopThreads();
  }

  static class TestDelegationTokenSecretManager extends SQLDelegationTokenSecretManagerImpl {
    private ReentrantLock keyRollLock;

    private synchronized ReentrantLock getKeyRollLock() {
      if (keyRollLock == null) {
        keyRollLock = new ReentrantLock();
      }
      return keyRollLock;
    }

    TestDelegationTokenSecretManager(Configuration conf) {
      super(conf, new TestConnectionFactory(conf),
          SQLSecretManagerRetriableHandlerImpl.getInstance(conf, new TestRetryHandler()));
    }

    // Tests can call this method to prevent delegation keys from
    // being rolled in the middle of a test to prevent race conditions
    public void lockKeyRoll() {
      getKeyRollLock().lock();
    }

    public void unlockKeyRoll() {
      if (getKeyRollLock().isHeldByCurrentThread()) {
        getKeyRollLock().unlock();
      }
    }

    @Override
    protected void rollMasterKey() throws IOException {
      try {
        lockKeyRoll();
        super.rollMasterKey();
      } finally {
        unlockKeyRoll();
      }
    }

    public void waitForTokenEviction(TokenIdentifier tokenId)
        throws InterruptedException, TimeoutException {
      // Wait until token is not found on cache
      GenericTestUtils.waitFor(() -> !this.currentTokens.containsKey(tokenId), 100, 5000);
    }

    public void removeExpiredStoredToken(TokenIdentifier tokenId) {
      super.removeExpiredStoredToken((AbstractDelegationTokenIdentifier) tokenId);
    }

    public void storeToken(AbstractDelegationTokenIdentifier ident,
        DelegationTokenInformation tokenInfo) throws IOException {
      super.storeToken(ident, tokenInfo);
    }

    public void setReadOnly(boolean readOnly) {
      ((TestConnectionFactory) getConnectionFactory()).readOnly = readOnly;
    }
  }

  static class TestConnectionFactory extends HikariDataSourceConnectionFactory {
    private boolean readOnly = false;
    TestConnectionFactory(Configuration conf) {
      super(conf);
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection connection = super.getConnection();
      // Change to default schema as derby driver looks for user schema
      connection.setSchema("APP");
      connection.setReadOnly(readOnly);
      return connection;
    }
  }

  static class TestRetryHandler extends SQLSecretManagerRetriableHandlerImpl {
    // Tracks the amount of times that a SQL command is executed, regardless of
    // whether it completed successfully or not.
    private static AtomicInteger executionAttemptCounter = new AtomicInteger();

    static void resetExecutionAttemptCounter() {
      executionAttemptCounter = new AtomicInteger();
    }

    static int getExecutionAttempts() {
      return executionAttemptCounter.get();
    }

    @Override
    public void execute(SQLCommandVoid command) throws SQLException {
      executionAttemptCounter.incrementAndGet();
      super.execute(command);
    }
  }
}
