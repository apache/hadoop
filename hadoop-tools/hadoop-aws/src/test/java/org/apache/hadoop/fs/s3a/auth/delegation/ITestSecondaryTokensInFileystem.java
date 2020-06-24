/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtUtilShell;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SESSION_TOKEN;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_SECONDARY_BINDINGS;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ENDPOINT;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.assertSecurityEnabled;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.extractTokenIdentifier;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupS3ADelegationToken;
import static org.apache.hadoop.test.LambdaTestUtils.doAs;

/**
 * Tests use of Hadoop delegation tokens within the FS itself.
 * <p></p>
 * This instantiates a MiniKDC as some of the operations tested require
 * UGI to be initialized with security enabled.
 * <p></p>
 * Derived from {@link ITestSessionDelegationInFileystem}; switches
 * to the FullDT tokens as primary, and the Injecting as secondary.
 */
@SuppressWarnings("StaticNonFinalField")
public class ITestSecondaryTokensInFileystem extends AbstractDelegationIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSecondaryTokensInFileystem.class);

  private static MiniKerberizedHadoopCluster cluster;

  private UserGroupInformation bobUser;

  private UserGroupInformation aliceUser;

  private S3ADelegationTokens delegationTokens;

  /***
   * Set up a mini Cluster with two users in the keytab.
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    cluster = new MiniKerberizedHadoopCluster();
    cluster.init(new Configuration());
    cluster.start();
  }

  /**
   * Tear down the Cluster.
   */
  @SuppressWarnings("ThrowableNotThrown")
  @AfterClass
  public static void teardownCluster() throws Exception {
    ServiceOperations.stopQuietly(LOG, cluster);
  }

  protected static MiniKerberizedHadoopCluster getCluster() {
    return cluster;
  }

  /**
   * Get the delegation token binding for this test suite.
   * @return which DT binding to use.
   */
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING;
  }

  /**
   * Get the kind of the tokens which are generated.
   * @return the kind of DT
   */
  public Text getTokenKind() {
    return FULL_TOKEN_KIND;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING,
        DELEGATION_SECONDARY_BINDINGS,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY);
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    enableDelegationTokens(conf, getDelegationBinding());
    // add a secondary token
    InjectingTokenBinding.addAsSecondaryToken(conf, true,
        DummyCredentialsProvider.NAME);

    conf.set(AWS_CREDENTIALS_PROVIDER, " ");
    // switch to SSE_S3.
    if (conf.getBoolean(KEY_ENCRYPTION_TESTS, true)) {
      conf.set(SERVER_SIDE_ENCRYPTION_ALGORITHM,
          S3AEncryptionMethods.SSE_S3.getMethod());
    }
    // set the YARN RM up for YARN tests.
    conf.set(YarnConfiguration.RM_PRINCIPAL, YARN_RM);
    return conf;
  }


  @Override
  public void setup() throws Exception {
    // clear any existing tokens from the FS
    resetUGI();
    UserGroupInformation.setConfiguration(createConfiguration());

    aliceUser = cluster.createAliceUser();
    bobUser = cluster.createBobUser();

    UserGroupInformation.setLoginUser(aliceUser);
    assertSecurityEnabled();
    // only now do the setup, so that any FS created is secure
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // make sure there aren't any tokens
    assertNull("Unexpectedly found an S3A token",
        lookupS3ADelegationToken(
            UserGroupInformation.getCurrentUser().getCredentials(),
            fs.getUri()));

    // DTs are inited but not started.
    delegationTokens = instantiateDTSupport(getConfiguration());
  }

  @SuppressWarnings("ThrowableNotThrown")
  @Override
  public void teardown() throws Exception {
    super.teardown();
    ServiceOperations.stopQuietly(LOG, delegationTokens);
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    MiniKerberizedHadoopCluster.closeUserFileSystems(aliceUser);
    MiniKerberizedHadoopCluster.closeUserFileSystems(bobUser);
    cluster.resetUGI();
  }

  /**
   * Are encryption tests enabled?
   * @return true if encryption is turned on.
   */
  protected boolean encryptionTestEnabled() {
    return getConfiguration().getBoolean(KEY_ENCRYPTION_TESTS, true);
  }

  @Test
  public void testGetDTfromFileSystem() throws Throwable {
    describe("Enable delegation tokens and request one");
    delegationTokens.start();
    S3AFileSystem fs = getFileSystem();
    assertNotNull("No tokens from " + fs,
        fs.getCanonicalServiceName());
    S3ATestUtils.MetricDiff invocationDiff = new S3ATestUtils.MetricDiff(fs,
        Statistic.INVOCATION_GET_DELEGATION_TOKEN);
    S3ATestUtils.MetricDiff issueDiff = new S3ATestUtils.MetricDiff(fs,
        Statistic.DELEGATION_TOKENS_ISSUED);
    Token<AbstractS3ATokenIdentifier> token =
        requireNonNull(fs.getDelegationToken(""),
            "no token from filesystem " + fs);
    assertEquals("token kind", getTokenKind(), token.getKind());
    assertTokenCreationCount(fs, 1);
    final String fsInfo = fs.toString();
    invocationDiff.assertDiffEquals("getDelegationToken() in " + fsInfo,
        1);
    issueDiff.assertDiffEquals("DTs issued in " + delegationTokens,
        1);

    Text service = delegationTokens.getService();
    assertEquals("service name", service, token.getService());
    Credentials creds = new Credentials();
    creds.addToken(service, token);
    assertEquals("retrieve token from " + creds,
        token, creds.getToken(service));
  }

  @Test
  public void testAddTokensFromFileSystem() throws Throwable {
    describe("verify FileSystem.addDelegationTokens() collects all tokens");
    S3AFileSystem fs = getFileSystem();
    Credentials creds = new Credentials();
    final long initialIssueCount = InjectingTokenBinding.getIssueCount();
    Token<?>[] tokens = fs.addDelegationTokens(YARN_RM, creds);
    Assertions.assertThat(tokens)
        .describedAs("Tokens from fileystem")
        .hasSize(2);
    final long secondIssueCount = InjectingTokenBinding.getIssueCount();
    Assertions.assertThat(secondIssueCount - initialIssueCount)
        .describedAs("InjectingTokenBinding issue count")
        .isEqualTo(1);

    // the first token in the list of issued tokens is the
    // primary.
    Token<?> token = requireNonNull(tokens[0], "token");
    LOG.info("FS token is {}", token);
    Text service = delegationTokens.getService();

    Token<? extends TokenIdentifier> retrieved = requireNonNull(
        creds.getToken(service),
        "retrieved token with key " + service + "; expected " + token);

    // now the secondary token
    List<SecondaryDelegationToken> secondaryTokens
        = delegationTokens.listSecondaryTokens();
    Assertions.assertThat(secondaryTokens)
        .describedAs("secondary token list")
        .hasSize(1);
    SecondaryDelegationToken binding2 = secondaryTokens.get(0);
    Text b2service = binding2.getService();
    Token<AbstractS3ATokenIdentifier> b2token = binding2
        .bindToToken(creds);
    InjectingTokenIdentifier b2tokenId = (InjectingTokenIdentifier)
        extractTokenIdentifier(b2service.toString(), b2token);
    Assertions.assertThat(b2tokenId)
        .describedAs("secondary token %s", b2tokenId)
        .extracting(InjectingTokenIdentifier::getIssueNumber)
        .isEqualTo(secondIssueCount);
  }

  /**
   * The this verifies that when an S3A token service is brought up with
   * secondary tokens, then they are picked up and the token specific
   * binding service instantiated.
   * This is how we verify that all is good.
   */
  @Test
  public void testDTCredentialProviderFromCurrentUserCreds() throws Throwable {
    describe("Add credentials to the current user, "
        + "then verify that they can be found when S3ADelegationTokens binds");
    Credentials cred = createDelegationTokens();
    assertAllTokensIssued(cred);
    UserGroupInformation.getCurrentUser().addCredentials(cred);
    delegationTokens.start();
    assertTrue("bind to existing DT failed",
        delegationTokens.isBoundToDT());
    SecondaryDelegationToken binding2
        = delegationTokens.listSecondaryTokens().get(0);
    Assertions.assertThat(binding2.getBoundDT())
        .describedAs("secondary token")
        .isNotNull();
    InjectingTokenIdentifier b2tokenId = (InjectingTokenIdentifier)
        extractTokenIdentifier(binding2.getCanonicalServiceName(),
            binding2.getBoundDT());

    Assertions.assertThat(b2tokenId)
        .describedAs("secondary token %s", b2tokenId)
        .extracting(InjectingTokenIdentifier::getIssueNumber)
        .isEqualTo(InjectingTokenBinding.getIssueCount());

    // Get new credential chain
    AWSCredentialProviderList providerList
        = delegationTokens.getCredentialProviders();
    List<AWSCredentialsProvider> awsProviders = providerList.getProviders();
    Assertions.assertThat(awsProviders)
        .describedAs("providers from delegation token service")
        .hasSizeGreaterThan(1)
        .element(awsProviders.size() - 1)
        .isInstanceOf(DummyCredentialsProvider.class);
  }

  /**
   * Assert that the number of tokens issues matches all which the
   * FS is expected to.
   * @param cred credentials.
   */
  public void assertAllTokensIssued(final Credentials cred) {
    Assertions.assertThat(cred.getAllTokens())
        .describedAs("Tokens from fileystem")
        .hasSize(2);
  }

  /**
   * Create credentials with the DTs of the current FS.
   * @return a non-empty set of credentials.
   * @throws IOException failure to create.
   */
  protected Credentials createDelegationTokens() throws IOException {
    return mkTokens(getFileSystem());
  }

  /**
   * Create a FS with a delegated token, verify it works as a filesystem,
   * and that you can pick up the same DT from that FS too.
   */
  @Test
  public void testDelegatedFileSystem() throws Throwable {
    describe("Delegation tokens can be passed to a new filesystem");
    S3AFileSystem fs = getFileSystem();

    URI uri = fs.getUri();
    // create delegation tokens from the test suites FS.
    Credentials creds = createDelegationTokens();
    final Text tokenKind = getTokenKind();
    AbstractS3ATokenIdentifier origTokenId = requireNonNull(
        lookupToken(
            creds,
            uri,
            tokenKind), "original");
    // attach to the user, so that when tokens are looked for, they get picked
    // up
    final UserGroupInformation currentUser
        = UserGroupInformation.getCurrentUser();
    currentUser.addCredentials(creds);
    // verify that the tokens went over
    requireNonNull(lookupToken(
        currentUser.getCredentials(),
        uri,
        tokenKind), "user credentials");
    Configuration conf = new Configuration(getConfiguration());
    String bucket = fs.getBucket();
    disableFilesystemCaching(conf);
    unsetHadoopCredentialProviders(conf);
    // remove any secrets we don't want the delegated FS to accidentally
    // pick up.
    // this is to simulate better a remote deployment.
    removeBaseAndBucketOverrides(bucket, conf,
        ACCESS_KEY, SECRET_KEY, SESSION_TOKEN,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY,
        DELEGATION_TOKEN_ROLE_ARN,
        DELEGATION_TOKEN_ENDPOINT);
    // this is done to make sure you cannot create an STS session no
    // matter how you pick up credentials.
    conf.set(DELEGATION_TOKEN_ENDPOINT, "http://localhost:8080/");
    bindProviderList(bucket, conf, CountInvocationsProvider.NAME);
    long originalCount = CountInvocationsProvider.getInvocationCount();

    // create a new FS instance, which is expected to pick up the
    // existing token
    Path testPath = path("testDTFileSystemClient");
    try (S3AFileSystem delegatedFS = newS3AInstance(uri, conf)) {
      LOG.info("Delegated filesystem is: {}", delegatedFS);
      assertBoundToDT(delegatedFS, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull("Encryption propagation failed",
            delegatedFS.getServerSideEncryptionAlgorithm());
        assertEquals("Encryption propagation failed",
            fs.getServerSideEncryptionAlgorithm(),
            delegatedFS.getServerSideEncryptionAlgorithm());
      }

      executeDelegatedFSOperations(delegatedFS, testPath);
      delegatedFS.mkdirs(testPath);

      S3ATestUtils.MetricDiff issueDiff = new S3ATestUtils.MetricDiff(
          delegatedFS,
          Statistic.DELEGATION_TOKENS_ISSUED);

      // verify that the FS returns the existing tokens
      // so that chained deployments will work
      Credentials creds2 = mkTokens(delegatedFS);
      assertAllTokensIssued(creds2);
      Assertions.assertThat(creds2.getAllTokens())
          .containsAll(creds.getAllTokens());

      AbstractS3ATokenIdentifier tokenFromDelegatedFS
          = requireNonNull(delegatedFS.getDelegationToken(""),
          "New token").decodeIdentifier();
      assertEquals("Newly issued token != old one",
          origTokenId,
          tokenFromDelegatedFS);
      issueDiff.assertDiffEquals("DTs issued in " + delegatedFS,
          0);
    }
    // the DT auth chain should override the original one.
    assertEquals("invocation count",
        originalCount,
        CountInvocationsProvider.getInvocationCount());

    // create a second instance, which will pick up the same value
    try (S3AFileSystem secondDelegate = newS3AInstance(uri, conf)) {
      assertBoundToDT(secondDelegate, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull("Encryption propagation failed",
            secondDelegate.getServerSideEncryptionAlgorithm());
        assertEquals("Encryption propagation failed",
            fs.getServerSideEncryptionAlgorithm(),
            secondDelegate.getServerSideEncryptionAlgorithm());
      }
      ContractTestUtils.assertDeleted(secondDelegate, testPath, true);
      assertNotNull("unbounded DT",
          secondDelegate.getDelegationToken(""));
      Credentials creds3 = mkTokens(secondDelegate);
      assertAllTokensIssued(creds3);
    }
  }

  /**
   * Override/extension point: run operations within a delegated FS.
   * @param delegatedFS filesystem.
   * @param testPath path to work on.
   * @throws IOException failures
   */
  protected void executeDelegatedFSOperations(final S3AFileSystem delegatedFS,
      final Path testPath) throws Exception {
    ContractTestUtils.assertIsDirectory(delegatedFS, new Path("/"));
    ContractTestUtils.touch(delegatedFS, testPath);
    ContractTestUtils.assertDeleted(delegatedFS, testPath, false);
    delegatedFS.mkdirs(testPath);
    ContractTestUtils.assertIsDirectory(delegatedFS, testPath);
    Path srcFile = new Path(testPath, "src.txt");
    Path destFile = new Path(testPath, "dest.txt");
    ContractTestUtils.touch(delegatedFS, srcFile);
    ContractTestUtils.rename(delegatedFS, srcFile, destFile);
    // this file is deleted afterwards, so leave alone
    ContractTestUtils.assertIsFile(delegatedFS, destFile);
    ContractTestUtils.assertDeleted(delegatedFS, testPath, true);
  }

  /**
   * YARN job submission uses
   * {@link TokenCache#obtainTokensForNamenodes(Credentials, Path[], Configuration)}
   * for token retrieval: call it here to verify it works.
   */
  @Test
  public void testYarnCredentialPickup() throws Throwable {
    describe("Verify tokens are picked up by the YARN"
        + " TokenCache.obtainTokensForNamenodes() API Call");
    Credentials cred = new Credentials();
    Path yarnPath = path("testYarnCredentialPickup");
    Path[] paths = new Path[]{yarnPath};
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    TokenCache.obtainTokensForNamenodes(cred, paths, conf);
    assertAllTokensIssued(cred);

  }

  protected File createTempTokenFile() throws IOException {
    File tokenfile = File.createTempFile("tokens", ".bin",
        cluster.getWorkDir());
    tokenfile.delete();
    return tokenfile;
  }

  protected String dtutil(int expected, String... args) throws Exception {
    final ByteArrayOutputStream dtUtilContent = new ByteArrayOutputStream();
    DtUtilShell dt = new DtUtilShell();
    dt.setOut(new PrintStream(dtUtilContent));
    dtUtilContent.reset();
    int r = doAs(aliceUser,
        () -> ToolRunner.run(getConfiguration(), dt, args));
    String s = dtUtilContent.toString();
    LOG.info("\n{}", s);
    assertEquals(expected, r);
    return s;
  }

  @Test
  public void testDTUtilShell() throws Throwable {
    describe("Verify the dtutil shell command can fetch all tokens");
    File tokenfile = createTempTokenFile();

    String tfs = tokenfile.toString();
    String fsURI = getFileSystem().getCanonicalUri().toString();
    dtutil(0,
        "get", fsURI,
        "-format", "protobuf",
        tfs);
    assertTrue("not created: " + tokenfile,
        tokenfile.exists());
    assertTrue("File is empty" + tokenfile,
        tokenfile.length() > 0);
    assertTrue("File only contains header " + tokenfile,
        tokenfile.length() > 6);
    Credentials creds = Credentials.readTokenStorageFile(tokenfile,
        getConfiguration());
    assertAllTokensIssued(creds);

  }

  @Test
  public void testDuplicateTokenKind() throws Throwable {
    describe("Verify that only one token kind per FS is permitted");
    S3AFileSystem fs = getFileSystem();

    URI uri = fs.getUri();
    // create delegation tokens from the test suites FS.
    Credentials creds = createDelegationTokens();
    final Text tokenKind = getTokenKind();
    AbstractS3ATokenIdentifier origTokenId = requireNonNull(
        lookupToken(
            creds,
            uri,
            tokenKind), "original");
    // attach to the user, so that when tokens are looked for, they get picked
    // up
    final UserGroupInformation currentUser
        = UserGroupInformation.getCurrentUser();
    currentUser.addCredentials(creds);
    // verify that the tokens went over
    requireNonNull(lookupToken(
        currentUser.getCredentials(),
        uri,
        tokenKind), "user credentials");
    Configuration conf = new Configuration(getConfiguration());
    String bucket = fs.getBucket();
    disableFilesystemCaching(conf);
    unsetHadoopCredentialProviders(conf);
    // remove any secrets we don't want the delegated FS to accidentally
    // pick up.
    // this is to simulate better a remote deployment.
    removeBaseAndBucketOverrides(bucket, conf,
        ACCESS_KEY, SECRET_KEY, SESSION_TOKEN,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY,
        DELEGATION_TOKEN_ROLE_ARN,
        DELEGATION_TOKEN_ENDPOINT);
    // this is done to make sure you cannot create an STS session no
    // matter how you pick up credentials.
    conf.set(DELEGATION_TOKEN_ENDPOINT, "http://localhost:8080/");
    bindProviderList(bucket, conf, CountInvocationsProvider.NAME);
    long originalCount = CountInvocationsProvider.getInvocationCount();

    // create a new FS instance, which is expected to pick up the
    // existing token
    Path testPath = path("testDTFileSystemClient");
    try (S3AFileSystem delegatedFS = newS3AInstance(uri, conf)) {
      LOG.info("Delegated filesystem is: {}", delegatedFS);
      assertBoundToDT(delegatedFS, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull("Encryption propagation failed",
            delegatedFS.getServerSideEncryptionAlgorithm());
        assertEquals("Encryption propagation failed",
            fs.getServerSideEncryptionAlgorithm(),
            delegatedFS.getServerSideEncryptionAlgorithm());
      }

      executeDelegatedFSOperations(delegatedFS, testPath);
      delegatedFS.mkdirs(testPath);

      S3ATestUtils.MetricDiff issueDiff = new S3ATestUtils.MetricDiff(
          delegatedFS,
          Statistic.DELEGATION_TOKENS_ISSUED);

      // verify that the FS returns the existing tokens
      // so that chained deployments will work
      Credentials creds2 = mkTokens(delegatedFS);
      assertAllTokensIssued(creds2);
      Assertions.assertThat(creds2.getAllTokens())
          .containsAll(creds.getAllTokens());

      AbstractS3ATokenIdentifier tokenFromDelegatedFS
          = requireNonNull(delegatedFS.getDelegationToken(""),
          "New token").decodeIdentifier();
      assertEquals("Newly issued token != old one",
          origTokenId,
          tokenFromDelegatedFS);
      issueDiff.assertDiffEquals("DTs issued in " + delegatedFS,
          0);
    }
    // the DT auth chain should override the original one.
    assertEquals("invocation count",
        originalCount,
        CountInvocationsProvider.getInvocationCount());

    // create a second instance, which will pick up the same value
    try (S3AFileSystem secondDelegate = newS3AInstance(uri, conf)) {
      assertBoundToDT(secondDelegate, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull("Encryption propagation failed",
            secondDelegate.getServerSideEncryptionAlgorithm());
        assertEquals("Encryption propagation failed",
            fs.getServerSideEncryptionAlgorithm(),
            secondDelegate.getServerSideEncryptionAlgorithm());
      }
      ContractTestUtils.assertDeleted(secondDelegate, testPath, true);
      assertNotNull("unbounded DT",
          secondDelegate.getDelegationToken(""));
      Credentials creds3 = mkTokens(secondDelegate);
      assertAllTokensIssued(creds3);
    }
  }

  /**
   * Our dummy credentials provider returns anonymous credentials.
   */
  public static class DummyCredentialsProvider
      implements AWSCredentialsProvider {

    public static final String NAME = DummyCredentialsProvider.class.getName();

    @Override
    public AWSCredentials getCredentials() {
      return new AnonymousAWSCredentials();
    }

    @Override
    public void refresh() {

    }
  }


}
