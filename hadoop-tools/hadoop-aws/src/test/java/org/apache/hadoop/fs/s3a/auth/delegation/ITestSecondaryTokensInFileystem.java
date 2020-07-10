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

import com.amazonaws.auth.AWSCredentialsProvider;
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
import org.apache.hadoop.fs.s3a.auth.delegation.providers.InjectingTokenBinding;
import org.apache.hadoop.fs.s3a.auth.delegation.providers.InjectingTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtUtilShell;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
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
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.assertSecurityEnabled;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.ERROR_DUPLICATE_TOKENS;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.extractTokenIdentifier;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupS3ADelegationToken;
import static org.apache.hadoop.test.LambdaTestUtils.doAs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests use of Hadoop delegation tokens within the FS itself.
 * <p></p>
 * This instantiates a MiniKDC as some of the operations tested require
 * UGI to be initialized with security enabled.
 * <p></p>
 * Derived from {@link ITestSessionDelegationInFileystem}; uses
 * the encrypting DT as primary;
 * the full ad injecting as secondary.
 * This set up makes it easy to verify that 2ary bindings
 * are working: if a bound filesystem can talk to S3, it
 * must be getting secrets from the secondary token list.
 */
@SuppressWarnings("StaticNonFinalField")
public class ITestSecondaryTokensInFileystem extends AbstractDelegationIT {

  /**
   * How many secondary tokens are expected to be issued.
   */
  public static final int EXPECTED_2ARY_TOKEN_COUNT = 2;

  /**
   * Total count of the tokens expected to be issued.
   */
  public static final int EXPECTED_TOTAL_TOKEN_COUNT =
      1 + EXPECTED_2ARY_TOKEN_COUNT;

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSecondaryTokensInFileystem.class);

  private static MiniKerberizedHadoopCluster cluster;

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
    return DELEGATION_TOKEN_ENCRYPTING_BINDING;
  }

  /**
   * Get the kind of the tokens which are generated.
   * @return the kind of DT
   */
  public Text getTokenKind() {
    return ENCRYPTING_TOKEN_KIND;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    disableFilesystemCaching(conf);
    resetAllDTConfigOptions(conf);
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    enableDelegationTokens(conf, getDelegationBinding());
    // add the full credentials as a secondary token
    // this can make it independent of the FS.
    conf.set(DELEGATION_SECONDARY_BINDINGS,
        DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING);
    conf.set(FULL_SECONDARY_TOKEN_NAME,
        "ITestSecondaryTokensInFileystem");

    InjectingTokenBinding.addAsSecondaryToken(conf, true,
        CountInvocationsProvider.NAME);

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
    cluster.resetUGI();
  }

  /**
   * Are encryption tests enabled?
   * @return true if encryption is turned on.
   */
  protected boolean encryptionTestEnabled() {
    return getConfiguration().getBoolean(KEY_ENCRYPTION_TESTS, true);
  }

  /**
   * filesystem getDelegationToken only returns the 1ary token.
   */
  @Test
  public void testGetPrimaryTokenfromFileSystem() throws Throwable {
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
        .hasSize(EXPECTED_TOTAL_TOKEN_COUNT);
    assertAllTokensIssued(creds);
    final long secondIssueCount = InjectingTokenBinding.getIssueCount();
    Assertions.assertThat(secondIssueCount - initialIssueCount)
        .describedAs("InjectingTokenBinding issue count")
        .isEqualTo(1);

    // the first token in the list of issued tokens is the
    // primary.
    Token<?> token = requireNonNull(tokens[0], "token");
    LOG.info("FS token is {}", token);
    Text service = delegationTokens.getService();

    requireNonNull(
        creds.getToken(service),
        "retrieved token with key " + service + "; expected " + token);

    // now the secondary token
    List<SecondaryDelegationToken> secondaryTokens
        = delegationTokens.listSecondaryTokens();
    Assertions.assertThat(secondaryTokens)
        .describedAs("secondary token list")
        .hasSize(EXPECTED_2ARY_TOKEN_COUNT);
    SecondaryDelegationToken binding2 = tail(secondaryTokens);
    Text b2service = binding2.getServiceName();
    Token<AbstractS3ATokenIdentifier> b2token = binding2
        .bindToToken(creds);
    InjectingTokenIdentifier b2tokenId = (InjectingTokenIdentifier)
        extractTokenIdentifier(b2service.toString(), b2token, "");
    Assertions.assertThat(b2tokenId)
        .describedAs("secondary token %s", b2tokenId)
        .extracting(InjectingTokenIdentifier::getIssueNumber)
        .isEqualTo(secondIssueCount);
  }

  /**
   * The this verifies that when an S3A token service is brought up with
   * secondary tokens, then they are picked up and the token specific
   * binding service instantiated.
   */
  @Test
  public void testDTCredentialProviderFromCurrentUserCreds()
      throws Throwable {

    describe("Add credentials to the current user, "
        + "then verify that they can be found when S3ADelegationTokens binds");
    Credentials cred = createDelegationTokens();
    assertAllTokensIssued(cred);
    UserGroupInformation.getCurrentUser().addCredentials(cred);
    delegationTokens.start();
    assertTrue("bind to existing DT failed",
        delegationTokens.isBoundToDT());
    SecondaryDelegationToken binding2
        = tail(delegationTokens.listSecondaryTokens());
    Assertions.assertThat(binding2.getBoundDT())
        .describedAs("secondary token")
        .isNotNull();
    InjectingTokenIdentifier b2tokenId = (InjectingTokenIdentifier)
        extractTokenIdentifier(binding2.getCanonicalServiceName(),
            binding2.getBoundDT(), "");

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
        .isInstanceOf(CountInvocationsProvider.class);
  }

  /**
   * Assert that the number of tokens issued matches all which the
   * FS is expected to issue.
   * @param cred credentials.
   */
  public void assertAllTokensIssued(final Credentials cred) {
    Assertions.assertThat(cred.getAllTokens())
        .describedAs("Tokens from fileystem")
        .hasSize(EXPECTED_TOTAL_TOKEN_COUNT);
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

    // now, this one has a different URI -landsat
    Path landsatCSVPath = S3ATestUtils.getLandsatCSVPath(conf);

    // create a new FS instance with a different URI
    // the encrypted token will not be picked up, but
    // because the full tokens are 2ary, with a service
    // name independent of the FS URI, they will get picked up.
    try (S3AFileSystem landsatFS =
             newS3AInstance(landsatCSVPath.toUri(), conf)) {
      S3ADelegationTokens dtSupport = getFSDelegationTokenSupport(fs);
      assertFalse("Expected 1ary dt to be unbound: " + dtSupport,
          dtSupport.isBoundToDT());
      landsatFS.getFileStatus(landsatCSVPath);
      assertNotNull("unbounded DT",
          landsatFS.getDelegationToken(""));
      Credentials creds3 = mkTokens(landsatFS);
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

  @Test
  public void testDuplicateTokenKind() throws Throwable {
    describe("Verify that duplicate secondary token kinds are rejected");

    Configuration conf = new Configuration(getConfiguration());
    disableFilesystemCaching(conf);
    unsetHadoopCredentialProviders(conf);

    InjectingTokenBinding.addAsSecondaryToken(conf, true,
        CountInvocationsProvider.NAME);
    InjectingTokenBinding.addAsSecondaryToken(conf, true,
        null);
    try (S3AFileSystem delegatedFS = new S3AFileSystem()) {
      intercept(ServiceStateException.class,
          ERROR_DUPLICATE_TOKENS, () ->
              delegatedFS.initialize(getFileSystem().getUri(), conf));
    }
  }
  @Test
  public void testDuplicateTokenKindOfFS() throws Throwable {
    describe("Verify thata secondary DT using the token name of the FS URI"
        + " is rejected");

    Configuration conf = new Configuration(getConfiguration());
    disableFilesystemCaching(conf);
    unsetHadoopCredentialProviders(conf);
    URI fsuri = getFileSystem().getUri();
    conf.set(INJECTING_SECONDARY_TOKEN_NAME, fsuri.toString());

    InjectingTokenBinding.addAsSecondaryToken(conf, true,
        CountInvocationsProvider.NAME);
    try (S3AFileSystem delegatedFS = new S3AFileSystem()) {
      intercept(ServiceStateException.class,
          ERROR_DUPLICATE_TOKENS, () ->
              delegatedFS.initialize(fsuri, conf));
    }
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
        () -> ToolRunner.run(getConfiguration(), dt, args(args)));
    String s = dtUtilContent.toString();
    LOG.info("\n{}", s);
    assertEquals("result of dtutil", expected, r);
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

  /**
   * Test the {@code hdfs fetchdt} command works with S3A tokens.
   */
  @Test
  public void testHDFSFetchDTCommand() throws Throwable {
    describe("Use the HDFS fetchdt CLI to fetch a token");

    Credentials creds = fetchTokensThroughDtUtils(
        aliceUser,
        getTokenKind(),
        createTempTokenFile());
    assertAllTokensIssued(creds);

  }

}
