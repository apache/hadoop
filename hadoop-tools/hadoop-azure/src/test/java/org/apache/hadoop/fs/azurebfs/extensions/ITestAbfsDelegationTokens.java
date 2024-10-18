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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtUtilShell;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP;
import static org.apache.hadoop.test.LambdaTestUtils.doAs;

/**
 * Test custom DT support in ABFS.
 * This brings up a mini KDC in class setup/teardown, as the FS checks
 * for that when it enables security.
 *
 * Much of this code is copied from
 * {@code org.apache.hadoop.fs.s3a.auth.delegation.AbstractDelegationIT}
 */
public class ITestAbfsDelegationTokens extends AbstractAbfsIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestAbfsDelegationTokens.class);

  /**
   * Created in static {@link #setupCluster()} call.
   */
  @SuppressWarnings("StaticNonFinalField")
  private static KerberizedAbfsCluster cluster;

  private UserGroupInformation aliceUser;

  /***
   * Set up the clusters.
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    resetUGI();
    cluster = new KerberizedAbfsCluster();
    cluster.init(new Configuration());
    cluster.start();
  }

  /**
   * Tear down the Cluster.
   */
  @SuppressWarnings("ThrowableNotThrown")
  @AfterClass
  public static void teardownCluster() throws Exception {
    resetUGI();
    ServiceOperations.stopQuietly(LOG, cluster);
  }

  public ITestAbfsDelegationTokens() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    // create the FS
    Configuration conf = getRawConfiguration();
    cluster.bindConfToCluster(conf);
    conf.setBoolean(HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
        false);
    resetUGI();
    UserGroupInformation.setConfiguration(conf);
    aliceUser = cluster.createAliceUser();

    assertSecurityEnabled();
    // log in as alice so that filesystems belong to that user
    UserGroupInformation.setLoginUser(aliceUser);
    StubDelegationTokenManager.useStubDTManager(conf);
    FileSystem.closeAllForUGI(UserGroupInformation.getLoginUser());
    super.setup();
    assertNotNull("No StubDelegationTokenManager created in filesystem init",
        getStubDTManager());
  }

  protected StubDelegationTokenManager getStubDTManager() throws IOException {
    return (StubDelegationTokenManager) getDelegationTokenManager().getTokenManager();
  }

  /**
   * Cleanup removes cached filesystems and the last instance of the
   * StubDT manager.
   */
  @Override
  public void teardown() throws Exception {
    // clean up all of alice's instances.
    FileSystem.closeAllForUGI(UserGroupInformation.getLoginUser());
    super.teardown();
  }

  /**
   * General assertion that security is turred on for a cluster.
   */
  public static void assertSecurityEnabled() {
    assertTrue("Security is needed for this test",
        UserGroupInformation.isSecurityEnabled());
  }

  /**
   * Reset UGI info.
   */
  protected static void resetUGI() {
    UserGroupInformation.reset();
  }

  /**
   * Create credentials with the DTs of the given FS.
   * @param fs filesystem
   * @return a non-empty set of credentials.
   * @throws IOException failure to create.
   */
  protected static Credentials mkTokens(final FileSystem fs)
      throws IOException {
    Credentials cred = new Credentials();
    fs.addDelegationTokens("rm/rm1@EXAMPLE.COM", cred);
    return cred;
  }

  @Test
  public void testTokenManagerBinding() throws Throwable {
    StubDelegationTokenManager instance
        = getStubDTManager();
    assertNotNull("No StubDelegationTokenManager created in filesystem init",
        instance);
    assertTrue("token manager not initialized: " + instance,
        instance.isInitialized());
  }

  /**
   * When bound to a custom DT manager, it provides the service name.
   * By default, the returned value matches that which can be derived from the
   * FS URI through {@link ExtensionHelper#buildCanonicalServiceName(URI)}.
   */
  @Test
  public void testCanonicalization() throws Throwable {
    String service = getCanonicalServiceName();
    assertNotNull("No canonical service name from filesystem " + getFileSystem(),
        service);
    assertEquals("canonical URI and service name mismatch",
        ExtensionHelper.buildCanonicalServiceName(getFilesystemURI()),
        ExtensionHelper.buildCanonicalServiceName(new URI(service)));
  }

  /**
   * Get the URI of the suite's FS.
   * @return FS URI.
   */
  protected URI getFilesystemURI() throws IOException {
    return getFileSystem().getUri();
  }

  /**
   * The canonical service name of the test suite's filesystem.
   * @return service name.
   */
  protected String getCanonicalServiceName() throws IOException {
    return getFileSystem().getCanonicalServiceName();
  }

  /**
   * Build the canonical service name from the FS URI.
   * @return string
   */
  protected String buildFilesystemURICanonicalServiceName() throws IOException {
    return ExtensionHelper.buildCanonicalServiceName(getFilesystemURI());
  }

  /**
   * Checks here to catch any regressions in canonicalization
   * logic.
   */
  @Test
  public void testDefaultCanonicalization() throws Throwable {

    // clear the token service name in the stub DT manager.
    clearTokenServiceName();

    assertEquals("canonicalServiceName is not that expected",
        buildFilesystemURICanonicalServiceName(),
        getCanonicalServiceName());
  }

  /**
   * Set the canonical service name of the stub DT manager
   * to null.
   */
  protected void clearTokenServiceName() throws IOException {
    getStubDTManager().setCanonicalServiceName(null);
  }

  /**
   * Request a token; this tests the collection workflow.
   */
  @Test
  public void testRequestToken() throws Throwable {
    AzureBlobFileSystem fs = getFileSystem();
    Credentials credentials = mkTokens(fs);
    assertEquals("Number of collected tokens", 1,
        credentials.numberOfTokens());
    verifyCredentialsContainsToken(credentials, fs);
  }

  /**
   * Request a token; this tests the collection workflow.
   */
  @Test
  public void testRequestTokenDefault() throws Throwable {
    clearTokenServiceName();
    final String canonicalServiceName = buildFilesystemURICanonicalServiceName();
    AzureBlobFileSystem fs = getFileSystem();
    assertEquals("canonicalServiceName is not the expected value",
        canonicalServiceName,
        fs.getCanonicalServiceName());

    Credentials credentials = mkTokens(fs);
    assertEquals("Number of collected tokens", 1,
        credentials.numberOfTokens());
    verifyCredentialsContainsToken(credentials,
        canonicalServiceName,
        fs.getCanonicalServiceName());
  }

  public void verifyCredentialsContainsToken(final Credentials credentials,
      FileSystem fs) throws IOException {
    verifyCredentialsContainsToken(credentials,
        fs.getCanonicalServiceName(),
        fs.getCanonicalServiceName());
  }

  /**
   * Verify that the set of credentials contains a token for the given
   * canonical service name, and that it is of the given kind.
   * @param credentials set of credentials
   * @param serviceName canonical service name for lookup.
   * @param tokenService service kind; also expected in string value.
   * @return the retrieved token.
   * @throws IOException IO failure
   */
  public StubAbfsTokenIdentifier verifyCredentialsContainsToken(
      final Credentials credentials,
      final String serviceName,
      final String tokenService) throws IOException {
    Token<? extends TokenIdentifier> token = credentials.getToken(
        new Text(serviceName));

    Assertions.assertThat(token)
        .describedAs("No token found for %s", serviceName)
        .isNotNull();

    Assertions.assertThat(token.getKind())
        .describedAs("Token Kind in %s", token)
        .isEqualTo(StubAbfsTokenIdentifier.TOKEN_KIND);
    Assertions.assertThat(token.getService().toString())
        .describedAs("Token Service in %s", token)
        .isEqualTo(tokenService);

    StubAbfsTokenIdentifier abfsId = (StubAbfsTokenIdentifier)
        token.decodeIdentifier();
    LOG.info("Created token {}", abfsId);
    assertEquals("token URI in AbfsTokenIdentifier " + abfsId,
        tokenService, abfsId.getUri().toString());
    return abfsId;
  }

  /**
   * This mimics the DT collection performed inside FileInputFormat to
   * collect DTs for a job.
   * @throws Throwable on failure.
   */
  @Test
  public void testJobsCollectTokens() throws Throwable {
    // get tokens for all the required FileSystems..
    AzureBlobFileSystem fs = getFileSystem();
    Credentials credentials = new Credentials();
    Path root = fs.makeQualified(new Path("/"));
    Path[] paths = {root};

    Configuration conf = fs.getConf();
    TokenCache.obtainTokensForNamenodes(credentials,
          paths,
          conf);
    verifyCredentialsContainsToken(credentials, fs);
  }

  /**
   * Run the DT Util command.
   * @param expected expected outcome
   * @param conf configuration for the command (hence: FS to create)
   * @param args other arguments
   * @return the output of the command.
   */
  protected String dtutil(final int expected,
      final Configuration conf,
      final String... args) throws Exception {
    final ByteArrayOutputStream dtUtilContent = new ByteArrayOutputStream();
    DtUtilShell dt = new DtUtilShell();
    dt.setOut(new PrintStream(dtUtilContent));
    dtUtilContent.reset();
    int r = doAs(aliceUser,
        () -> ToolRunner.run(conf, dt, args));
    String s = dtUtilContent.toString();
    LOG.info("\n{}", s);
    assertEquals("Exit code from command dtutil "
        + StringUtils.join(" ", args) + " with output " + s,
        expected, r);
    return s;
  }

  /**
   * Verify the dtutil shell command can fetch tokens
   */
  @Test
  public void testDTUtilShell() throws Throwable {
    File tokenfile = cluster.createTempTokenFile();

    String tfs = tokenfile.toString();
    String fsURI = getFileSystem().getUri().toString();
    dtutil(0, getRawConfiguration(),
        "get", fsURI,
        "-format", "protobuf",
        tfs);
    assertTrue("not created: " + tokenfile,
        tokenfile.exists());
    assertTrue("File is empty " + tokenfile,
        tokenfile.length() > 0);
    assertTrue("File only contains header " + tokenfile,
        tokenfile.length() > 6);

    String printed = dtutil(0, getRawConfiguration(), "print", tfs);

    // now look in the printed output and verify that it contains
    // expected strings.
    Assertions.assertThat(printed)
        .describedAs("canonical service name in output")
        .contains(getCanonicalServiceName());
    Assertions.assertThat(printed)
        .describedAs("token identifier in output")
        .contains(StubAbfsTokenIdentifier.ID);
  }

  /**
   * Creates a new FS instance with the simplest binding lifecycle;
   * get a token.
   * This verifies the classic binding mechanism works.
   */
  @Test
  public void testBaseDTLifecycle() throws Throwable {

    Configuration conf = new Configuration(getRawConfiguration());
    ClassicDelegationTokenManager.useClassicDTManager(conf);
    try (FileSystem fs = FileSystem.newInstance(getFilesystemURI(), conf)) {
      Credentials credentials = mkTokens(fs);
      assertEquals("Number of collected tokens", 1,
          credentials.numberOfTokens());
      // The (test) classic DT manager builds up its URI service name from
      // a constant. this is canonicalized and used as the service name.
      verifyCredentialsContainsToken(credentials,
          fs.getCanonicalServiceName(),
          ClassicDelegationTokenManager.UNSET_URI_SERVICE_NAME);
    }
  }
}
