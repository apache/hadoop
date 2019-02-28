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
import org.apache.hadoop.security.SecurityUtil;
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
   * The stub returns the URI by default.
   */
  @Test
  public void testCanonicalization() throws Throwable {
    String service = getCanonicalServiceName();
    assertNotNull("No canonical service name from filesystem " + getFileSystem(),
        service);
    assertEquals("canonical URI and service name mismatch",
        getFilesystemURI(), new URI(service));
  }

  protected URI getFilesystemURI() throws IOException {
    return getFileSystem().getUri();
  }

  protected String getCanonicalServiceName() throws IOException {
    return getFileSystem().getCanonicalServiceName();
  }

  /**
   * Checks here to catch any regressions in canonicalization
   * logic.
   */
  @Test
  public void testDefaultCanonicalization() throws Throwable {
    FileSystem fs = getFileSystem();
    clearTokenServiceName();

    assertEquals("canonicalServiceName is not the default",
        getDefaultServiceName(fs), getCanonicalServiceName());
  }

  protected String getDefaultServiceName(final FileSystem fs) {
    return SecurityUtil.buildDTServiceName(fs.getUri(), 0);
  }

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

    AzureBlobFileSystem fs = getFileSystem();
    assertEquals("canonicalServiceName is not the default",
        getDefaultServiceName(fs), fs.getCanonicalServiceName());

    Credentials credentials = mkTokens(fs);
    assertEquals("Number of collected tokens", 1,
        credentials.numberOfTokens());
    verifyCredentialsContainsToken(credentials,
        getDefaultServiceName(fs), getFilesystemURI().toString());
  }

  public void verifyCredentialsContainsToken(final Credentials credentials,
      FileSystem fs) throws IOException {
    verifyCredentialsContainsToken(credentials,
        fs.getCanonicalServiceName(),
        fs.getUri().toString());
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

    assertEquals("Token Kind in " + token,
        StubAbfsTokenIdentifier.TOKEN_KIND, token.getKind());
    assertEquals("Token Service Kind in " + token,
        tokenService, token.getService().toString());

    StubAbfsTokenIdentifier abfsId = (StubAbfsTokenIdentifier)
        token.decodeIdentifier();
    LOG.info("Created token {}", abfsId);
    assertEquals("token URI in " + abfsId,
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
    assertTrue("no " + fsURI + " in " + printed,
        printed.contains(fsURI));
    assertTrue("no " + StubAbfsTokenIdentifier.ID + " in " + printed,
        printed.contains(StubAbfsTokenIdentifier.ID));
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
      verifyCredentialsContainsToken(credentials,
          fs.getCanonicalServiceName(),
          ClassicDelegationTokenManager.UNSET);
    }
  }
}
