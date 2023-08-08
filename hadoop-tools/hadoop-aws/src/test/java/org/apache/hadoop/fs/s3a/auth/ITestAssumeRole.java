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

package org.apache.hadoop.fs.s3a.auth;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.stream.IntStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.MultipartUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.s3a.statistics.CommitterStatistics;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.*;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.*;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.forbidden;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.newAssumedRoleConfig;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Tests use of assumed roles.
 * Only run if an assumed role is provided.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ITestAssumeRole extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAssumeRole.class);

  private static final Path ROOT = new Path("/");

  private static final Statement STATEMENT_ALL_BUCKET_READ_ACCESS
      = statement(true, S3_ALL_BUCKETS, S3_BUCKET_READ_OPERATIONS);

  /**
   * test URI, built in setup.
   */
  private URI uri;

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

  /**
   * Error code from STS server.
   */
  protected static final String VALIDATION_ERROR
      = "ValidationError";

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
    uri = new URI(S3ATestConstants.DEFAULT_CSVTEST_FILE);
  }

  @Override
  public void teardown() throws Exception {
    cleanupWithLogger(LOG, roleFS);
    super.teardown();
  }

  private void assumeRoleTests() {
    assume("No ARN for role tests", !getAssumedRoleARN().isEmpty());
  }

  private String getAssumedRoleARN() {
    return getContract().getConf().getTrimmed(ASSUMED_ROLE_ARN, "");
  }

  /**
   * Expect a filesystem to fail to instantiate.
   * @param conf config to use
   * @param clazz class of exception to expect
   * @param text text in exception
   * @param <E> type of exception as inferred from clazz
   * @return the caught exception if it was of the expected type and contents
   * @throws Exception if the exception was the wrong class
   */
  private <E extends Throwable> E expectFileSystemCreateFailure(
      Configuration conf,
      Class<E> clazz,
      String text) throws Exception {
    return interceptClosing(clazz,
        text,
        () -> new Path(getFileSystem().getUri()).getFileSystem(conf));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateCredentialProvider() throws IOException {
    describe("Create the credential provider");

    Configuration conf = createValidRoleConf();
    try (AssumedRoleCredentialProvider provider
             = new AssumedRoleCredentialProvider(uri, conf)) {
      LOG.info("Provider is {}", provider);
      AWSCredentials credentials = provider.getCredentials();
      assertNotNull("Null credentials from " + provider, credentials);
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateCredentialProviderNoURI() throws IOException {
    describe("Create the credential provider");

    Configuration conf = createValidRoleConf();
    try (AssumedRoleCredentialProvider provider
             = new AssumedRoleCredentialProvider(null, conf)) {
      LOG.info("Provider is {}", provider);
      AWSCredentials credentials = provider.getCredentials();
      assertNotNull("Null credentials from " + provider, credentials);
    }
  }

  /**
   * Create a valid role configuration.
   * @return a configuration set to use to the role ARN.
   * @throws JsonProcessingException problems working with JSON policies.
   */
  @SuppressWarnings("deprecation")
  protected Configuration createValidRoleConf() throws JsonProcessingException {
    String roleARN = getAssumedRoleARN();

    Configuration conf = new Configuration(getContract().getConf());
    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    conf.set(ASSUMED_ROLE_ARN, roleARN);
    conf.set(ASSUMED_ROLE_SESSION_NAME, "valid");
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "45m");
    bindRolePolicy(conf, RESTRICTED_POLICY);
    return conf;
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumedInvalidRole() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_ARN, ROLE_ARN_EXAMPLE);
    interceptClosing(AWSSecurityTokenServiceException.class,
        "",
        () -> new AssumedRoleCredentialProvider(uri, conf));
  }

  @Test
  public void testAssumeRoleFSBadARN() throws Exception {
    describe("Attemnpt to create the FS with an invalid ARN");
    Configuration conf = createAssumedRoleConfig();
    conf.set(ASSUMED_ROLE_ARN, ROLE_ARN_EXAMPLE);
    expectFileSystemCreateFailure(conf, AccessDeniedException.class, "");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumeRoleNoARN() throws Exception {
    describe("Attemnpt to create the FS with no ARN");
    Configuration conf = createAssumedRoleConfig();
    conf.unset(ASSUMED_ROLE_ARN);
    expectFileSystemCreateFailure(conf,
        IOException.class,
        AssumedRoleCredentialProvider.E_NO_ROLE);
  }

  @Test
  public void testAssumeRoleFSBadPolicy() throws Exception {
    describe("Attemnpt to create the FS with malformed JSON");
    Configuration conf = createAssumedRoleConfig();
    // add some malformed JSON
    conf.set(ASSUMED_ROLE_POLICY,  "}");
    expectFileSystemCreateFailure(conf,
        AWSBadRequestException.class,
        "JSON");
  }

  @Test
  public void testAssumeRoleFSBadPolicy2() throws Exception {
    describe("Attempt to create the FS with valid but non-compliant JSON");
    Configuration conf = createAssumedRoleConfig();
    // add some invalid JSON
    conf.set(ASSUMED_ROLE_POLICY, "{'json':'but not what AWS wants}");
    expectFileSystemCreateFailure(conf,
        AWSBadRequestException.class,
        "Syntax errors in policy");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumeRoleCannotAuthAssumedRole() throws Exception {
    describe("Assert that you can't use assumed roles to auth assumed roles");

    Configuration conf = createAssumedRoleConfig();
    unsetHadoopCredentialProviders(conf);
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        AssumedRoleCredentialProvider.NAME);
    expectFileSystemCreateFailure(conf,
        IOException.class,
        E_FORBIDDEN_AWS_PROVIDER);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumeRoleBadInnerAuth() throws Exception {
    describe("Try to authenticate with a keypair with spaces");

    Configuration conf = createAssumedRoleConfig();
    unsetHadoopCredentialProviders(conf);
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER, SimpleAWSCredentialsProvider.NAME);
    conf.set(ACCESS_KEY, "not valid");
    conf.set(SECRET_KEY, "not secret");
    expectFileSystemCreateFailure(conf,
        AWSBadRequestException.class,
        "not a valid " +
        "key=value pair (missing equal-sign) in Authorization header");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumeRoleBadInnerAuth2() throws Exception {
    describe("Try to authenticate with an invalid keypair");

    Configuration conf = createAssumedRoleConfig();
    unsetHadoopCredentialProviders(conf);
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER, SimpleAWSCredentialsProvider.NAME);
    conf.set(ACCESS_KEY, "notvalid");
    conf.set(SECRET_KEY, "notsecret");
    expectFileSystemCreateFailure(conf,
        AccessDeniedException.class,
        "The security token included in the request is invalid");
  }

  @Test
  public void testAssumeRoleBadSession() throws Exception {
    describe("Try to authenticate with an invalid session");

    Configuration conf = createAssumedRoleConfig();
    conf.set(ASSUMED_ROLE_SESSION_NAME,
        "Session names cannot hava spaces!");
    expectFileSystemCreateFailure(conf,
        AWSBadRequestException.class,
        "Member must satisfy regular expression pattern");
  }

  /**
   * A duration >1h is forbidden client-side in AWS SDK 1.11.271;
   * with the ability to extend durations deployed in March 2018,
   * duration checks will need to go server-side, and, presumably,
   * later SDKs will remove the client side checks.
   * This code exists to see when this happens.
   */
  @Test
  public void testAssumeRoleThreeHourSessionDuration() throws Exception {
    describe("Try to authenticate with a long session duration");

    Configuration conf = createAssumedRoleConfig();
    // add a duration of three hours
    conf.setInt(ASSUMED_ROLE_SESSION_DURATION, 3 * 60 * 60);
    try {
      new Path(getFileSystem().getUri()).getFileSystem(conf).close();
      LOG.info("Successfully created token of a duration >3h");
    } catch (IOException ioe) {
      assertExceptionContains(VALIDATION_ERROR, ioe);
    }
  }

  /**
   * A duration >1h is forbidden client-side in AWS SDK 1.11.271;
   * with the ability to extend durations deployed in March 2018.
   * with the later SDKs, the checks go server-side and
   * later SDKs will remove the client side checks.
   * This test doesn't look into the details of the exception
   * to avoid being too brittle.
   */
  @Test
  public void testAssumeRoleThirtySixHourSessionDuration() throws Exception {
    describe("Try to authenticate with a long session duration");

    Configuration conf = createAssumedRoleConfig();
    conf.setInt(ASSUMED_ROLE_SESSION_DURATION, 36 * 60 * 60);
    IOException ioe = expectFileSystemCreateFailure(conf,
        IOException.class, null);
  }

  /**
   * Create the assumed role configuration.
   * @return a config bonded to the ARN of the assumed role
   */
  public Configuration createAssumedRoleConfig() {
    return createAssumedRoleConfig(getAssumedRoleARN());
  }

  /**
   * Create a config for an assumed role; it also disables FS caching.
   * @param roleARN ARN of role
   * @return the new configuration
   */
  private Configuration createAssumedRoleConfig(String roleARN) {
    return newAssumedRoleConfig(getContract().getConf(), roleARN);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumeRoleUndefined() throws Throwable {
    describe("Verify that you cannot instantiate the"
        + " AssumedRoleCredentialProvider without a role ARN");
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_ARN, "");
    interceptClosing(IOException.class,
        AssumedRoleCredentialProvider.E_NO_ROLE,
        () -> new AssumedRoleCredentialProvider(uri, conf));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAssumedIllegalDuration() throws Throwable {
    describe("Expect the constructor to fail if the session is to short");
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "30s");
    interceptClosing(AWSSecurityTokenServiceException.class, "",
        () -> new AssumedRoleCredentialProvider(uri, conf));
  }

  @Test
  public void testAssumeRoleCreateFS() throws IOException {
    describe("Create an FS client with the role and do some basic IO");

    String roleARN = getAssumedRoleARN();
    Configuration conf = createAssumedRoleConfig(roleARN);
    Path path = new Path(getFileSystem().getUri());
    LOG.info("Creating test FS and user {} with assumed role {}",
        conf.get(ACCESS_KEY), roleARN);

    try (FileSystem fs = path.getFileSystem(conf)) {
      fs.getFileStatus(ROOT);
      fs.mkdirs(path("testAssumeRoleFS"));
    }
  }

  @Test
  public void testAssumeRoleRestrictedPolicyFS() throws Exception {
    describe("Restrict the policy for this session; verify that reads fail.");

    Configuration conf = createAssumedRoleConfig();
    bindRolePolicy(conf, RESTRICTED_POLICY);
    Path path = new Path(getFileSystem().getUri());
    try (FileSystem fs = path.getFileSystem(conf)) {
      forbidden("getFileStatus",
          () -> fs.getFileStatus(methodPath()));
      forbidden("",
          () -> fs.listStatus(ROOT));
      forbidden("",
          () -> fs.listFiles(ROOT, true));
      forbidden("",
          () -> fs.listLocatedStatus(ROOT));
      forbidden("",
          () -> fs.mkdirs(path("testAssumeRoleFS")));
    }
  }

  /**
   * Tighten the extra policy on the assumed role call for torrent access,
   * and verify that it blocks all other operations.
   * That is: any non empty policy in the assumeRole API call overrides
   * all of the policies attached to the role before.
   * switches the role instance to only those policies in the
   */
  @Test
  public void testAssumeRolePoliciesOverrideRolePerms() throws Throwable {

    describe("extra policies in assumed roles need;"
        + " all required policies stated");
    Configuration conf = createAssumedRoleConfig();

    bindRolePolicy(conf,
        policy(
            statement(false, S3_ALL_BUCKETS, S3_GET_OBJECT_TORRENT),
            ALLOW_S3_GET_BUCKET_LOCATION, STATEMENT_ALLOW_KMS_RW));
    Path path = path("testAssumeRoleStillIncludesRolePerms");
    roleFS = (S3AFileSystem) path.getFileSystem(conf);
    assertTouchForbidden(roleFS, path);
  }

  /**
   * After blocking all write verbs used by S3A, try to write data (fail)
   * and read data (succeed).
   * SSE-KMS key access is set to decrypt only.
   */
  @Test
  public void testReadOnlyOperations() throws Throwable {

    describe("Restrict role to read only");
    Configuration conf = createAssumedRoleConfig();

    bindRolePolicy(conf,
        policy(
            statement(false, S3_ALL_BUCKETS, S3_PATH_WRITE_OPERATIONS),
            STATEMENT_ALL_S3, STATEMENT_ALLOW_KMS_RW));
    Path path = methodPath();
    roleFS = (S3AFileSystem) path.getFileSystem(conf);
    // list the root path, expect happy
    roleFS.listStatus(ROOT);

    // touch will fail
    assertTouchForbidden(roleFS, path);
    // you can delete it, because it's not there and getFileStatus() is allowed
    roleFS.delete(path, true);

    //create it with the full FS
    getFileSystem().mkdirs(path);

    // and delete will not
    assertDeleteForbidden(this.roleFS, path);

    // list multipart uploads.
    // This is part of the read policy.
    int counter = 0;
    MultipartUtils.UploadIterator iterator = roleFS.listUploads("/");
    while (iterator.hasNext()) {
      counter++;
      iterator.next();
    }
    LOG.info("Found {} outstanding MPUs", counter);
  }

  /**
   * Write successfully to the directory with full R/W access,
   * fail to write or delete data elsewhere.
   */
  @SuppressWarnings("StringConcatenationMissingWhitespace")
  @Test
  public void testRestrictedWriteSubdir() throws Throwable {

    describe("Attempt writing to paths where a role only has"
        + " write access to a subdir of the bucket");
    Path restrictedDir = methodPath();
    Path child = new Path(restrictedDir, "child");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(restrictedDir, true);

    Configuration conf = createAssumedRoleConfig();

    bindRolePolicyStatements(conf,
        STATEMENT_ALL_BUCKET_READ_ACCESS, STATEMENT_ALLOW_KMS_RW,
        new Statement(Effects.Allow)
          .addActions(S3_ALL_OPERATIONS)
          .addResources(directory(restrictedDir)));
    roleFS = (S3AFileSystem) restrictedDir.getFileSystem(conf);

    roleFS.getFileStatus(ROOT);
    roleFS.mkdirs(restrictedDir);
    assertIsDirectory(restrictedDir);
    // you can create an adjacent child
    touch(roleFS, child);
    assertIsFile(child);
    // child delete rights
    ContractTestUtils.assertDeleted(roleFS, child, true);
    // parent delete rights
    ContractTestUtils.assertDeleted(roleFS, restrictedDir, true);
    // delete will try to create an empty parent directory marker, and may fail
    roleFS.delete(restrictedDir, false);
    // this sibling path has the same prefix as restrictedDir, but is
    // adjacent. This verifies that a restrictedDir* pattern isn't matching
    // siblings, so granting broader rights
    Path sibling = new Path(restrictedDir.toUri() + "sibling");
    touch(fs, sibling);
    assertTouchForbidden(roleFS, sibling);
    assertDeleteForbidden(roleFS, sibling);
  }

  public Path methodPath() throws IOException {
    return path(getMethodName());
  }

  /**
   * Without simulation of STS failures, and with STS overload likely to
   * be very rare, there'll be no implicit test coverage of
   * {@link AssumedRoleCredentialProvider#operationRetried(String, Exception, int, boolean)}.
   * This test simply invokes the callback for both the first and second retry event.
   *
   * If the handler ever adds more than logging, this test ensures that things
   * don't break.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testAssumedRoleRetryHandler() throws Throwable {
    try(AssumedRoleCredentialProvider provider
            = new AssumedRoleCredentialProvider(getFileSystem().getUri(),
        createAssumedRoleConfig())) {
      provider.operationRetried("retry", new IOException("failure"), 0, true);
      provider.operationRetried("retry", new IOException("failure"), 1, true);
    }
  }

  @Test
  public void testRestrictedCommitActions() throws Throwable {
    describe("Attempt commit operations against a path with restricted rights");
    Configuration conf = createAssumedRoleConfig();
    final int uploadPartSize = 5 * 1024 * 1024;

    ProgressCounter progress = new ProgressCounter();
    progress.assertCount("Progress counter should be zero", 0);
    Path basePath = methodPath();
    Path readOnlyDir = new Path(basePath, "readOnlyDir");
    Path writeableDir = new Path(basePath, "writeableDir");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);
    fs.mkdirs(readOnlyDir);

    bindRolePolicyStatements(conf, STATEMENT_ALLOW_KMS_RW,
        STATEMENT_ALL_BUCKET_READ_ACCESS,
        new Statement(Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(writeableDir))
    );
    roleFS = (S3AFileSystem) writeableDir.getFileSystem(conf);
    CommitterStatistics committerStatistics = fs.newCommitterStatistics();
    CommitOperations fullOperations = new CommitOperations(fs,
        committerStatistics, "/");
    CommitOperations operations = new CommitOperations(roleFS,
        committerStatistics, "/");

    File localSrc = File.createTempFile("source", "");
    writeCSVData(localSrc);
    Path uploadDest = new Path(readOnlyDir, "restricted.csv");

    forbidden("initiate MultiPartUpload",
        () -> {
          return operations.uploadFileToPendingCommit(localSrc,
              uploadDest, "", uploadPartSize, progress);
        });
    progress.assertCount("progress counter not expected.", 0);
    // delete the file
    localSrc.delete();
    // create a directory there
    localSrc.mkdirs();

    // create some local files and upload them with permissions

    int range = 2;
    IntStream.rangeClosed(1, range)
        .parallel()
        .forEach((i) -> eval(() -> {
          String name = "part-000" + i;
          File src = new File(localSrc, name);
          Path dest = new Path(readOnlyDir, name);
          writeCSVData(src);
          SinglePendingCommit pending =
              fullOperations.uploadFileToPendingCommit(src, dest, "",
                  uploadPartSize, progress);
          pending.save(fs,
              new Path(readOnlyDir, name + CommitConstants.PENDING_SUFFIX),
              SinglePendingCommit.serializer());
          assertTrue(src.delete());
        }));
    progress.assertCount("progress counter is not expected",
        range);

    try(CommitContext commitContext =
            operations.createCommitContextForTesting(uploadDest,
                null, 0)) {
      // we expect to be able to list all the files here
      Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>>
          pendingCommits = operations.loadSinglePendingCommits(readOnlyDir,
          true, commitContext);

      // all those commits must fail
      List<SinglePendingCommit> commits = pendingCommits.getLeft().getCommits();
      assertEquals(range, commits.size());
      commits.parallelStream().forEach(
          (c) -> {
            CommitOperations.MaybeIOE maybeIOE =
                commitContext.commit(c, "origin");
            Path path = c.destinationPath();
            assertCommitAccessDenied(path, maybeIOE);
          });

      // fail of all list and abort of .pending files.
      LOG.info("abortAllSinglePendingCommits({})", readOnlyDir);
      assertCommitAccessDenied(readOnlyDir,
          operations.abortAllSinglePendingCommits(readOnlyDir, commitContext, true));

      // try writing a magic file
      Path magicDestPath = new Path(readOnlyDir,
          CommitConstants.MAGIC + "/" + "magic.txt");
      forbidden("", () -> {
        touch(roleFS, magicDestPath);
        // shouldn't get here; if we do: return the existence of the 0-byte
        // dest file.
        return fs.getFileStatus(magicDestPath);
      });

      // a recursive list and abort is blocked.
      forbidden("",
          () -> operations.abortPendingUploadsUnderPath(readOnlyDir));
    } finally {
      LOG.info("Cleanup");
      fullOperations.abortPendingUploadsUnderPath(readOnlyDir);
      LOG.info("Committer statistics {}",
          ioStatisticsSourceToString(committerStatistics));
    }
  }

  /**
   * Verifies that an operation returning a "MaybeIOE" failed
   * with an AccessDeniedException in the maybe instance.
   * @param path path operated on
   * @param maybeIOE result to inspect
   */
  public void assertCommitAccessDenied(final Path path,
      final CommitOperations.MaybeIOE maybeIOE) {
    IOException ex = maybeIOE.getException();
    assertNotNull("no IOE in " + maybeIOE + " for " + path, ex);
    if (!(ex instanceof AccessDeniedException)) {
      ContractTestUtils.fail("Wrong exception class for commit to "
          + path, ex);
    }
  }

  /**
   * Write some CSV data to a local file.
   * @param localSrc local file
   * @throws IOException failure
   */
  public void writeCSVData(final File localSrc) throws IOException {
    try(FileOutputStream fo = new FileOutputStream(localSrc)) {
      fo.write("1, true".getBytes());
    }
  }

  @Test
  public void testPartialDelete() throws Throwable {
    describe("delete with part of the child tree read only; multidelete");
    executePartialDelete(createAssumedRoleConfig(), false);
  }

  @Test
  public void testPartialDeleteSingleDelete() throws Throwable {
    describe("delete with part of the child tree read only");
    executePartialDelete(createAssumedRoleConfig(), true);
  }

  /**
   * Have a directory with full R/W permissions, but then remove
   * write access underneath, and try to delete it.
   * @param conf FS configuration
   * @param singleDelete flag to indicate this is a single delete operation
   */
  public void executePartialDelete(final Configuration conf,
      final boolean singleDelete)
      throws Exception {
    conf.setBoolean(ENABLE_MULTI_DELETE, !singleDelete);
    Path destDir = methodPath();
    Path readOnlyDir = new Path(destDir, "readonlyDir");

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(destDir, true);

    bindRolePolicyStatements(conf, STATEMENT_ALLOW_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_ALL_OPERATIONS),
        new Statement(Effects.Deny)
            .addActions(S3_PATH_WRITE_OPERATIONS)
            .addResources(directory(readOnlyDir))
    );
    roleFS = (S3AFileSystem) destDir.getFileSystem(conf);

    int range = 10;
    touchFiles(fs, readOnlyDir, range);
    touchFiles(roleFS, destDir, range);
    forbidden("", () -> roleFS.delete(readOnlyDir, true));
    forbidden("", () -> roleFS.delete(destDir, true));

    // and although you can't delete under the path, if the file doesn't
    // exist, the delete call fails fast.
    Path pathWhichDoesntExist = new Path(readOnlyDir, "no-such-path");
    assertFalse("deleting " + pathWhichDoesntExist,
        roleFS.delete(pathWhichDoesntExist, true));
  }

  /**
   * Block access to bucket locations and verify that {@code getBucketLocation}
   * fails -but that the bucket-info command recovers from this.
   */
  @Test
  public void testBucketLocationForbidden() throws Throwable {

    describe("Restrict role to read only");
    Configuration conf = createAssumedRoleConfig();

    bindRolePolicyStatements(conf, STATEMENT_ALLOW_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_ALL_OPERATIONS),
        statement(false, S3_ALL_BUCKETS, S3_GET_BUCKET_LOCATION));
    Path path = methodPath();
    roleFS = (S3AFileSystem) path.getFileSystem(conf);
    forbidden("",
        () -> roleFS.getBucketLocation());
    S3GuardTool.BucketInfo infocmd = new S3GuardTool.BucketInfo(conf);
    URI fsUri = getFileSystem().getUri();
    String info = exec(infocmd, S3GuardTool.BucketInfo.NAME,
        fsUri.toString());
    Assertions.assertThat(info)
        .contains(S3GuardTool.BucketInfo.LOCATION_UNKNOWN);
  }
}
