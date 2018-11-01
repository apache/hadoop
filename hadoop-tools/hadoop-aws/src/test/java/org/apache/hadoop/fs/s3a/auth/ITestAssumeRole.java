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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException;
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
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.CommitOperations;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.*;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.*;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.forbidden;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Tests use of assumed roles.
 * Only run if an assumed role is provided.
 */
@SuppressWarnings({"IOResourceOpenedButNotSafelyClosed", "ThrowableNotThrown"})
public class ITestAssumeRole extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAssumeRole.class);

  private static final Path ROOT = new Path("/");

  /**
   * test URI, built in setup.
   */
  private URI uri;

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
    uri = new URI(S3ATestConstants.DEFAULT_CSVTEST_FILE);
  }

  @Override
  public void teardown() throws Exception {
    S3AUtils.closeAll(LOG, roleFS);
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
   * @throws Exception if the exception was the wrong class
   */
  private <E extends Throwable> void expectFileSystemCreateFailure(
      Configuration conf,
      Class<E> clazz,
      String text) throws Exception {
    interceptClosing(clazz,
        text,
        () -> new Path(getFileSystem().getUri()).getFileSystem(conf));
  }

  @Test
  public void testCreateCredentialProvider() throws IOException {
    describe("Create the credential provider");

    String roleARN = getAssumedRoleARN();

    Configuration conf = new Configuration(getContract().getConf());
    conf.set(AWS_CREDENTIALS_PROVIDER, AssumedRoleCredentialProvider.NAME);
    conf.set(ASSUMED_ROLE_ARN, roleARN);
    conf.set(ASSUMED_ROLE_SESSION_NAME, "valid");
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "45m");
    bindRolePolicy(conf, RESTRICTED_POLICY);
    try (AssumedRoleCredentialProvider provider
             = new AssumedRoleCredentialProvider(uri, conf)) {
      LOG.info("Provider is {}", provider);
      AWSCredentials credentials = provider.getCredentials();
      assertNotNull("Null credentials from " + provider, credentials);
    }
  }

  @Test
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
  public void testAssumeRoleCannotAuthAssumedRole() throws Exception {
    describe("Assert that you can't use assumed roles to auth assumed roles");

    Configuration conf = createAssumedRoleConfig();
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        AssumedRoleCredentialProvider.NAME);
    expectFileSystemCreateFailure(conf,
        IOException.class,
        AssumedRoleCredentialProvider.E_FORBIDDEN_PROVIDER);
  }

  @Test
  public void testAssumeRoleBadInnerAuth() throws Exception {
    describe("Try to authenticate with a keypair with spaces");

    Configuration conf = createAssumedRoleConfig();
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.NAME);
    conf.set(ACCESS_KEY, "not valid");
    conf.set(SECRET_KEY, "not secret");
    expectFileSystemCreateFailure(conf,
        AWSBadRequestException.class,
        "not a valid " +
        "key=value pair (missing equal-sign) in Authorization header");
  }

  @Test
  public void testAssumeRoleBadInnerAuth2() throws Exception {
    describe("Try to authenticate with an invalid keypair");

    Configuration conf = createAssumedRoleConfig();
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.NAME);
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
  public void testAssumedIllegalDuration() throws Throwable {
    describe("Expect the constructor to fail if the session is to short");
    Configuration conf = new Configuration();
    conf.set(ASSUMED_ROLE_SESSION_DURATION, "30s");
    interceptClosing(IllegalArgumentException.class, "",
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
      fs.getFileStatus(new Path("/"));
      fs.mkdirs(path("testAssumeRoleFS"));
    }
  }

  @Test
  public void testAssumeRoleRestrictedPolicyFS() throws Exception {
    describe("Restrict the policy for this session; verify that reads fail");

    Configuration conf = createAssumedRoleConfig();
    bindRolePolicy(conf, RESTRICTED_POLICY);
    Path path = new Path(getFileSystem().getUri());
    try (FileSystem fs = path.getFileSystem(conf)) {
      forbidden("getFileStatus",
          () -> fs.getFileStatus(new Path("/")));
      forbidden("getFileStatus",
          () -> fs.listStatus(new Path("/")));
      forbidden("getFileStatus",
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
        policy(statement(false, S3_ALL_BUCKETS, S3_GET_OBJECT_TORRENT)));
    Path path = path("testAssumeRoleStillIncludesRolePerms");
    roleFS = (S3AFileSystem) path.getFileSystem(conf);
    assertTouchForbidden(roleFS, path);
  }

  /**
   * After blocking all write verbs used by S3A, try to write data (fail)
   * and read data (succeed).
   */
  @Test
  public void testReadOnlyOperations() throws Throwable {

    describe("Restrict role to read only");
    Configuration conf = createAssumedRoleConfig();

    bindRolePolicy(conf,
        policy(
            statement(false, S3_ALL_BUCKETS, S3_PATH_WRITE_OPERATIONS),
            STATEMENT_ALL_S3, STATEMENT_ALL_DDB));
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
        STATEMENT_ALL_DDB,
        statement(true, S3_ALL_BUCKETS, S3_ROOT_READ_OPERATIONS),
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

  @Test
  public void testRestrictedRename() throws Throwable {
    describe("rename with parent paths not writeable");
    executeRestrictedRename(createAssumedRoleConfig());
  }

  @Test
  public void testRestrictedSingleDeleteRename() throws Throwable {
    describe("rename with parent paths not writeable"
        + " and multi-object delete disabled");
    Configuration conf = createAssumedRoleConfig();
    conf.setBoolean(ENABLE_MULTI_DELETE, false);
    executeRestrictedRename(conf);
  }

  /**
   * Execute a sequence of rename operations.
   * @param conf FS configuration
   */
  public void executeRestrictedRename(final Configuration conf)
      throws IOException {
    Path basePath = methodPath();
    Path restrictedDir = new Path(basePath, "renameSrc");
    Path destPath = new Path(basePath, "renameDest");
    Path child = new Path(restrictedDir, "child");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);

    bindRolePolicyStatements(conf,
        STATEMENT_ALL_DDB,
        statement(true, S3_ALL_BUCKETS, S3_ROOT_READ_OPERATIONS),
        new Statement(Effects.Allow)
          .addActions(S3_PATH_RW_OPERATIONS)
          .addResources(directory(restrictedDir))
          .addResources(directory(destPath))
    );
    roleFS = (S3AFileSystem) restrictedDir.getFileSystem(conf);

    roleFS.getFileStatus(ROOT);
    roleFS.mkdirs(restrictedDir);
    // you can create an adjacent child
    touch(roleFS, child);

    roleFS.delete(destPath, true);
    // as dest doesn't exist, this will map child -> dest
    assertRenameOutcome(roleFS, child, destPath, true);

    assertIsFile(destPath);
    assertIsDirectory(restrictedDir);
    Path renamedDestPath = new Path(restrictedDir, destPath.getName());
    assertRenameOutcome(roleFS, destPath, restrictedDir, true);
    assertIsFile(renamedDestPath);
    roleFS.delete(restrictedDir, true);
    roleFS.delete(destPath, true);
  }

  @Test
  public void testRestrictedRenameReadOnlyData() throws Throwable {
    describe("rename with source read only, multidelete");
    executeRenameReadOnlyData(createAssumedRoleConfig());
  }

  @Test
  public void testRestrictedRenameReadOnlySingleDelete() throws Throwable {
    describe("rename with source read only single delete");
    Configuration conf = createAssumedRoleConfig();
    conf.setBoolean(ENABLE_MULTI_DELETE, false);
    executeRenameReadOnlyData(conf);
  }

  /**
   * Execute a sequence of rename operations where the source
   * data is read only to the client calling rename().
   * This will cause the inner delete() operations to fail, whose outcomes
   * are explored.
   * Multiple files are created (in parallel) for some renames, so exploring
   * the outcome on bulk delete calls, including verifying that a
   * MultiObjectDeleteException is translated to an AccessDeniedException.
   * <ol>
   *   <li>The exception raised is AccessDeniedException,
   *   from single and multi DELETE calls.</li>
   *   <li>It happens after the COPY. Not ideal, but, well, we can't pretend
   *   it's a filesystem forever.</li>
   * </ol>
   * @param conf FS configuration
   */
  public void executeRenameReadOnlyData(final Configuration conf)
      throws Exception {
    assume("Does not work with S3Guard", !getFileSystem().hasMetadataStore());
    Path basePath = methodPath();
    Path destDir = new Path(basePath, "renameDest");
    Path readOnlyDir = new Path(basePath, "readonlyDir");
    Path readOnlyFile = new Path(readOnlyDir, "readonlyChild");

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);

    // this file is readable by the roleFS, but cannot be deleted
    touch(fs, readOnlyFile);

    bindRolePolicyStatements(conf,
        STATEMENT_ALL_DDB,
        statement(true, S3_ALL_BUCKETS, S3_ROOT_READ_OPERATIONS),
          new Statement(Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(destDir))
    );
    roleFS = (S3AFileSystem) destDir.getFileSystem(conf);

    roleFS.delete(destDir, true);
    roleFS.mkdirs(destDir);
    // rename will fail in the delete phase
    forbidden(readOnlyFile.toString(),
        () -> roleFS.rename(readOnlyFile, destDir));

    // and the source file is still there
    assertIsFile(readOnlyFile);

    // but so is the copied version, because there's no attempt
    // at rollback, or preflight checking on the delete permissions
    Path renamedFile = new Path(destDir, readOnlyFile.getName());

    assertIsFile(renamedFile);

    ContractTestUtils.assertDeleted(roleFS, renamedFile, true);
    assertFileCount("Empty Dest Dir", roleFS,
        destDir, 0);
    // create a set of files
    // this is done in parallel as it is 10x faster on a long-haul test run.
    int range = 10;
    touchFiles(fs, readOnlyDir, range);
    // don't forget about that original file!
    final long createdFiles = range + 1;
    // are they all there?
    assertFileCount("files ready to rename", roleFS,
        readOnlyDir, createdFiles);

    // try to rename the directory
    LOG.info("Renaming readonly files {} to {}", readOnlyDir, destDir);
    AccessDeniedException ex = forbidden("",
        () -> roleFS.rename(readOnlyDir, destDir));
    LOG.info("Result of renaming read-only files is AccessDeniedException", ex);
    assertFileCount("files copied to the destination", roleFS,
        destDir, createdFiles);
    assertFileCount("files in the source directory", roleFS,
        readOnlyDir, createdFiles);

    // and finally (so as to avoid the delay of POSTing some more objects,
    // delete that r/o source
    forbidden("", () -> roleFS.delete(readOnlyDir, true));
  }

  /**
   * Parallel-touch a set of files in the destination directory.
   * @param fs filesystem
   * @param destDir destination
   * @param range range 1..range inclusive of files to create.
   */
  public void touchFiles(final S3AFileSystem fs,
      final Path destDir,
      final int range) {
    IntStream.rangeClosed(1, range).parallel().forEach(
        (i) -> eval(() -> touch(fs, new Path(destDir, "file-" + i))));
  }

  @Test
  public void testRestrictedCommitActions() throws Throwable {
    describe("Attempt commit operations against a path with restricted rights");
    Configuration conf = createAssumedRoleConfig();
    conf.setBoolean(CommitConstants.MAGIC_COMMITTER_ENABLED, true);
    final int uploadPartSize = 5 * 1024 * 1024;

    Path basePath = methodPath();
    Path readOnlyDir = new Path(basePath, "readOnlyDir");
    Path writeableDir = new Path(basePath, "writeableDir");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);
    fs.mkdirs(readOnlyDir);

    bindRolePolicyStatements(conf,
        STATEMENT_ALL_DDB,
        statement(true, S3_ALL_BUCKETS, S3_ROOT_READ_OPERATIONS),
        new Statement(Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(writeableDir))
    );
    roleFS = (S3AFileSystem) writeableDir.getFileSystem(conf);
    CommitOperations fullOperations = new CommitOperations(fs);
    CommitOperations operations = new CommitOperations(roleFS);

    File localSrc = File.createTempFile("source", "");
    writeCSVData(localSrc);
    Path uploadDest = new Path(readOnlyDir, "restricted.csv");

    forbidden("initiate MultiPartUpload",
        () -> {
          return operations.uploadFileToPendingCommit(localSrc,
              uploadDest, "", uploadPartSize);
        });
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
                  uploadPartSize);
          pending.save(fs, new Path(readOnlyDir,
              name + CommitConstants.PENDING_SUFFIX), true);
          assertTrue(src.delete());
        }));

    try {
      // we expect to be able to list all the files here
      Pair<PendingSet, List<Pair<LocatedFileStatus, IOException>>>
          pendingCommits = operations.loadSinglePendingCommits(readOnlyDir,
          true);

      // all those commits must fail
      List<SinglePendingCommit> commits = pendingCommits.getLeft().getCommits();
      assertEquals(range, commits.size());
      commits.parallelStream().forEach(
          (c) -> {
            CommitOperations.MaybeIOE maybeIOE = operations.commit(c, "origin");
            Path path = c.destinationPath();
            assertCommitAccessDenied(path, maybeIOE);
          });

      // fail of all list and abort of .pending files.
      LOG.info("abortAllSinglePendingCommits({})", readOnlyDir);
      assertCommitAccessDenied(readOnlyDir,
          operations.abortAllSinglePendingCommits(readOnlyDir, true));

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
    executePartialDelete(createAssumedRoleConfig());
  }

  @Test
  public void testPartialDeleteSingleDelete() throws Throwable {
    describe("delete with part of the child tree read only");
    Configuration conf = createAssumedRoleConfig();
    conf.setBoolean(ENABLE_MULTI_DELETE, false);
    executePartialDelete(conf);
  }

  /**
   * Have a directory with full R/W permissions, but then remove
   * write access underneath, and try to delete it.
   * @param conf FS configuration
   */
  public void executePartialDelete(final Configuration conf)
      throws Exception {
    Path destDir = methodPath();
    Path readOnlyDir = new Path(destDir, "readonlyDir");

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(destDir, true);

    bindRolePolicyStatements(conf,
        STATEMENT_ALL_DDB,
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
   * Assert that the number of files in a destination matches that expected.
   * @param text text to use in the message
   * @param fs filesystem
   * @param path path to list (recursively)
   * @param expected expected count
   * @throws IOException IO problem
   */
  private static void assertFileCount(String text, FileSystem fs,
      Path path, long expected)
      throws IOException {
    List<String> files = new ArrayList<>();
    applyLocatedFiles(fs.listFiles(path, true),
        (status) -> files.add(status.getPath().toString()));
    long actual = files.size();
    if (actual != expected) {
      String ls = files.stream().collect(Collectors.joining("\n"));
      fail(text + ": expected " + expected + " files in " + path
          + " but got " + actual + "\n" + ls);
    }
  }
}
