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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.MetricDiff;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;
import static org.apache.hadoop.fs.s3a.Statistic.FILES_DELETE_REJECTED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Effects;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Statement;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.directory;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.statement;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.bindRolePolicyStatements;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.forbidden;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.newAssumedRoleConfig;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_BINDING;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.extractUndeletedPaths;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.removeUndeletedPaths;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.toPathList;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertFileCount;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.extractCause;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsSourceToString;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.test.GenericTestUtils.buildPaths;
import static org.apache.hadoop.test.LambdaTestUtils.eval;

/**
 * Test partial failures of delete and rename operations, especially
 * that the S3Guard tables are consistent with the state of
 * the filesystem.
 *
 * All these test have a unique path for each run, with a roleFS having
 * full RW access to part of it, and R/O access to a restricted subdirectory
 *
 * <ol>
 *   <li>
 *     The tests are parameterized to single/multi delete, which control which
 *     of the two delete mechanisms are used.
 *   </li>
 *   <li>
 *     In multi delete, in a scale test run, a significantly larger set of files
 *     is created and then deleted.
 *   </li>
 *   <li>
 *     This isn't done in the single delete as it is much slower and it is not
 *     the situation we are trying to create.
 *   </li>
 * </ol>
 *
 * This test manages to create lots of load on the s3guard prune command
 * when that is tested in a separate test suite;
 * too many tombstone files for the test to complete.
 * An attempt is made in {@link #deleteTestDirInTeardown()} to prune these test
 * files.
 */
@SuppressWarnings("ThrowableNotThrown")
@RunWith(Parameterized.class)
public class ITestPartialRenamesDeletes extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestPartialRenamesDeletes.class);

  private static final Path ROOT = new Path("/");

  private static final Statement STATEMENT_ALL_BUCKET_READ_ACCESS
      = statement(true, S3_ALL_BUCKETS, S3_BUCKET_READ_OPERATIONS);

  /** Many threads for scale performance: {@value}. */
  public static final int EXECUTOR_THREAD_COUNT = 64;

  /**
   * For submitting work.
   */
  private static final ListeningExecutorService EXECUTOR =
      MoreExecutors.listeningDecorator(
          BlockingThreadPoolExecutorService.newInstance(
              EXECUTOR_THREAD_COUNT,
              EXECUTOR_THREAD_COUNT * 2,
              30, TimeUnit.SECONDS,
              "test-operations"));


  /**
   * The number of files in a non-scaled test.
   * <p>
   * Value: {@value}.
   */
  public static final int FILE_COUNT_NON_SCALED = 2;

  /**
   * The number of files for a scaled test. This is still
   * less than half the amount which can be fitted into a delete
   * request, so that even with this many R/W and R/O files,
   * both can fit in the same request.
   * Then, when a partial delete occurs, we can make assertions
   * knowing that all R/W files should have been deleted and all
   * R/O files rejected.
   * <p>
   * Value: {@value}.
   */
  public static final int FILE_COUNT_SCALED = 10;

  public static final int DIR_COUNT = 2;
  public static final int DIR_COUNT_SCALED = 4;
  public static final int DEPTH = 2;
  public static final int DEPTH_SCALED = 2;

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

  /**
   * Base path for this test run.
   * This is generated uniquely for each test.
   */
  private Path basePath;

  /**
   * A directory which restricted roles have full write access to.
   */
  private Path writableDir;

  /**
   * A directory to which restricted roles have only read access.
   */
  private Path readOnlyDir;

  /**
   * A file under {@link #readOnlyDir} which cannot be written or deleted.
   */
  private Path readOnlyChild;

  /**
   * A directory to which restricted roles have no read access.
   */
  private Path noReadDir;

  /** delete policy: single or multi? */
  private final boolean multiDelete;

  /**
   * Configuration for the assume role FS.
   */
  private Configuration assumedRoleConfig;

  private int fileCount;
  private int dirCount;
  private int dirDepth;

  /**
   * Was the -Dscale switch passed in to the test run?
   */
  private boolean scaleTest;

  /**
   * Test array for parameterized test runs.
   * <ul>
   *   <li>Run 0: single deletes</li>
   *   <li>Run 1: multi deletes</li>
   * </ul>
   *
   * @return a list of parameter tuples.
   */
  @Parameterized.Parameters(name = "bulk-delete={0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true},
    });
  }

  /**
   * Constructor.
   * @param multiDelete single vs multi delete in the role FS?
   */
  public ITestPartialRenamesDeletes(final boolean multiDelete) {
    this.multiDelete = multiDelete;
  }

  /**
   * This sets up a unique path for every test run, so as to guarantee isolation
   * from previous runs.
   * It creates a role policy which has read access to everything except
   * the contents of {@link #noReadDir}, and with write access to
   * {@link #writableDir}.
   * @throws Exception failure
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
    basePath = uniquePath();
    readOnlyDir = new Path(basePath, "readonlyDir");
    writableDir = new Path(basePath, "writableDir");
    readOnlyChild = new Path(readOnlyDir, "child");
    noReadDir = new Path(basePath, "noReadDir");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);
    fs.mkdirs(writableDir);

    // create the baseline assumed role
    assumedRoleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(assumedRoleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
        STATEMENT_ALL_BUCKET_READ_ACCESS,  // root:     r-x
        new Statement(Effects.Allow)       // dest:     rwx
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(writableDir)),
        new Statement(Effects.Deny)       // noReadDir: --x
            .addActions(S3_ALL_GET)
            .addActions(S3_ALL_PUT)
            .addActions(S3_ALL_DELETE)
            .addResources(directory(noReadDir)));
    // the role configured to that set of restrictions
    roleFS = (S3AFileSystem) readOnlyDir.getFileSystem(assumedRoleConfig);

    // switch to the big set of files iff this is a multidelete run
    // with -Dscale set.
    // without that the DELETE calls become a key part of the bottleneck
    scaleTest = multiDelete && getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    fileCount = scaleTest ? FILE_COUNT_SCALED : FILE_COUNT_NON_SCALED;
    dirCount = scaleTest ? DIR_COUNT_SCALED : DIR_COUNT;
    dirDepth = scaleTest ? DEPTH_SCALED : DEPTH;
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
   * Create the assumed role configuration.
   * @return a config bonded to the ARN of the assumed role
   */
  public Configuration createAssumedRoleConfig() {
    return createAssumedRoleConfig(getAssumedRoleARN());
  }

  /**
   * Create a config for an assumed role; it also disables FS caching
   * and sets the multi delete option to that of the current mode.
   * @param roleARN ARN of role
   * @return the new configuration
   */
  private Configuration createAssumedRoleConfig(String roleARN) {
    Configuration conf = newAssumedRoleConfig(getContract().getConf(),
        roleARN);
    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING,
        ENABLE_MULTI_DELETE);
    conf.setBoolean(ENABLE_MULTI_DELETE, multiDelete);
    return conf;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    // ramp up the number of connections we can have for maximum PUT
    // performance
    removeBucketOverrides(bucketName, conf,
        MAX_THREADS,
        MAXIMUM_CONNECTIONS,
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        DIRECTORY_MARKER_POLICY,
        BULK_DELETE_PAGE_SIZE);
    conf.setInt(MAX_THREADS, EXECUTOR_THREAD_COUNT);
    conf.setInt(MAXIMUM_CONNECTIONS, EXECUTOR_THREAD_COUNT * 2);
    // turn off prune delays, so as to stop scale tests creating
    // so much cruft that future CLI prune commands take forever
    conf.setInt(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, 0);
    // use the keep policy to ensure that surplus markers exist
    // to complicate failures
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_KEEP);
    // set the delete page size to its maximum to ensure that all
    // entries are included in the same large delete, even on
    // scale runs. This is needed for assertions on the result.
    conf.setInt(BULK_DELETE_PAGE_SIZE, 1_000);
    return conf;
  }

  /**
   * Create a unique path, which includes method name,
   * multidelete flag and a timestamp.
   * @return a string to use for paths.
   * @throws IOException path creation failure.
   */
  private Path uniquePath() throws IOException {
    long now = System.currentTimeMillis();
    return path(
        String.format("%s-%s-%06d.%03d",
            getMethodName(),
            multiDelete ? "multi" : "single",
            now / 1000, now % 1000));
  }

  /**
   * This is here to verify role and path setup.
   */
  @Test
  public void testCannotTouchUnderRODir() throws Throwable {
    forbidden("touching the empty child " + readOnlyChild,
        "",
        () -> {
          touch(roleFS, readOnlyChild);
          return readOnlyChild;
        });
  }
  @Test
  public void testCannotReadUnderNoReadDir() throws Throwable {
    Path path = new Path(noReadDir, "unreadable.txt");
    createFile(getFileSystem(), path, true, "readonly".getBytes());
    forbidden("trying to read " + path,
        "",
        () -> readUTF8(roleFS, path, -1));
  }

  @Test
  public void testMultiDeleteOptionPropagated() throws Throwable {
    describe("Verify the test parameter propagates to the store context");
    StoreContext ctx = roleFS.createStoreContext();
    Assertions.assertThat(ctx.isMultiObjectDeleteEnabled())
        .as(ctx.toString())
        .isEqualTo(multiDelete);
  }

  /**
   * Execute a sequence of rename operations with access locked down.
   */
  @Test
  public void testRenameParentPathNotWriteable() throws Throwable {
    describe("rename with parent paths not writeable; multi=%s", multiDelete);
    final Configuration conf = createAssumedRoleConfig();
    bindRolePolicyStatements(conf,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
        STATEMENT_ALL_BUCKET_READ_ACCESS,
        new Statement(Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(readOnlyDir))
            .addResources(directory(writableDir)));
    roleFS = (S3AFileSystem) readOnlyDir.getFileSystem(conf);

    S3AFileSystem fs = getFileSystem();
    roleFS.getFileStatus(ROOT);
    fs.mkdirs(readOnlyDir);
    // you can create an adjacent child
    touch(fs, readOnlyChild);

    fs.delete(writableDir, true);
    // as dest doesn't exist, this will map child -> dest
    assertRenameOutcome(roleFS, readOnlyChild, writableDir, true);

    assertIsFile(writableDir);
    assertIsDirectory(readOnlyDir);
    Path renamedDestPath = new Path(readOnlyDir, writableDir.getName());
    assertRenameOutcome(roleFS, writableDir, readOnlyDir, true);
    assertIsFile(renamedDestPath);
    roleFS.delete(readOnlyDir, true);
    roleFS.delete(writableDir, true);
  }

  @Test
  public void testRenameSingleFileFailsInDelete() throws Throwable {
    describe("rename with source read only; multi=%s", multiDelete);
    Path readOnlyFile = readOnlyChild;

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);

    // this file is readable by the roleFS, but cannot be deleted
    touch(fs, readOnlyFile);

    roleFS.delete(writableDir, true);
    roleFS.mkdirs(writableDir);
    // rename will fail in the delete phase
    expectRenameForbidden(readOnlyFile, writableDir);

    // and the source file is still there
    assertIsFile(readOnlyFile);

    // and so is the copied version, because there's no attempt
    // at rollback, or preflight checking on the delete permissions
    Path renamedFile = new Path(writableDir, readOnlyFile.getName());

    assertIsFile(renamedFile);

    ContractTestUtils.assertDeleted(roleFS, renamedFile, true);
    assertFileCount("Empty Dest Dir", roleFS,
        writableDir, 0);
  }

  /**
   * Execute a sequence of rename operations where the source
   * data is read only to the client calling rename().
   * This will cause the inner delete() operations to fail, whose outcomes
   * are explored.
   * Multiple files are created (in parallel) for some renames, so the test
   * explores the outcome on bulk delete calls, including verifying that a
   * MultiObjectDeleteException is translated to an AccessDeniedException.
   * <ol>
   *   <li>The exception raised is AccessDeniedException,
   *   from single and multi DELETE calls.</li>
   *   <li>It happens after the COPY. Not ideal, but, well, we can't pretend
   *   it's a filesystem forever.</li>
   * </ol>
   */
  @Test
  public void testRenameDirFailsInDelete() throws Throwable {
    describe("rename with source read only; multi=%s", multiDelete);

    // the full FS
    S3AFileSystem fs = getFileSystem();

    roleFS.mkdirs(writableDir);

    // create a set of files
    // this is done in parallel as it is 10x faster on a long-haul test run.
    List<Path> dirs = new ArrayList<>(dirCount);
    List<Path> createdFiles = createDirsAndFiles(fs, readOnlyDir, dirDepth,
        fileCount, dirCount,
        new ArrayList<>(fileCount),
        dirs);
    // are they all there?
    int expectedFileCount = createdFiles.size();
    assertFileCount("files ready to rename", roleFS,
        readOnlyDir, expectedFileCount);

    // try to rename the directory
    LOG.info("Renaming readonly files {} to {}", readOnlyDir, writableDir);

    AccessDeniedException deniedException = expectRenameForbidden(readOnlyDir,
        writableDir);
    if (multiDelete) {
      // look in that exception for a multidelete
      MultiObjectDeleteException mde = extractCause(
          MultiObjectDeleteException.class, deniedException);
      final List<Path> undeleted
          = extractUndeletedPaths(mde, fs::keyToQualifiedPath);

      List<Path> expectedUndeletedFiles = new ArrayList<>(createdFiles);
      if (getFileSystem().getDirectoryMarkerPolicy()
          .keepDirectoryMarkers(readOnlyDir)) {
        // directory markers are being retained,
        // so will also be in the list of undeleted files
        expectedUndeletedFiles.addAll(dirs);
      }
      Assertions.assertThat(undeleted)
          .as("files which could not be deleted")
          .containsExactlyInAnyOrderElementsOf(expectedUndeletedFiles);
    }
    LOG.info("Result of renaming read-only files is as expected",
        deniedException);
    assertFileCount("files in the source directory", roleFS,
        readOnlyDir, expectedFileCount);
    // now lets look at the destination.
    // even with S3Guard on, we expect the destination to match that of
    // the remote state.
    // the test will exist
    describe("Verify destination directory exists");
    assertIsDirectory(writableDir);
    assertFileCount("files in the dest directory", roleFS,
        writableDir, expectedFileCount);
    // all directories in the source tree must still exist,
    // which for S3Guard means no tombstone markers were added
    LOG.info("Verifying all directories still exist");
    for (Path dir : dirs) {
      assertIsDirectory(dir);
    }
  }

  @Test
  public void testRenameFileFailsNoWrite() throws Throwable {
    describe("Try to rename to a write-only destination fails with src"
        + " & dest unchanged.");
    roleFS.mkdirs(writableDir);
    S3AFileSystem fs = getFileSystem();
    Path source = new Path(writableDir, "source");
    touch(fs, source);
    fs.mkdirs(readOnlyDir);
    Path dest = new Path(readOnlyDir, "dest");
    describe("Renaming files {} to {}", writableDir, dest);
    // rename fails but doesn't raise an exception. Good or bad?
    expectRenameForbidden(source, dest);
    assertIsFile(source);
    assertPathDoesNotExist("rename destination", dest);
  }

  @Test
  public void testCopyDirFailsToReadOnlyDir() throws Throwable {
    describe("Try to copy to a read-only destination");
    roleFS.mkdirs(writableDir);
    S3AFileSystem fs = getFileSystem();
    List<Path> files = createFiles(fs, writableDir, dirDepth, fileCount,
        dirCount);

    fs.mkdirs(readOnlyDir);
    Path dest = new Path(readOnlyDir, "dest");
    expectRenameForbidden(writableDir, dest);
    assertPathDoesNotExist("rename destination", dest);
    assertFileCount("files in the source directory", roleFS,
        writableDir, files.size());
  }

  @Test
  public void testCopyFileFailsOnSourceRead() throws Throwable {
    describe("The source file isn't readable, so the COPY fails");
    Path source = new Path(noReadDir, "source");
    S3AFileSystem fs = getFileSystem();
    touch(fs, source);
    fs.mkdirs(writableDir);
    Path dest = new Path(writableDir, "dest");
    expectRenameForbidden(source, dest);
    assertIsFile(source);
    assertPathDoesNotExist("rename destination", dest);
  }

  @Test
  public void testCopyDirFailsOnSourceRead() throws Throwable {
    describe("The source file isn't readable, so the COPY fails");
    S3AFileSystem fs = getFileSystem();
    List<Path> files = createFiles(fs, noReadDir, dirDepth, fileCount,
        dirCount);
    fs.mkdirs(writableDir);
    Path dest = new Path(writableDir, "dest");
    expectRenameForbidden(noReadDir, dest);
    assertFileCount("files in the source directory", fs,
        noReadDir, files.size());
  }

  /**
   * Have a directory with full R/W permissions, but then remove
   * write access underneath, and try to delete it.
   * This verifies that failures in the delete fake dir stage.
   * are not visible.
   */
  @Test
  public void testPartialEmptyDirDelete() throws Throwable {
    describe("delete an empty directory with parent dir r/o"
        + " multidelete=%s", multiDelete);

    // the full FS
    final Path deletableChild = new Path(writableDir, "deletableChild");
    // deletable child is created.
    roleFS.mkdirs(deletableChild);
    assertPathExists("parent dir after create", writableDir);
    assertPathExists("grandparent dir after create", writableDir.getParent());
    // now delete it.
    roleFS.delete(deletableChild, true);
    assertPathExists("parent dir after deletion", writableDir);
    assertPathExists("grandparent dir after deletion", writableDir.getParent());
    assertPathDoesNotExist("deletable dir after deletion", deletableChild);
  }

  /**
   * Have a directory with full R/W permissions, but then remove
   * write access underneath, and try to delete it.
   */
  @Test
  public void testPartialDirDelete() throws Throwable {
    describe("delete with part of the child tree read only;"
            + " multidelete=%s", multiDelete);

    // the full FS
    S3AFileSystem fs = getFileSystem();
    StoreContext storeContext = fs.createStoreContext();

    List<Path> dirs = new ArrayList<>(dirCount);
    List<Path> readOnlyFiles = createDirsAndFiles(
        fs, readOnlyDir, dirDepth,
        fileCount, dirCount,
        new ArrayList<>(fileCount),
        dirs);
    List<Path> deletableFiles = createFiles(fs,
        writableDir, dirDepth, fileCount, dirCount);

    // as a safety check, verify that one of the deletable files can be deleted
    Path head = deletableFiles.remove(0);
    assertTrue("delete " + head + " failed",
        roleFS.delete(head, false));
    List<Path> allFiles = Stream.concat(
        readOnlyFiles.stream(),
        deletableFiles.stream())
        .collect(Collectors.toList());
    List<MultiObjectDeleteSupport.KeyPath> keyPaths = allFiles.stream()
        .map(path ->
            new MultiObjectDeleteSupport.KeyPath(
                storeContext.pathToKey(path),
                path,
                false))
        .collect(Collectors.toList());

    // this set can be deleted by the role FS
    MetricDiff rejectionCount = new MetricDiff(roleFS, FILES_DELETE_REJECTED);
    MetricDiff deleteVerbCount = new MetricDiff(roleFS, OBJECT_DELETE_REQUEST);
    MetricDiff bulkDeleteVerbCount = new MetricDiff(roleFS,
        OBJECT_BULK_DELETE_REQUEST);
    MetricDiff deleteObjectCount = new MetricDiff(roleFS,
        OBJECT_DELETE_OBJECTS);

    describe("Trying to delete read only directory");
    AccessDeniedException ex = expectDeleteForbidden(readOnlyDir);
    if (multiDelete) {
      // multi-delete status checks
      extractCause(MultiObjectDeleteException.class, ex);
      deleteVerbCount.assertDiffEquals("Wrong delete request count", 0);
      bulkDeleteVerbCount.assertDiffEquals("Wrong bulk delete request count",
          1);
      deleteObjectCount.assertDiffEquals("Number of keys in delete request",
          readOnlyFiles.size());
      rejectionCount.assertDiffEquals("Wrong rejection count",
          readOnlyFiles.size());
      reset(rejectionCount, deleteVerbCount, deleteObjectCount,
          bulkDeleteVerbCount);
    }
    // all the files are still there? (avoid in scale test due to cost)
    if (!scaleTest) {
      readOnlyFiles.forEach(this::pathMustExist);
    }

    describe("Trying to delete upper-level directory");
    ex = expectDeleteForbidden(basePath);
    String iostats = ioStatisticsSourceToString(roleFS);

    if (multiDelete) {
      // multi-delete status checks
      deleteVerbCount.assertDiffEquals("Wrong delete request count", 0);
      bulkDeleteVerbCount.assertDiffEquals(
          "Wrong count of delete operations in " + iostats, 1);
      MultiObjectDeleteException mde = extractCause(
          MultiObjectDeleteException.class, ex);
      List<MultiObjectDeleteSupport.KeyPath> undeletedKeyPaths =
          removeUndeletedPaths(mde, keyPaths, storeContext::keyToPath);
      final List<Path> undeleted = toPathList(
          undeletedKeyPaths);
      deleteObjectCount.assertDiffEquals(
          "Wrong count of objects in delete request",
          allFiles.size());
      Assertions.assertThat(undeleted)
          .as("files which could not be deleted")
          .containsExactlyInAnyOrderElementsOf(readOnlyFiles);
      Assertions.assertThat(toPathList(keyPaths))
          .as("files which were deleted")
          .containsExactlyInAnyOrderElementsOf(deletableFiles);
      rejectionCount.assertDiffEquals("Wrong rejection count",
          readOnlyFiles.size());
    }
    reset(rejectionCount, deleteVerbCount);

    // build the set of all paths under the directory tree through
    // a directory listing (i.e. not getFileStatus()).
    // small risk of observed inconsistency here on unguarded stores.
    final Set<Path> readOnlyListing = listFilesUnderPath(readOnlyDir, true);

    String directoryList = readOnlyListing.stream()
        .map(Path::toString)
        .collect(Collectors.joining(", ", "[", "]"));

    Assertions.assertThat(readOnlyListing)
        .as("ReadOnly directory " + directoryList)
        .containsExactlyInAnyOrderElementsOf(readOnlyFiles);
  }

  /**
   * Verifies the logic of handling directory markers in
   * delete operations, specifically:
   * <ol>
   *   <li>all markers above empty directories MUST be deleted</li>
   *   <li>all markers above non-empty directories MUST NOT be deleted</li>
   * </ol>
   * As the delete list may include subdirectories, we need to work up from
   * the bottom of the list of deleted files before probing the parents,
   * that being done by a s3guard get(path, need-empty-directory) call.
   * <p></p>
   * This is pretty sensitive code.
   */
  @Test
  public void testSubdirDeleteFailures() throws Throwable {
    describe("Multiobject delete handling of directorYesFory markers");
    assume("Multiobject delete only", multiDelete);
  }

  /**
   * Expect the delete() call to fail.
   * @param path path to delete.
   * @return the expected exception.
   * @throws Exception any other failure.
   */
  private AccessDeniedException expectDeleteForbidden(Path path)
      throws Exception {
    try (DurationInfo ignored =
            new DurationInfo(LOG, true, "delete %s", path)) {
      return forbidden("Expected an error deleting "  + path,
          "",
          () -> {
            boolean r = roleFS.delete(path, true);
            return " delete=" + r + " " + ls(path.getParent());
          });
    }
  }

  /**
   * Expect that a rename will fail with an exception using the roleFS.
   * @param src source
   * @param dest dest
   * @return the exception raised.
   * @throws Exception any other failure
   */
  private AccessDeniedException expectRenameForbidden(Path src, Path dest)
      throws Exception {
    try (DurationInfo ignored =
            new DurationInfo(LOG, true,
                "rename(%s, %s)", src, dest)) {
      return forbidden(
          "Renaming " + src + " to " + dest,
          "",
          () -> {
            boolean result = roleFS.rename(src, dest);
            LOG.error("Rename should have been forbidden but returned {}",
                result);
            LOG.error("Source directory:\n{}",
                ContractTestUtils.ls(getFileSystem(), src.getParent()));
            LOG.error("Destination directory:\n{}",
                ContractTestUtils.ls(getFileSystem(), src.getParent()));
            return "Rename unexpectedly returned " + result;
          });
    }
  }

  /**
   * Assert that a path must exist, map IOEs to RTEs for loops.
   * @param p path.
   */
  private void pathMustExist(Path p) {
    eval(() -> assertPathExists("Missing path", p));
  }

  /**
   * List all files under a path.
   * @param path path to list
   * @param recursive recursive listing?
   * @return an unordered set of the paths.
   * @throws IOException failure
   */
  private Set<Path> listFilesUnderPath(Path path, boolean recursive)
      throws IOException {
    Set<Path> files = new TreeSet<>();
    try (DurationInfo ignore =
             new DurationInfo(LOG, "ls -R %s", path)) {
      applyLocatedFiles(getFileSystem().listFiles(path, recursive),
          (status) -> files.add(status.getPath()));
    }
    return files;
  }

  /**
   * Write the text to a file asynchronously. Logs the operation duration.
   * @param fs filesystem
   * @param path path
   * @return future to the patch created.
   */
  private static CompletableFuture<Path> put(FileSystem fs,
      Path path, String text) {
    return submit(EXECUTOR, () -> {
      try (DurationInfo ignore =
               new DurationInfo(LOG, false, "Creating %s", path)) {
        createFile(fs, path, true, text.getBytes(Charsets.UTF_8));
        return path;
      }
    });
  }

  /**
   * Build a set of files in a directory tree.
   * @param fs filesystem
   * @param destDir destination
   * @param depth file depth
   * @param fileCount number of files to create.
   * @param dirCount number of dirs to create at each level
   * @return the list of files created.
   */
  public static List<Path> createFiles(final FileSystem fs,
      final Path destDir,
      final int depth,
      final int fileCount,
      final int dirCount) throws IOException {
    return createDirsAndFiles(fs, destDir, depth, fileCount, dirCount,
        new ArrayList<Path>(fileCount),
        new ArrayList<Path>(dirCount));
  }

  /**
   * Build a set of files in a directory tree.
   * @param fs filesystem
   * @param destDir destination
   * @param depth file depth
   * @param fileCount number of files to create.
   * @param dirCount number of dirs to create at each level
   * @param paths [out] list of file paths created
   * @param dirs [out] list of directory paths created.
   * @return the list of files created.
   */
  public static List<Path> createDirsAndFiles(final FileSystem fs,
      final Path destDir,
      final int depth,
      final int fileCount,
      final int dirCount,
      final List<Path> paths,
      final List<Path> dirs) throws IOException {
    buildPaths(paths, dirs, destDir, depth, fileCount, dirCount);
    List<CompletableFuture<Path>> futures = new ArrayList<>(paths.size()
        + dirs.size());

    // create directories. With dir marker retention, that adds more entries
    // to cause deletion issues
    try (DurationInfo ignore =
             new DurationInfo(LOG, "Creating %d directories", dirs.size())) {
      for (Path path : dirs) {
        futures.add(submit(EXECUTOR, () ->{
          fs.mkdirs(path);
          return path;
        }));
      }
      waitForCompletion(futures);
    }

    try (DurationInfo ignore =
            new DurationInfo(LOG, "Creating %d files", paths.size())) {
      for (Path path : paths) {
        futures.add(put(fs, path, path.getName()));
      }
      waitForCompletion(futures);
      return paths;
    }
  }

  /**
   * Verifies that s3:DeleteObjectVersion is not required for rename.
   * <p></p>
   * See HADOOP-17621.
   * <p></p>
   * This test will only show a regression if the bucket has versioning
   * enabled *and* S3Guard is enabled.
   */
  @Test
  public void testRenamePermissionRequirements() throws Throwable {
    describe("Verify rename() only needs s3:DeleteObject permission");
    // close the existing roleFS
    IOUtils.cleanupWithLogger(LOG, roleFS);

    // create an assumed role config which doesn't have
    // s3:DeleteObjectVersion permission, and attempt rename
    // and then delete.
    Configuration roleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(roleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALLOW_SSE_KMS_RW,
        STATEMENT_ALL_BUCKET_READ_ACCESS,  // root:     r-x
        new Statement(Effects.Allow)       // dest:     rwx
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(basePath)),
        new Statement(Effects.Deny)
            .addActions(S3_DELETE_OBJECT_VERSION)
            .addResources(directory(basePath)));
    roleFS = (S3AFileSystem) basePath.getFileSystem(roleConfig);

    Path srcDir = new Path(basePath, "src");
    Path destDir = new Path(basePath, "dest");
    roleFS.mkdirs(srcDir);

    // the role FS has everything but that deleteObjectVersion permission, so
    // MUST be able to create files
    List<Path> createdFiles = createFiles(roleFS, srcDir, dirDepth, fileCount,
        dirCount);
    roleFS.rename(srcDir, destDir);
    roleFS.rename(destDir, srcDir);
    roleFS.delete(srcDir, true);

  }
}
