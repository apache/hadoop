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
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.Constants.ENABLE_MULTI_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_THREADS;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.MetricDiff;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.reset;
import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;
import static org.apache.hadoop.fs.s3a.Statistic.FILES_DELETE_REJECTED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUESTS;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Effects;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.Statement;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.directory;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.statement;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.S3_ALL_BUCKETS;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.S3_BUCKET_READ_OPERATIONS;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.S3_PATH_RW_OPERATIONS;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.STATEMENT_ALLOW_SSE_KMS_RW;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.STATEMENT_S3GUARD_CLIENT;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.bindRolePolicyStatements;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.forbidden;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.newAssumedRoleConfig;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.extractUndeletedPaths;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.removeUndeletedPaths;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.assertFileCount;
import static org.apache.hadoop.fs.s3a.test.ExtraAssertions.extractCause;
import static org.apache.hadoop.test.LambdaTestUtils.eval;

/**
 * Test partial failures of delete and rename operations, especially
 * that the S3Guard tables are consistent with the state of
 * the filesystem.
 *
 * All these test have a unique path for each run, with a roleFS having
 * full RW access to part of it, and R/O access to a restricted subdirectory
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
  private static final ListeningExecutorService executor =
      BlockingThreadPoolExecutorService.newInstance(
          EXECUTOR_THREAD_COUNT,
          EXECUTOR_THREAD_COUNT * 2,
          30, TimeUnit.SECONDS,
          "test-operations");

  public static final int FILE_COUNT_NON_SCALED = 10;

  /**
   * The number of files for a scaled test. This is still
   * less than half the amount which can be fitted into a delete
   * request, so that even with this many R/W and R/O files,
   * both can fit in the same request.
   * Then, when a partial delete occurs, we can make assertions
   * knowing that all R/W files should have been deleted and all
   * R/O files rejected.
   */
  public static final int FILE_COUNT_SCALED = 400;

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

  private Path basePath;

  private Path destDir;

  private Path readonlyChild;

  private Path readOnlyDir;

  /** delete policy: single or multi? */
  private final boolean multiDelete;

  private Configuration assumedRoleConfig;

  private int filecount;

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
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true},
    });
  }

  public ITestPartialRenamesDeletes(final boolean multiDelete) {
    this.multiDelete = multiDelete;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();
    basePath = uniquePath();
    readOnlyDir = new Path(basePath, "readonlyDir");
    destDir = new Path(basePath, "renameDest");
    readonlyChild = new Path(readOnlyDir, "child");
    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);
    fs.mkdirs(destDir);
    assumedRoleConfig = createAssumedRoleConfig();
    bindRolePolicyStatements(assumedRoleConfig,
        STATEMENT_S3GUARD_CLIENT,
        STATEMENT_ALL_BUCKET_READ_ACCESS,
        new Statement(Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(destDir))
    );
    roleFS = (S3AFileSystem) readOnlyDir.getFileSystem(assumedRoleConfig);
    scaleTest = multiDelete && getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    // switch to the big set of files iff this is a multidelete run
    // with -Dscale set.
    // without that the DELETE calls become a key part of the bottleneck
    filecount = scaleTest
        ? FILE_COUNT_SCALED
        : FILE_COUNT_NON_SCALED;
  }

  @Override
  public void teardown() throws Exception {
    S3AUtils.closeAll(LOG, roleFS);
    super.teardown();
  }

  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    super.deleteTestDirInTeardown();
    Path path = getContract().getTestPath();
    try {
      prune(path);
    } catch (IOException e) {
      LOG.warn("When pruning the test directory {}", path, e);
    }
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
    String bucketName = getTestBucketName(conf);

    removeBucketOverrides(bucketName, conf, ENABLE_MULTI_DELETE);
    conf.setBoolean(ENABLE_MULTI_DELETE, multiDelete);
    return conf;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    // ramp up the number of connections we can have for maximum PUT
    // performance
    removeBucketOverrides(bucketName, conf, MAX_THREADS, MAXIMUM_CONNECTIONS);
    conf.setInt(MAX_THREADS, EXECUTOR_THREAD_COUNT);
    conf.setInt(MAXIMUM_CONNECTIONS, EXECUTOR_THREAD_COUNT * 2);
    conf.setBoolean(ENABLE_MULTI_DELETE, multiDelete);
    // turn off prune delays, so as to stop scale tests creating
    // so much cruft that future CLI prune commands take forever
    conf.setInt(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, 0);
    return conf;
  }


  /**
   * Create a unique path, which includes method name,
   * multdelete flag and a random UUID.
   * @return a string to use for paths.
   * @throws IOException path creation failure.
   */
  private Path uniquePath() throws IOException {
    return path(
        String.format("%s-%s-%04d",
            getMethodName(),
            multiDelete ? "multi" : "single",
            System.currentTimeMillis() % 10000));
  }

  /**
   * This is here to verify role and path setup.
   */
  @Test
  public void testCannotTouchUnderRODir() throws Throwable {
    forbidden("touching the empty child " + readonlyChild,
        "",
        () -> ContractTestUtils.touch(roleFS, readonlyChild));
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
            .addResources(directory(destDir)));
    roleFS = (S3AFileSystem) readOnlyDir.getFileSystem(conf);

    S3AFileSystem fs = getFileSystem();
    roleFS.getFileStatus(ROOT);
    fs.mkdirs(readOnlyDir);
    // you can create an adjacent child
    touch(fs, readonlyChild);

    fs.delete(destDir, true);
    // as dest doesn't exist, this will map child -> dest
    assertRenameOutcome(roleFS, readonlyChild, destDir, true);

    assertIsFile(destDir);
    assertIsDirectory(readOnlyDir);
    Path renamedDestPath = new Path(readOnlyDir, destDir.getName());
    assertRenameOutcome(roleFS, destDir, readOnlyDir, true);
    assertIsFile(renamedDestPath);
    roleFS.delete(readOnlyDir, true);
    roleFS.delete(destDir, true);
  }

  @Test
  public void testRenameSingleFileFailsLeavingSource() throws Throwable {
    describe("rename with source read only; multi=%s", multiDelete);
    Path readOnlyFile = readonlyChild;

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);

    // this file is readable by the roleFS, but cannot be deleted
    touch(fs, readOnlyFile);

    roleFS.delete(destDir, true);
    roleFS.mkdirs(destDir);
    // rename will fail in the delete phase
    AccessDeniedException deniedException = expectRenameForbidden(
        readOnlyFile, destDir);


    // and the source file is still there
    assertIsFile(readOnlyFile);

    // but so is the copied version, because there's no attempt
    // at rollback, or preflight checking on the delete permissions
    Path renamedFile = new Path(destDir, readOnlyFile.getName());

    assertIsFile(renamedFile);

    ContractTestUtils.assertDeleted(roleFS, renamedFile, true);
    assertFileCount("Empty Dest Dir", roleFS,
        destDir, 0);
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
   */
  @Test
  public void testRenameFilesetFailsLeavingSourceUnchanged() throws Throwable {
    describe("rename with source read only; multi=%s", multiDelete);

    // the full FS
    S3AFileSystem fs = getFileSystem();
    fs.delete(basePath, true);

    roleFS.mkdirs(destDir);

    // create a set of files
    // this is done in parallel as it is 10x faster on a long-haul test run.
    List<Path> createdFiles = createFiles(fs, readOnlyDir, filecount);
    // are they all there?
    assertFileCount("files ready to rename", roleFS,
        readOnlyDir, (long) filecount);

    // try to rename the directory
    LOG.info("Renaming readonly files {} to {}", readOnlyDir, destDir);

    AccessDeniedException deniedException = expectRenameForbidden(readOnlyDir, destDir);
    if (multiDelete) {
      // look in that exception for a multidelete
      MultiObjectDeleteException mde = extractCause(
          MultiObjectDeleteException.class, deniedException);
      final List<Path> undeleted
          = extractUndeletedPaths(mde, fs::keyToQualifiedPath);
      Assertions.assertThat(undeleted)
          .as("files which could not be deleted")
          .hasSize(filecount)
          .containsAll(createdFiles)
          .containsExactlyInAnyOrderElementsOf(createdFiles);
    }
    LOG.info("Result of renaming read-only files is AccessDeniedException",
        deniedException);
    assertFileCount("files in the source directory", roleFS,
        readOnlyDir, (long) filecount);
  }

  /**
   * Have a directory with full R/W permissions, but then remove
   * write access underneath, and try to delete it.
   *
   */
  @Test
  public void testPartialDelete() throws Throwable {
    describe("delete with part of the child tree read only; multidelete");

    // the full FS
    S3AFileSystem fs = getFileSystem();

    List<Path> readOnlyFiles = createFiles(fs, readOnlyDir, filecount);
    List<Path> deletableFiles = createFiles(fs, destDir, filecount);

    // as a safety check, verify that one of the deletable files can be deleted
    Path head = deletableFiles.remove(0);
    assertTrue("delete " + head + " failed",
        roleFS.delete(head, false));
    List<Path> allFiles = Stream.concat(
        readOnlyFiles.stream(),
        deletableFiles.stream())
        .collect(Collectors.toList());

    // this set can be deleted by the role FS
    MetricDiff rejectionCount = new MetricDiff(roleFS, FILES_DELETE_REJECTED);
    MetricDiff deleteVerbCount = new MetricDiff(roleFS, OBJECT_DELETE_REQUESTS);

    describe("Trying to delete read only directory");
    AccessDeniedException ex = expectDeleteForbidden(readOnlyDir);
    if (multiDelete) {
      // multi-delete status checks
      extractCause(MultiObjectDeleteException.class, ex);
      rejectionCount.assertDiffEquals("Wrong rejection count", filecount);
      deleteVerbCount.assertDiffEquals("Wrong delete count", 1);
      reset(rejectionCount, deleteVerbCount);
    }
    // all the files are still there? (avoid in scale test due to cost)
    if (!scaleTest) {
      readOnlyFiles.forEach(this::pathMustExist);
    }

    describe("Trying to delete upper-level directory");
    ex = expectDeleteForbidden(basePath);
    if (multiDelete) {
      // multi-delete status checks
      extractCause(MultiObjectDeleteException.class, ex);
      deleteVerbCount.assertDiffEquals("Wrong delete count", 1);
      MultiObjectDeleteException mde = extractCause(
          MultiObjectDeleteException.class, ex);
      final List<Path> undeleted
          = removeUndeletedPaths(mde, allFiles, fs::keyToQualifiedPath);
      Assertions.assertThat(undeleted)
          .as("files which could not be deleted")
          .containsExactlyInAnyOrderElementsOf(readOnlyFiles);
      Assertions.assertThat(allFiles)
          .as("files which were deleted")
          .containsExactlyInAnyOrderElementsOf(deletableFiles);
      rejectionCount.assertDiffEquals("Wrong rejection count", filecount);
    }
    reset(rejectionCount, deleteVerbCount);

    // build the set of all paths under the directory tree through
    // a directory listing (i.e. not getFileStatus()).
    // small risk of observed inconsistency here on unguarded stores.
    final Set<Path> roFListing = listFilesUnderPath(readOnlyDir, true);

    String directoryList = roFListing
        .stream()
        .map(Path::toString)
        .collect(Collectors.joining(", ", "[", "]"));

    Assertions.assertThat(roFListing)
        .as("ReadOnly directory " + directoryList)
        .containsAll(readOnlyFiles);
  }

  private AccessDeniedException expectDeleteForbidden(Path path) throws Exception {
    try(DurationInfo ignored =
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
   * Expect that a rename will fail with an exception.
   * @param src source
   * @param dest dest
   * @return the exception raised.
   * @throws Exception any other failure
   */
  private AccessDeniedException expectRenameForbidden(Path src, Path dest)
      throws Exception {
    try(DurationInfo ignored =
            new DurationInfo(LOG, true, "rename")) {
      return forbidden(
          "Renaming " + src + " to " + dest,
          "",
          () -> {
            roleFS.rename(src, dest);
            return ContractTestUtils.ls(getFileSystem(), src.getParent());
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
   * Assert that a path must exist, map IOEs to RTEs for loops.
   * @param p path.
   */
  private void pathMustNotExist(Path p) {
    eval(() -> assertPathDoesNotExist("Path should not exist", p));
  }

  /**
   * Prune the store for everything under the test path.
   * @param path path.
   * @throws IOException on failure.
   */
  private void prune(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();
    if (fs.hasMetadataStore()) {
      MetadataStore store = fs.getMetadataStore();
      try(DurationInfo ignored = new DurationInfo(LOG, true, "prune %s", path)) {
        store.prune(System.currentTimeMillis(), fs.pathToKey(path));
      }
    }
  }

  /**
   * List all files under a path.
   * @param path path to list
   * @param recursive recursive listing?
   * @return an unordered set of the paths.
   * @throws IOException failure
   */
  private Set<Path> listFilesUnderPath(Path path, boolean recursive) throws IOException {
    Set<Path> files = new TreeSet<>();
    applyLocatedFiles(getFileSystem().listFiles(path, recursive),
        (status) -> {
          files.add(status.getPath());
        });
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
    return submit(executor, () -> {
      try (DurationInfo ignore =
               new DurationInfo(LOG, "Creating %s", path)) {
        createFile(fs, path, true, text.getBytes(Charsets.UTF_8));
        return path;
      }
    });
  }

  /**
   * Parallel-touch a set of files in the destination directory.
   * @param fs filesystem
   * @param destDir destination
   * @param range range 1..range inclusive of files to create.
   * @return the list of paths created.
   */
  public static List<Path> createFiles(final FileSystem fs,
      final Path destDir,
      final int range) throws IOException {
    List<CompletableFuture<Path>> futures = new ArrayList<>(range);
    List<Path> paths = new ArrayList<>(range);
    for (int i = 0; i < range; i++) {
      String name = "file-" + i;
      Path p = new Path(destDir, name);
      paths.add(p);
      futures.add(put(fs, p, name));
    }
    CompletableFuture.allOf(futures.toArray(
        new CompletableFuture[futures.size()])).join();
    return paths;
  }

}
