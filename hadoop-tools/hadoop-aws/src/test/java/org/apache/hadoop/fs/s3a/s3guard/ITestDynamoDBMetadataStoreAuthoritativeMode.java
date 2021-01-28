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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_RECORD_WRITES;
import static org.apache.hadoop.fs.s3a.s3guard.AuthoritativeAuditOperation.ERROR_PATH_NOT_AUTH_IN_FS;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.authoritativeEmptyDirectoryMarker;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Authoritative.CHECK_FLAG;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Authoritative.REQUIRE_AUTH;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Import.AUTH_FLAG;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.VERBOSE;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.expectExecResult;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_ACCEPTABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to verify the expected behaviour of DynamoDB and authoritative mode.
 * The main testFS is non-auth; we also create a test FS which runs in auth
 * mode.
 * Making the default FS non-auth means that test path cleanup in the
 * superclass isn't going to get mislead by anything authoritative.
 *
 * For performance boosting we demand create the auth FS and its test
 * paths on the first test setup().
 * This also fixes the auth/nonauth paths so that a specific
 * bit of the FS is expected to be auth in the FS.
 *
 * This test is designed to run in parallel mode with other tests which
 * may or may not be auth mode.
 *
 * It shouldn't make any difference -tests here simply must not make
 * any assumptions about the state of any path outside the test tree.
 */
@SuppressWarnings("StaticNonFinalField")
public class ITestDynamoDBMetadataStoreAuthoritativeMode
    extends AbstractS3ATestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestDynamoDBMetadataStoreAuthoritativeMode.class);

  public static final String AUDIT = S3GuardTool.Authoritative.NAME;

  public static final String IMPORT = S3GuardTool.Import.NAME;

  private String fsUriStr;

  /**
   * Authoritative FS.
   */
  private static S3AFileSystem authFS;

  /**
   * The unguarded file system.
   */
  private static S3AFileSystem unguardedFS;

  /**
   * Base path in the store for auth and nonauth paths.
   */
  private static Path basePath;

  /**
   * Path under basePath which will be declared as authoritative.
   */
  private static Path authPath;

  /**
   * Path under basePath which will be declared as non-authoritative.
   */
  private static Path nonauthPath;

  /**
   * test method specific auth path.
   */
  private Path methodAuthPath;

  /**
   * test method specific non-auth path.
   */
  private Path methodNonauthPath;

  /**
   * Auditor of store state.
   */
  private AuthoritativeAuditOperation auditor;

  /**
   * Path {@code $methodAuthPath/dir}.
   */
  private Path dir;

  /**
   * Path {@code $methodAuthPath/dir/file}.
   */
  private Path dirFile;

  /**
   * List of tools to close in test teardown.
   */
  private final List<S3GuardTool> toolsToClose = new ArrayList<>();

  /**
   * The metastore of the auth filesystem.
   */
  private DynamoDBMetadataStore metastore;

  /**
   * After all tests have run, close the filesystems.
   */
  @AfterClass
  public static void closeFileSystems() {
    IOUtils.cleanupWithLogger(LOG, authFS, unguardedFS);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    conf.setTimeDuration(
        S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY,
        0,
        TimeUnit.MILLISECONDS);
    return conf;
  }

  /**
   * Test case setup will on-demand create the class-level fields
   * of the authFS and the auth/non-auth paths.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    S3ATestUtils.assumeS3GuardState(true, conf);
    assume("Filesystem isn't running DDB",
        fs.getMetadataStore() instanceof DynamoDBMetadataStore);
    URI fsURI = fs.getUri();
    fsUriStr = fsURI.toString();
    if (!fsUriStr.endsWith("/")) {
      fsUriStr = fsUriStr + "/";
    }


    if (authFS == null) {
      // creating the test stores
      basePath = path("base");
      authPath = new Path(basePath, "auth");
      nonauthPath = new Path(basePath, "nonauth");
      final Configuration authconf = new Configuration(conf);
      final URI uri = authPath.toUri();
      authconf.set(AUTHORITATIVE_PATH, uri.toString());
      authFS = (S3AFileSystem) FileSystem.newInstance(uri, authconf);

      // and create the unguarded at the same time
      final Configuration unguardedConf = new Configuration(conf);
      removeBaseAndBucketOverrides(unguardedConf,
          S3_METADATA_STORE_IMPL);
      unguardedFS = (S3AFileSystem) FileSystem.newInstance(uri, unguardedConf);
    }
    metastore = (DynamoDBMetadataStore) authFS.getMetadataStore();
    auditor = new AuthoritativeAuditOperation(
        authFS.createStoreContext(),
        metastore,
        true,
        true);

    cleanupMethodPaths();
    dir = new Path(methodAuthPath, "dir");
    dirFile = new Path(dir, "file");
  }

  @Override
  public void teardown() throws Exception {
    toolsToClose.forEach(t -> IOUtils.cleanupWithLogger(LOG, t));
    try {
      cleanupMethodPaths();
    } catch (IOException ignored) {
    }
    super.teardown();
  }

  /**
   * Clean up from other test runs which halted.
   * Uses the authfs; no-op if null.
   * @throws IOException Failure
   */
  private void cleanupMethodPaths() throws IOException {
    S3AFileSystem fs = authFS;
    if (fs != null) {
      methodAuthPath = new Path(authPath, getMethodName());
      fs.delete(methodAuthPath, true);
      methodNonauthPath = new Path(nonauthPath, getMethodName());
      fs.delete(methodNonauthPath, true);
    }
  }

  /**
   * Declare that the tool is to be closed in teardown.
   * @param tool tool to close
   * @return the tool.
   */
  protected <T extends S3GuardTool> T toClose(T tool) {
    toolsToClose.add(tool);
    return tool;
  }

  /**
   * Get the conf of the auth FS.
   * @return the auth FS config.
   */
  private Configuration getAuthConf() {
    return authFS.getConf();
  }

  @Test
  public void testEmptyDirMarkerIsAuth() {
    final S3AFileStatus st = new S3AFileStatus(true, dir, "root");
    final DDBPathMetadata md = (DDBPathMetadata)
        authoritativeEmptyDirectoryMarker(st);
    Assertions.assertThat(md)
        .describedAs("Metadata %s", md)
        .matches(DDBPathMetadata::isAuthoritativeDir, "is auth dir")
        .matches(d -> d.isEmptyDirectory() == Tristate.TRUE,
            "isEmptyDirectory");
  }

  @Test
  public void testMkDirAuth() throws Throwable {
    describe("create an empty dir and assert it is tagged as authoritative");
    authFS.mkdirs(dir);
    expectAuthRecursive(dir);
  }

  @Test
  public void testListStatusMakesEmptyDirAuth() throws Throwable {
    describe("Verify listStatus marks an Empty dir as auth");
    mkNonauthDir(dir);
    // initial dir is non-auth
    expectNonauthNonRecursive(dir);
    authFS.listStatus(dir);
    // dir is auth;
    expectAuthRecursive(dir);
    // Next list will not go to s3
    assertListDoesNotUpdateAuth(dir);
  }

  @Test
  public void testListFilesRecursiveWhenAllListingsAreAuthoritative()
      throws Exception {
    describe("listFiles does not make further calls to the fs when"
        + "all nested directory listings are authoritative");
    Set<Path> originalFiles = new HashSet<>();

    Path parentDir = dir;
    Path parentFile = dirFile;
    Path nestedDir1 = new Path(dir, "nested1");
    Path nestedFile1 = new Path(nestedDir1, "nestedFile1");
    Path nestedDir2 = new Path(nestedDir1, "nested2/");
    Path nestedFile2 = new Path(nestedDir2, "nestedFile2");

    originalFiles.add(parentFile);
    originalFiles.add(nestedFile1);
    originalFiles.add(nestedFile2);

    mkAuthDir(parentDir);
    mkAuthDir(nestedDir1);
    mkAuthDir(nestedDir2);
    touchFile(parentFile);
    touchFile(nestedFile1);
    touchFile(nestedFile2);

    S3ATestUtils.MetricDiff objListRequests =
        new S3ATestUtils.MetricDiff(authFS, OBJECT_LIST_REQUEST);

    RemoteIterator<LocatedFileStatus> statusIterator =
        authFS.listFiles(dir, true);

    List<Path> pathsFromStatusIterator = toPaths(statusIterator);

    Assertions.assertThat(pathsFromStatusIterator)
        .as("listFiles should return all the items in actual"
            + "S3 directory and nothing more")
        .hasSameElementsAs(originalFiles)
        .hasSameSizeAs(originalFiles);

    objListRequests.assertDiffEquals("There must not be any OBJECT_LIST "
        + "requests as all directory listings are authoritative", 0);
  }

  @Test
  public void testListFilesRecursiveWhenSomePathsAreNotAuthoritative()
      throws Exception {
    describe("listFiles correctly constructs recursive listing"
        + "when authoritative and non-authoritative paths are mixed");
    List<Path> originalFiles = new ArrayList<>();
    Path parentDir = dir;
    Path parentFile = dirFile;
    Path nestedDir1 = new Path(dir, "nested1");
    Path nestedFile1 = new Path(nestedDir1, "nestedFile1");
    Path nestedDir2 = new Path(nestedDir1, "nested2/");
    Path nestedFile2 = new Path(nestedDir2, "nestedFile2");

    originalFiles.add(parentFile);
    originalFiles.add(nestedFile1);
    originalFiles.add(nestedFile2);

    mkAuthDir(parentDir);
    mkNonauthDir(nestedDir1);
    mkAuthDir(nestedDir2);
    touchFile(parentFile);
    touchFile(nestedFile1);
    touchFile(nestedFile2);

    S3ATestUtils.MetricDiff objListRequests =
        new S3ATestUtils.MetricDiff(authFS, OBJECT_LIST_REQUEST);

    RemoteIterator<LocatedFileStatus> statusIterator =
        authFS.listFiles(dir, true);

    List<Path> pathsFromStatusIterator = toPaths(statusIterator);

    Assertions.assertThat(pathsFromStatusIterator)
        .as("listFiles should return all the items in actual"
            + "S3 directory and nothing more")
        .hasSameElementsAs(originalFiles)
        .hasSameSizeAs(originalFiles);
    objListRequests.assertDiffEquals("Only 1 OBJECT_LIST call is expected"
        + "as a nested directory listing is not authoritative", 1);
  }

  @Test
  public void testListStatusMakesDirAuth() throws Throwable {
    describe("Verify listStatus marks a dir as auth");
    final Path subdir = new Path(dir, "subdir");

    mkAuthDir(dir);
    expectAuthRecursive(dir);
    authFS.mkdirs(subdir);
    // dir and subdirs are auth
    expectAuthRecursive(dir);
    expectAuthRecursive(subdir);
    // now mark the dir as nonauth
    markDirNonauth(dir);
    expectNonauthNonRecursive(dir);
    expectAuthRecursive(subdir);

    // look at the MD & make sure that the dir and subdir are auth
    final DirListingMetadata listing = metastore.listChildren(dir);
    Assertions.assertThat(listing)
        .describedAs("metadata of %s", dir)
        .matches(d -> !d.isAuthoritative(), "is not auth");
    Assertions.assertThat(listing.getListing())
        .describedAs("listing of %s", dir)
        .hasSize(1)
        .allMatch(md -> ((DDBPathMetadata) md).isAuthoritativeDir(),
            "is auth");

    // directory list makes the dir auth and leaves the child auth
    assertListUpdatesAuth(dir);

    // and afterwards, a followup list does not write anything to DDB
    // (as the dir is auth, its not going to go near the FS to update...)
    expectOperationUpdatesDDB(0, () -> authFS.listStatus(dir));
    // mark the dir nonauth again
    markDirNonauth(dir);
    // and only one record is written to DDB, the dir marker as auth
    // the subdir is not overwritten
    expectOperationUpdatesDDB(1, () -> authFS.listStatus(dir));
  }

  @Test
  public void testAddFileMarksNonAuth() throws Throwable {
    describe("Adding a file marks dir as nonauth but leaves ancestors alone");
    mkAuthDir(methodAuthPath);
    touchFile(dirFile);
    expectNonauthRecursive(dir);
    assertListUpdatesAuth(dir);
    expectAuthRecursive(methodAuthPath);
  }

  /**
   * When you delete the single file in a directory then a fake directory
   * marker is added. This must be auth.
   */
  @Test
  public void testDeleteSingleFileLeavesMarkersAlone() throws Throwable {
    describe("Deleting a file with no peers makes no changes to ancestors");
    mkAuthDir(methodAuthPath);
    touchFile(dirFile);
    assertListUpdatesAuth(dir);
    authFS.delete(dirFile, false);
    expectAuthRecursive(methodAuthPath);
  }

  @Test
  public void testDeleteMultipleFileLeavesMarkersAlone() throws Throwable {
    describe("Deleting a file from a dir with >1 file makes no changes"
        + " to ancestors");
    mkAuthDir(methodAuthPath);
    touchFile(dirFile);
    Path file2 = new Path(dir, "file2");
    touchFile(file2);
    assertListUpdatesAuth(dir);
    authFS.delete(dirFile, false);
    expectAuthRecursive(methodAuthPath);
  }

  @Test
  public void testDeleteEmptyDirLeavesParentAuth() throws Throwable {
    describe("Deleting a directory retains the auth status "
        + "of the parent directory");
    mkAuthDir(dir);
    mkAuthDir(dirFile);
    expectAuthRecursive(dir);
    authFS.delete(dirFile, false);
    expectAuthRecursive(dir);
  }

  /**
   * Assert the number of pruned files matches expectations.
   * @param path path to prune
   * @param mode prune mode
   * @param limit timestamp before which files are deleted
   * @param expected number of entries to be pruned
   */
  protected void assertPruned(final Path path,
      final MetadataStore.PruneMode mode,
      final long limit,
      final int expected)
      throws IOException {
    String keyPrefix
        = PathMetadataDynamoDBTranslation.pathToParentKey(path);
    Assertions.assertThat(
        authFS.getMetadataStore().prune(
            mode,
            limit,
            keyPrefix))
        .describedAs("Number of files pruned under %s", keyPrefix)
        .isEqualTo(expected);
  }

  @Test
  public void testPruneFilesMarksNonAuth() throws Throwable {
    describe("Pruning a file marks dir as nonauth");
    mkAuthDir(methodAuthPath);

    touchFile(dirFile);
    assertListUpdatesAuth(dir);

    assertPruned(dir,
        MetadataStore.PruneMode.ALL_BY_MODTIME,
        Long.MAX_VALUE,
        1);
    expectNonauthRecursive(dir);
  }

  @Test
  public void testPruneTombstoneRetainsAuth() throws Throwable {
    describe("Verify that deleting and then pruning a file does not change"
        + " the state of the parent.");
    mkAuthDir(methodAuthPath);

    touchFile(dirFile);
    assertListUpdatesAuth(dir);
    // add a second file to avoid hitting the mkdir-is-nonauth issue that causes
    // testDeleteSingleFileLeavesMarkersAlone() to fail
    Path file2 = new Path(dir, "file2");
    touchFile(file2);
    authFS.delete(dirFile, false);
    expectAuthRecursive(dir);
    assertPruned(dir, MetadataStore.PruneMode.TOMBSTONES_BY_LASTUPDATED,
        Long.MAX_VALUE, 1);
    expectAuthRecursive(dir);
  }

  @Test
  public void testRenameFile() throws Throwable {
    describe("renaming a file");
    final Path source = new Path(dir, "source");
    final Path dest = new Path(dir, "dest");
    touchFile(source);
    assertListUpdatesAuth(dir);
    authFS.rename(source, dest);
    expectAuthRecursive(dir);
  }

  @Test
  public void testRenameDirMarksDestAsAuth() throws Throwable {
    describe("renaming a dir must mark dest tree as auth");
    final Path base = methodAuthPath;
    mkAuthDir(base);
    final Path source = new Path(base, "source");
    final Path dest = new Path(base, "dest");
    mkAuthDir(source);
    expectAuthRecursive(base);
    Path subdir = new Path(source, "subdir");
    Path f = new Path(subdir, "file");
    touchFile(f);
    expectNonauthRecursive(base);
    // list the source directories so everything is
    // marked as auth
    authFS.listStatus(source);
    authFS.listStatus(subdir);
    expectAuthRecursive(base);
    authFS.rename(source, dest);
    expectAuthRecursive(base);
  }

  @Test
  @Ignore("TODO: HADOOP-16465")
  public void testListLocatedStatusMarksDirAsAuth() throws Throwable {
    describe("validate listLocatedStatus()");
    final Path subdir = new Path(dir, "subdir");
    final Path subdirfile = new Path(subdir, "file");
    touchFile(subdirfile);
    // Subdir list makes it auth
    expectAuthoritativeUpdate(1, 1, () -> {
      final RemoteIterator<LocatedFileStatus> st
          = authFS.listLocatedStatus(subdir);
      applyLocatedFiles(st,
          f -> LOG.info("{}", f));
      return null;
    });
    expectAuthNonRecursive(subdir);
  }

  @Test
  public void testS3GuardImportMarksDirAsAuth() throws Throwable {
    describe("import with authoritive=true marks directories");
    // the base dir is auth
    mkAuthDir(methodAuthPath);
    int expected = 0;
    final Path subdir = new Path(dir, "subdir");
    final Path subdirfile = new Path(subdir, "file");
    ContractTestUtils.touch(authFS, subdirfile);
    expected++;
    for (int i = 0; i < 5; i++) {
      ContractTestUtils.touch(authFS, new Path(subdir, "file-" + i));
      expected++;
    }
    final Path emptydir = new Path(dir, "emptydir");
    unguardedFS.mkdirs(emptydir);
    expected++;

    S3AFileStatus status1 = (S3AFileStatus) authFS.getFileStatus(subdirfile);
    final MetadataStore authMS = authFS.getMetadataStore();
    final ImportOperation importer = new ImportOperation(unguardedFS,
        authMS,
        (S3AFileStatus) unguardedFS.getFileStatus(dir),
        true, true);
    final Long count = importer.execute();
    expectAuthRecursive(dir);
    // the parent dir shouldn't have changed
    expectAuthRecursive(methodAuthPath);

    // file entry
    S3AFileStatus status2 = (S3AFileStatus) authFS.getFileStatus(subdirfile);
    Assertions.assertThat(status2.getETag())
        .describedAs("Etag of %s", status2)
        .isEqualTo(status1.getETag());
    // only picked up on versioned stores.
    Assertions.assertThat(status2.getVersionId())
        .describedAs("version ID of %s", status2)
        .isEqualTo(status1.getVersionId());

    // the import finds files and empty dirs
    Assertions.assertThat(count)
        .describedAs("Count of imports under %s", dir)
        .isEqualTo(expected);
  }

  /**
   * Given a flag, add a - prefix.
   * @param flag flag to wrap
   * @return a flag for use in the CLI
   */
  private String f(String flag) {
    return "-" + flag;
  }

  @Test
  public void testAuditS3GuardTool() throws Throwable {
    describe("Test the s3guard audit CLI");
    mkNonauthDir(methodAuthPath);
    final String path = methodAuthPath.toString();
    // this is non-auth, so the scan is rejected
    expectExecResult(EXIT_NOT_ACCEPTABLE,
        authTool(),
        AUDIT,
        f(CHECK_FLAG),
        f(REQUIRE_AUTH),
        f(VERBOSE),
        path);
    // a non-auth audit is fine
    exec(authTool(),
        AUDIT,
        f(VERBOSE),
        path);
    // non-auth import
    exec(importTool(),
        IMPORT,
        f(VERBOSE),
        path);
    // which will leave the result unchanged
    expectExecResult(EXIT_NOT_ACCEPTABLE,
        authTool(),
        AUDIT,
        f(CHECK_FLAG),
        f(REQUIRE_AUTH),
        f(VERBOSE),
        path);
    // auth import
    exec(importTool(),
        IMPORT,
        f(AUTH_FLAG),
        f(VERBOSE),
        path);
    // so now the audit succeeds
    exec(authTool(),
        AUDIT,
        f(REQUIRE_AUTH),
        path);
  }

  /**
   * Create an import tool instance with the auth FS Config.
   * It will be closed in teardown.
   * @return a new instance.
   */
  protected S3GuardTool.Import importTool() {
    return toClose(new S3GuardTool.Import(getAuthConf()));
  }

  /**
   * Create an auth tool instance with the auth FS Config.
   * It will be closed in teardown.
   * @return a new instance.
   */
  protected S3GuardTool.Authoritative authTool() {
    return toClose(new S3GuardTool.Authoritative(getAuthConf()));
  }

  @Test
  public void testAuditS3GuardToolNonauthDir() throws Throwable {
    describe("Test the s3guard audit -check-conf against a nonauth path");
    mkdirs(methodNonauthPath);
    expectExecResult(ERROR_PATH_NOT_AUTH_IN_FS,
        authTool(),
        AUDIT,
        f(CHECK_FLAG),
        methodNonauthPath.toString());
  }

  @Test
  public void testImportNonauthDir() throws Throwable {
    describe("s3guard import against a nonauth path marks the dirs as auth");
    final String path = methodNonauthPath.toString();
    mkdirs(methodNonauthPath);
    // auth import
    exec(importTool(),
        IMPORT,
        f(AUTH_FLAG),
        f(VERBOSE),
        path);
    exec(authTool(),
        AUDIT,
        f(REQUIRE_AUTH),
        f(VERBOSE),
        path);
  }

  @Test
  public void testAuditS3GuardTooMissingDir() throws Throwable {
    describe("Test the s3guard audit against a missing path");
    expectExecResult(EXIT_NOT_FOUND,
        authTool(),
        AUDIT,
        methodAuthPath.toString());
  }

  /**
   * Touch a file in the authoritative fs.
   * @param file path of file
   * @throws IOException Failure
   */
  protected void touchFile(final Path file) throws IOException {
    ContractTestUtils.touch(authFS, file);
  }

  /**
   * Invoke an operation expecting the meta store to have its
   * directoryMarkedAuthoritative count to be be updated {@code updates}
   * times and S3 LIST requests made {@code lists} times.
   * @param <T> Return type
   * @param updates Expected count
   * @param lists Expected lists
   * @param fn Function to invoke
   * @return Result of the function call
   * @throws Exception Failure
   */
  private <T> T expectAuthoritativeUpdate(
      int updates,
      int lists,
      Callable<T> fn)
      throws Exception {
    S3ATestUtils.MetricDiff authDirsMarked = new S3ATestUtils.MetricDiff(authFS,
        S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED);
    S3ATestUtils.MetricDiff listRequests = new S3ATestUtils.MetricDiff(authFS,
        OBJECT_LIST_REQUEST);
    final T call = fn.call();
    authDirsMarked.assertDiffEquals(updates);
    listRequests.assertDiffEquals(lists);
    return call;
  }

  /**
   * Invoke an operation expecting {@code writes} records written to DDB.
   * @param <T> Return type
   * @param writes Expected count
   * @param fn Function to invoke
   * @return Result of the function call
   * @throws Exception Failure
   */
  private <T> T expectOperationUpdatesDDB(
      int writes,
      Callable<T> fn)
      throws Exception {
    S3ATestUtils.MetricDiff writeDiff = new S3ATestUtils.MetricDiff(authFS,
        S3GUARD_METADATASTORE_RECORD_WRITES);
    final T call = fn.call();
    writeDiff.assertDiffEquals(writes);
    return call;
  }

  /**
   * Creates a Path array from all items retrieved from
   * {@link RemoteIterator<LocatedFileStatus>}.
   *
   * @param remoteIterator iterator
   * @return a list of Paths
   * @throws IOException
   */
  private List<Path> toPaths(RemoteIterator<LocatedFileStatus> remoteIterator)
      throws IOException {
    List<Path> list = new ArrayList<>();
    while (remoteIterator.hasNext()) {
      LocatedFileStatus fileStatus = remoteIterator.next();
      list.add(fileStatus.getPath());
    }
    return list;
  }

  /**
   * Assert that a listStatus call increments the
   * "s3guard_metadatastore_authoritative_directories_updated" counter.
   * Then checks that the directory is recursively authoritative.
   * @param path path to scan
   */
  private void assertListUpdatesAuth(Path path) throws Exception {
    expectAuthoritativeUpdate(1, 1, () -> authFS.listStatus(path));
    expectAuthRecursive(path);
  }

  /**
   * Assert that a listStatus call does not increment the
   * "s3guard_metadatastore_authoritative_directories_updated" counter.
   * @param path path to scan
   */
  private void assertListDoesNotUpdateAuth(Path path) throws Exception {
    expectAuthoritativeUpdate(0, 0, () -> authFS.listStatus(path));
  }

  /**
   * Create a directory if needed, force it to be authoritatively listed.
   * @param path dir
   */
  private void mkAuthDir(Path path) throws IOException {
    authFS.mkdirs(path);
  }

  /**
   * Create a non-auth directory.
   * @param path dir
   */
  private void mkNonauthDir(Path path) throws IOException {
    authFS.mkdirs(path);
    // overwrite entry with a nonauth one
    markDirNonauth(path);
  }

  /**
   * Mark a directory as nonauth.
   * @param path path to the directory
   * @throws IOException failure
   */
  private void markDirNonauth(final Path path) throws IOException {
    S3Guard.putWithTtl(metastore,
        nonAuthEmptyDirectoryMarker((S3AFileStatus) authFS.getFileStatus(path)),
        null, null);
  }

  /**
   * Create an empty dir marker which, when passed to the
   * DDB metastore, is considered authoritative.
   * @param status file status
   * @return path metadata.
   */
  private PathMetadata nonAuthEmptyDirectoryMarker(
      final S3AFileStatus status) {
    return new DDBPathMetadata(status, Tristate.TRUE,
        false, false, 0);
  }
  /**
   * Performed a recursive audit of the directory
   * -require everything to be authoritative.
   * @param path directory
   */
  private void expectAuthRecursive(Path path) throws Exception {
    auditor.executeAudit(path, true, true);
  }

  /**
   * Performed a non-recursive audit of the directory
   * -require the directory to be authoritative.
   * @param path directory
   */
  private void expectAuthNonRecursive(Path path) throws Exception {
    auditor.executeAudit(path, true, false);
  }

  /**
   * Performed a recursive audit of the directory
   * -expect a failure.
   * @param path directory
   * @return the path returned by the exception
   */
  private Path expectNonauthRecursive(Path path) throws Exception {
    return intercept(
        AuthoritativeAuditOperation.NonAuthoritativeDirException.class,
        () -> auditor.executeAudit(path, true, true))
        .getPath();
  }

  /**
   * Performed a recursive audit of the directory
   * -expect a failure.
   * @param path directory
   * @return the path returned by the exception
   */
  private Path expectNonauthNonRecursive(Path path) throws Exception {
    return intercept(
        AuthoritativeAuditOperation.NonAuthoritativeDirException.class,
        () -> auditor.executeAudit(path, true, true))
        .getPath();
  }

}
