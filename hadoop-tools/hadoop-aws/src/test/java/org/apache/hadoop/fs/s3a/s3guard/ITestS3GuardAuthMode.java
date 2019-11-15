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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUESTS;
import static org.apache.hadoop.fs.s3a.Statistic.S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test to verify the expected behaviour of DynamoDB and authoritative mode.
 * The main testFS is non-auth; we also create a test FS which runs in auth mode.
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
public class ITestS3GuardAuthMode extends AbstractS3ATestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3GuardAuthMode.class);

  private StoreContext storeContext;

  private String fsUriStr;

  private DynamoDBMetadataStore metastore;

  private static S3AFileSystem authFS;

  private static Path basePath;

  private static Path authPath;

  private static Path nonauthPath;

  private Path methodAuthPath;

  private Path methodNonauthPath;

  private S3GuardAuthoritativeAudit auditor;

  @AfterClass
  public static void closeAuthFS() {
    IOUtils.cleanupWithLogger(LOG, authFS);
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

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();
    S3ATestUtils.assumeS3GuardState(true, conf);
    storeContext = fs.createStoreContext();
    assume("Filesystem isn't running DDB",
        storeContext.getMetadataStore() instanceof DynamoDBMetadataStore);
    metastore = (DynamoDBMetadataStore) storeContext.getMetadataStore();
    URI fsURI = storeContext.getFsURI();
    fsUriStr = fsURI.toString();
    if (!fsUriStr.endsWith("/")) {
      fsUriStr = fsUriStr + "/";
    }
    auditor = new S3GuardAuthoritativeAudit(metastore, true);


    if (authFS == null) {
      // creating the test FS.
      basePath = path("base");
      authPath = new Path(basePath, "auth");
      nonauthPath = new Path(basePath, "nonauth");
      final Configuration authconf = new Configuration(conf);
      final URI uri = authPath.toUri();
      authconf.set(AUTHORITATIVE_PATH, uri.toString());
      authconf.setBoolean(METADATASTORE_AUTHORITATIVE, true);
      authFS = (S3AFileSystem) FileSystem.newInstance(uri, authconf);
    }
    cleanupMethodPaths();
  }

  @Override
  public void teardown() throws Exception {
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

  @Test
  @Ignore("Needs mkdir to be authoritative")
  public void testMkDirAuth() throws Throwable {
    describe("create an empty dir and assert it is tagged as authoritative");
    final Path dir = new Path(methodAuthPath, "dir");
    authFS.mkdirs(dir);
    expectAuthRecursive(dir);
    expectAuthRecursive(methodAuthPath);
  }

  @Test
  public void testListStatusMakesEmptyDirAuth() throws Throwable {
    describe("Verify listStatus marks an Empty dir as auth");
    final Path dir = new Path(methodAuthPath, "emptydir");
    authFS.mkdirs(dir);
    expectNonauth(dir);
    authFS.listStatus(dir);
    // dir is auth; subdir is not
    expectAuthRecursive(dir);
    // Next list will not go to s3
    assertListDoesNotUpdateAuth(dir);
  }

  @Test
  public void testListStatusMakesDirAuth() throws Throwable {
    describe("Verify listStatus marks a dir as auth");
    final Path dir = new Path(methodAuthPath, "lsdir");
    final Path subdir = new Path(dir, "subdir");

    mkAuthDir(dir);
    expectAuthRecursive(dir);
    authFS.mkdirs(subdir);
    // dir is auth; subdir is not
    expectAuthNonRecursive(dir);
    expectNonauth(dir);
    assertListDoesNotUpdateAuth(dir);
    // Subdir list makes it auth
    assertListUpdatesAuth(subdir);
  }

  @Test
  public void testAddFileMarksNonAuth() throws Throwable {
    describe("adding a file marks dir as nonauth");
    final Path dir = methodAuthPath;
    final Path file = new Path(dir, "testAddFileMarksNonAuth");

    touchAuth(file);
    expectNonauth(dir);
    assertListUpdatesAuth(dir);
  }

  @Test
  public void testDeleteFileMarksNonAuth() throws Throwable {
    describe("adding a file marks dir as nonauth");
    final Path dir = methodAuthPath;
    final Path file = new Path(dir, "testDeleteFileMarksNonAuth");

    touchAuth(file);
    assertListUpdatesAuth(dir);
    authFS.delete(file, false);
    expectNonauth(dir);
  }

  @Test
  public void testPruneFilesMarksNonAuth() throws Throwable {
    describe("adding a file marks dir as nonauth");
    final Path dir = methodAuthPath;
    final Path file = new Path(dir, "file");

    touchAuth(file);
    assertListUpdatesAuth(dir);
    String keyPrefix
        = PathMetadataDynamoDBTranslation.pathToParentKey(dir);
    Assertions.assertThat(
        metastore.prune(
            MetadataStore.PruneMode.ALL_BY_MODTIME,
            Long.MAX_VALUE,
            keyPrefix))
        .describedAs("Prune of keys under %s", keyPrefix)
        .isEqualTo(1);
    expectNonauth(dir);
  }

  @Test
  public void testPruneTombstoneRetainsAuth() throws Throwable {
    describe("adding a file marks dir as nonauth");
    final Path dir = methodAuthPath;
    final Path file = new Path(dir, "file");

    touchAuth(file);
    assertListUpdatesAuth(dir);
    authFS.delete(file, false);
    expectNonauth(dir);
    assertListUpdatesAuth(dir);
    String keyPrefix
        = PathMetadataDynamoDBTranslation.pathToParentKey(dir);
    Assertions.assertThat(
        metastore.prune(
            MetadataStore.PruneMode.TOMBSTONES_BY_LASTUPDATED,
            Long.MAX_VALUE,
            keyPrefix))
        .describedAs("Prune of keys under %s", keyPrefix)
        .isEqualTo(1);
    expectAuthRecursive(dir);
  }

  @Test
  public void testRenameFile() throws Throwable {
    describe("renaming a file");
    final Path dir = methodAuthPath;
    final Path source = new Path(dir, "source");
    final Path dest = new Path(dir, "dest");
    touchAuth(source);
    assertListUpdatesAuth(dir);
    authFS.rename(source, dest);
    expectNonauth(dir);
  }

  @Test
  public void testRenameDirMarksDestAsAuth() throws Throwable {
    describe("renaming a file");
    final Path dir = methodAuthPath;
    final Path source = new Path(dir, "source");
    final Path dest = new Path(dir, "dest");
    mkAuthDir(source);
    Path file = new Path(source, "subdir/file");
    touchAuth(file);
    authFS.rename(source, dest);
    expectNonauth(dir);
    expectAuthRecursive(dest);
  }

  protected void touchAuth(final Path file) throws IOException {
    ContractTestUtils.touch(authFS, file);
  }


  /**
   * Invoke an operation expecting the meta store to be updated{@code updates}
   * times and S3 LIST requests made {@code lists} times.
   * @param <T> Return type
   * @param updates Expected count
   * @param lists Expected lists
   * @param fn Function to invoke
   * @return Result of the function call
   * @throws Exception Failure
   */
  private <T> T expectAuthoritativeUpdate(int updates,
      int lists,
      Callable<T> fn)
      throws Exception {
    S3ATestUtils.MetricDiff authDirsMarked = new S3ATestUtils.MetricDiff(authFS,
        S3GUARD_METADATASTORE_AUTHORITATIVE_DIRECTORIES_UPDATED);
    S3ATestUtils.MetricDiff listRequests = new S3ATestUtils.MetricDiff(authFS,
        OBJECT_LIST_REQUESTS);
    final T call = fn.call();
    authDirsMarked.assertDiffEquals(updates);
    listRequests.assertDiffEquals(lists);
    return call;
  }

  private void assertListUpdatesAuth(Path path) throws Exception {
    expectAuthoritativeUpdate(1, 1, () -> authFS.listStatus(path));
    expectAuthRecursive(path);
  }

  private void assertListDoesNotUpdateAuth(Path path) throws Exception {
    expectAuthoritativeUpdate(0, 0, () -> authFS.listStatus(path));
  }


  /**
   * Create a directory if needed, force it to be authoritatively listed.
   * @param dir dir
   */
  private void mkAuthDir(Path dir) throws IOException {
    authFS.mkdirs(dir);
    authFS.listStatus(dir);
  }

  private void expectAuthRecursive(Path dir) throws Exception {
    auditor.executeAudit(dir, true, true);
  }

  private void expectAuthNonRecursive(Path dir) throws Exception {
    auditor.executeAudit(dir, true, false);
  }

  private void expectNonauth(Path dir) throws Exception {
    intercept(S3GuardAuthoritativeAudit.NonAuthoritativeDirException.class,
        () -> auditor.executeAudit(dir, true, true));
  }
  // test rename (aut, auth) -> auth
  // test touch(auth) -> nonauth

}
