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

package org.apache.hadoop.fs.s3a.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.createTestPath;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This is a test suite designed to verify that directory markers do
 * not get misconstrued as empty directories during operations
 * which explicitly or implicitly list directory trees.
 * <p></p>
 * It is also intended it to be backported to all releases
 * which are enhanced to read directory trees where markers have
 * been retained.
 * Hence: it does not use any of the new helper classes to
 * measure the cost of operations or attempt to create markers
 * through the FS APIs.
 * <p></p>
 * Instead, the directory structure to test is created through
 * low-level S3 SDK API calls.
 * We also skip any probes to measure/assert metrics.
 * We're testing the semantics here, not the cost of the operations.
 * Doing that makes it a lot easier to backport.
 *
 * <p></p>
 * Similarly: JUnit assertions over AssertJ.
 * <p></p>
 * The tests work with unguarded buckets only.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestDirectoryMarkerListing extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestDirectoryMarkerListing.class);

  private static final String NAME = "ITestDirectoryMarkerListing";

  private static final String FILENAME = "fileUnderMarker";

  private static final String HELLO = "hello";

  private Path markerDir;

  private String markerKey;

  private String markerKeySlash;

  private String bucket;

  private AmazonS3 s3client;

  private String fileUnderMarker;

  private Path pathUnderMarker;

  private Path basePath;

  private Path markerPeer;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    removeBaseAndBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    assume("unguarded FS only",
        !fs.hasMetadataStore());

    basePath = fs.qualify(createTestPath(new Path("/base-" + NAME)));
    markerDir = new Path(basePath, "marker");
    // peer path has the same initial name to make sure there
    // is no confusion there.
    markerPeer = new Path(basePath, "markerpeer");
    markerKey = fs.pathToKey(markerDir);
    markerKeySlash = markerKey + "/";
    fileUnderMarker = markerKeySlash + FILENAME;
    pathUnderMarker = new Path(markerDir, FILENAME);
    bucket = fs.getBucket();
    s3client = fs.getAmazonS3ClientForTesting("markers");
  }


  @Test
  public void test_010_createMarker() throws Throwable {
    describe("Create the test markers for the suite");
    createTestObjects();
    head(markerKeySlash);
    assertIsDirectory(markerDir);
  }

  @Test
  public void test_020_objectUnderMarker() throws Throwable {
    assertIsFile(pathUnderMarker);
    assertIsDirectory(markerDir);
    head(fileUnderMarker);

  }

  @Test
  public void test_030_listStatus() throws Throwable {
    assumeTestObjectsExist();
    containsFileUnderMarkerOnly(
        toList(getFileSystem().listStatus(markerDir)));
  }


  /*
  =================================================================
    The listing operations
  =================================================================
  */

  @Test
  public void test_040_listStatus() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses = toList(
        getFileSystem().listFiles(markerDir, false));
    containsFileUnderMarkerOnly(statuses);
  }

  @Test
  public void test_050_listStatusRecursive() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses = toList(
        getFileSystem().listFiles(markerDir, true));
    containsFileUnderMarkerOnly(statuses);
  }

  /**
   * Path listing above the base dir MUST only find the file
   * and not the marker.
   */
  @Test
  public void test_060_listStatusBaseRecursive() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses = toList(
        getFileSystem().listFiles(basePath, true));
    containsStatusOfPaths(statuses, pathUnderMarker, markerPeer);
  }

  @Test
  public void test_070_globStatusBaseRecursive() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses =
        exec("glob", () ->
            toList(getFileSystem().globStatus(new Path(basePath, "*"))));
    containsStatusOfPaths(statuses, markerDir, markerPeer);
    isFileAtPath(markerPeer, statuses.get(1));
  }

  @Test
  public void test_080_globStatusMarkerDir() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses =
        exec("glob", () ->
            toList(getFileSystem().globStatus(new Path(markerDir, "*"))));
    containsFileUnderMarkerOnly(statuses);
  }

  /**
   * The list here returns the marker peer and dir in a different order. Wny?
   *
   */
  @Test
  public void test_090_listLocatedStatusBaseDir() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses =
        exec("listLocatedStatus", () ->
            toList(getFileSystem().listLocatedStatus(basePath)));

    containsStatusOfPaths(statuses, markerPeer, markerDir);
  }

  @Test
  public void test_100_listLocatedStatusMarkerDir() throws Throwable {
    assumeTestObjectsExist();
    List<FileStatus> statuses =
        exec("listLocatedStatus", () ->
            toList(getFileSystem().listLocatedStatus(markerDir)));

    containsFileUnderMarkerOnly(statuses);
  }


  /*
  =================================================================
    Creation Rejection
  =================================================================
  */

  @Test
  public void test_200_create_no_overwrite_marker() throws Throwable {
    assumeTestObjectsExist();
    head(markerKeySlash);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().create(markerDir, false)));
    // dir is still there.
    head(markerKeySlash);
  }

  @Test
  public void test_210_create_no_overwrite_file() throws Throwable {
    head(fileUnderMarker);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().create(pathUnderMarker, false)));
    verifyTestObjectsExist();
  }

  @Test
  public void test_220_createfile_no_overwrite() throws Throwable {
    head(fileUnderMarker);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().createFile(pathUnderMarker)
                .overwrite(false)
                .build()));
    verifyTestObjectsExist();
  }

  /*
  =================================================================
    Rename.
    These tests use methodPaths for src and test
  =================================================================
  */

  @Test
  public void test_300_rename_base() throws Throwable {

  }

  /*
  =================================================================
    Delete.
  =================================================================
  */

  @Test
  public void test_900_delete() throws Throwable {
    assumeTestObjectsExist();
    S3AFileSystem fs = getFileSystem();
    intercept(PathIsNotEmptyDirectoryException.class, () ->
        fs.delete(markerDir, false));
    head(fileUnderMarker);
    fs.delete(markerDir, true);
    intercept(AmazonS3Exception.class, () ->
        head(fileUnderMarker));
    fs.delete(basePath, true);
  }

  /*
  =================================================================
    Utility methods and assertions.
  =================================================================
  */

  /**
   * Creates the test objects at the well known paths;
   * no probes for existence before or after.
   */
  private void createTestObjects() throws Exception {
    S3AFileSystem fs = getFileSystem();
    // put the empty dir
    fs.mkdirs(markerDir);
    touch(fs, markerPeer);
    put(fileUnderMarker, HELLO);
  }

  private void assumeTestObjectsExist() throws Exception {
    assumeExists(fileUnderMarker);
    assumeExists(markerKeySlash);
  }

  private void verifyTestObjectsExist() throws Exception {
    head(fileUnderMarker);
    head(markerKeySlash);
  }

  private void put(final String key, final String content) throws Exception {
    exec("PUT " + key, () ->
        s3client.putObject(bucket, key, content));
  }


  private ObjectMetadata head(final String key) throws Exception {
    return exec("HEAD " + key, () ->
        s3client.getObjectMetadata(bucket, key));
  }

  private void assumeExists(final String key) throws Exception {
    try {
      head(key);
    } catch (AmazonS3Exception e) {
      Assume.assumeTrue("object " + key + " not found", false);
    }
  }

  private String marker(Path path) throws Exception {
    String key = getFileSystem().pathToKey(path) + "/";
    put(key, "");
    return key;
  }


  private ObjectMetadata head(final Path path) throws Exception {
    return head(getFileSystem().pathToKey(path));
  }

  private <T> T exec(String op, Callable<T> call) throws Exception {
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try {
      return call.call();
    } finally {
      timer.end(op);
    }
  }

  private void containsFileUnderMarkerOnly(final List<FileStatus> statuses) {
    assertEquals("Status length",
        1, statuses.size());
    isFileUnderMarker(statuses.get(0));

  }

  /**
   * Expect the list of status to match that of the paths.
   * @param statuses status list
   * @param paths ordered varargs list of paths
   * @param <T> type of status
   */
  private <T extends FileStatus> void containsStatusOfPaths(
      List<T> statuses, Path... paths) {
    String summary = statuses.stream()
        .map(Object::toString)
        .collect(Collectors.joining(";"));
    assertEquals("mismatch in size of listing " + summary,
        paths.length, statuses.size());
    for (int i = 0; i < statuses.size(); i++) {
      assertEquals("Path mismatch at element " + i + " in " + summary,
          paths[i], statuses.get(i).getPath());
    }
  }

  private void isFileUnderMarker(final FileStatus stat) {
    isFileAtPath(pathUnderMarker, stat);
  }

  private void isFileAtPath(final Path path, final FileStatus stat) {
    assertTrue("Is not file " + stat,
        stat.isFile());
    assertName(path, stat);
  }

  private void assertName(final Path path, final FileStatus stat) {
    assertEquals("filename is not the expected path :" + stat,
        path, stat.getPath());
  }

  private <T extends FileStatus> List<FileStatus>
      toList(RemoteIterator<T> status) throws IOException {

    List<FileStatus> l = new ArrayList<>();
    while (status.hasNext()) {
      l.add(status.next());
    }
    return dump(l);

  }

  private <T extends FileStatus> List<FileStatus>
      toList(T[] status) throws IOException {
    List<FileStatus> l = Arrays.asList(status);
    return dump(l);
  }

  private <T> List<T> dump(List<T> l) {
    int c = 1;
    for (T t : l) {
      LOG.info("{}\t{}", c++, t);
    }
    return l;
  }


}
