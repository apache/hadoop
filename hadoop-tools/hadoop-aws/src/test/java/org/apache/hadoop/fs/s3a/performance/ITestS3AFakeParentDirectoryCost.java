/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.apache.hadoop.fs.s3a.Constants.CREATE_FAKE_PARENT_DIRECTORY;
import static org.apache.hadoop.fs.s3a.Statistic.DIRECTORIES_CREATED;
import static org.apache.hadoop.fs.s3a.Statistic.DIRECTORIES_DELETED;
import static org.apache.hadoop.fs.s3a.Statistic.FAKE_DIRECTORIES_DELETED;
import static org.apache.hadoop.fs.s3a.Statistic.FILES_DELETED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_COPY_REQUESTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_OBJECTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.COPY_OP;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.DELETE_MARKER_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.DELETE_OBJECT_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_DIR_PROBE_H;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_DIR_PROBE_L;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_FILE_PROBE_H;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_FILE_PROBE_L;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILE_STATUS_FILE_PROBE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.GET_FILE_STATUS_FNFE;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.GET_FILE_STATUS_ON_DIR;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.RENAME_SINGLE_FILE_SAME_DIR;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use metrics to assert about the cost of file API calls.
 * <p></p>
 * Parameterized on directory marker keep vs delete.
 * There's extra complexity related to bulk/non-bulk delete calls.
 * If bulk deletes are disabled, many more requests are made to delete
 * parent directories. The counters of objects deleted are constant
 * irrespective of the delete mode.
 */
@RunWith(Parameterized.class)
public class ITestS3AFakeParentDirectoryCost extends AbstractS3ACostTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AFakeParentDirectoryCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"keep-markers", true},
        {"delete-markers", false},
    });
  }

  public ITestS3AFakeParentDirectoryCost(final String name,
      final boolean keepMarkers) {
    super(keepMarkers);
  }

  @Override
  public void teardown() throws Exception {
    if (isKeepingMarkers()) {
      // do this ourselves to avoid audits teardown failing
      // when surplus markers are found
      deleteTestDirInTeardown();
    }
    super.teardown();
  }

  @Override
  protected Configuration getConfiguration() {
    Configuration conf = super.getConfiguration();
    conf.setBoolean(CREATE_FAKE_PARENT_DIRECTORY, false);
    return conf;
  }

  /**
   * This creates a directory with a child and then deletes it.
   * The parent dir must be found and declared as empty.
   * <p>When deleting markers, that forces the recreation of a new marker.</p>
   */
  @Test
  public void testDeleteSingleFileInDir() throws Throwable {
    describe("delete a file");
    S3AFileSystem fs = getFileSystem();
    // creates the marker
    Path dir = dir(methodPath());
    // file creation may have deleted that marker, but it may
    // still be there
    Path simpleFile = file(new Path(dir, "simple.txt"));

    boolean rawAndKeeping = !isDeleting();
    boolean rawAndDeleting = isDeleting();
    verifyMetrics(() -> {
          fs.delete(simpleFile, false);
          return "after fs.delete(simpleFile) " + getMetricSummary();
        },
        probe(rawAndKeeping, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        // if deleting markers, look for the parent too
        probe(rawAndDeleting, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H + FILESTATUS_DIR_PROBE_H),
        with(OBJECT_LIST_REQUEST,
            0),
        with(DIRECTORIES_DELETED, 0),
        with(FILES_DELETED, 1),

        // a single DELETE call is made to delete the object
        with(OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST),

        // keeping: create no parent dirs or delete parents
        withWhenKeeping(DIRECTORIES_CREATED, 0),
        withWhenKeeping(OBJECT_BULK_DELETE_REQUEST, 0),

        // deleting: create a parent and delete any of its parents
        withWhenDeleting(DIRECTORIES_CREATED, isDeleting() ? 0 : 1),
        // a bulk delete for all parents is issued.
        // the number of objects in it depends on the depth of the tree;
        // don't worry about that
        withWhenDeleting(OBJECT_BULK_DELETE_REQUEST,  0)
                 );
  }

  /**
   * This creates a directory with a two files and then deletes one of the
   * files.
   */
  @Test
  public void testDeleteFileInDir() throws Throwable {
    describe("delete a file in a directory with multiple files");
    S3AFileSystem fs = getFileSystem();
    // creates the marker
    Path dir = dir(methodPath());
    // file creation may have deleted that marker, but it may
    // still be there
    Path file1 = file(new Path(dir, "file1.txt"));
    Path file2 = file(new Path(dir, "file2.txt"));

    boolean rawAndKeeping = !isDeleting();
    boolean rawAndDeleting = isDeleting();
    verifyMetrics(() -> {
          fs.delete(file1, false);
          return "after fs.delete(file1) " + getMetricSummary();
        },
        // delete file. For keeping: that's it
        probe(rawAndKeeping, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        // if deleting markers, look for the parent too
        probe(rawAndDeleting, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H + FILESTATUS_DIR_PROBE_H),
        with(OBJECT_LIST_REQUEST,
            0),
        with(DIRECTORIES_DELETED, 0),
        with(FILES_DELETED, 1),

        // no need to create a parent
        with(DIRECTORIES_CREATED, 0),

        // keeping: create no parent dirs or delete parents
        withWhenKeeping(OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST),

        // deleting: create a parent and delete any of its parents
        withWhenDeleting(OBJECT_DELETE_REQUEST,
            DELETE_OBJECT_REQUEST));
  }

  @Test
  public void testDirMarkersSubdir() throws Throwable {
    describe("verify cost of deep subdir creation");

    Path methodPath = methodPath();
    Path parent = new Path(methodPath, "parent");
    Path subDir = new Path(parent, "1/2/3/4/5/6");
    S3AFileSystem fs = getFileSystem();
    // this creates a peer of the parent dir, so ensures
    // that when parent dir is deleted, no markers need to
    // be recreated...that complicates all the metrics which
    // are measured
    Path sibling = new Path(methodPath, "sibling");
    ContractTestUtils.touch(fs, sibling);

    int dirsCreated = 2;
    fs.delete(parent, true);

    LOG.info("creating parent dir {}", parent);
    fs.mkdirs(parent);

    LOG.info("creating sub directory {}", subDir);
    // one dir created, possibly a parent removed
    final int fakeDirectoriesToDelete = directoriesInPath(subDir) - 1;
    verifyMetrics(() -> {
          mkdirs(subDir);
          return "after mkdir(subDir) " + getMetricSummary();
        },
        with(DIRECTORIES_CREATED, 1),
        with(DIRECTORIES_DELETED, 0),
        withWhenKeeping(getDeleteMarkerStatistic(), 0),
        withWhenKeeping(FAKE_DIRECTORIES_DELETED, 0),
        withWhenDeleting(getDeleteMarkerStatistic(),
            isBulkDelete() ? DELETE_MARKER_REQUEST : fakeDirectoriesToDelete),
        // delete all possible fake dirs above the subdirectory
        withWhenDeleting(FAKE_DIRECTORIES_DELETED,
            fakeDirectoriesToDelete));

    LOG.info("About to delete {}", parent);
    // now delete the deep tree.
    verifyMetrics(() -> {
          fs.delete(parent, true);
          return "deleting parent dir " + parent + " " + getMetricSummary();
        },

        // keeping: the parent dir marker needs deletion alongside
        // the subdir one.
        withWhenKeeping(OBJECT_DELETE_OBJECTS, dirsCreated),

        // deleting: only the marker at the bottom needs deleting
        withWhenDeleting(OBJECT_DELETE_OBJECTS, 1));

    // followup with list calls to make sure all is clear.
    verifyNoListing(parent);
    verifyNoListing(subDir);
    // now reinstate the directory, which in HADOOP-17244 hitting problems
    fs.mkdirs(parent);
    FileStatus[] children = fs.listStatus(parent);
    Assertions.assertThat(children)
        .describedAs("Children of %s", parent)
        .isEmpty();
  }

  /**
   * List a path, verify that there are no direct child entries.
   * @param path path to scan
   */
  protected void verifyNoListing(final Path path) throws Exception {
    intercept(FileNotFoundException.class, () -> {
      FileStatus[] statuses = getFileSystem().listStatus(path);
      return Arrays.deepToString(statuses);
    });
  }

  @Test
  public void testDirMarkersFileCreation() throws Throwable {
    describe("verify cost of file creation");

    Path srcBaseDir = dir(methodPath());

    Path srcDir = dir(new Path(srcBaseDir, "1/2/3/4/5/6"));

    // creating a file should trigger demise of the src dir marker
    // unless markers are being kept

    final int directories = directoriesInPath(srcDir);
    verifyMetrics(() -> {
          final Path srcPath = new Path(srcDir, "source.txt");
          file(srcPath);
          LOG.info("Metrics: {}\n{}", getMetricSummary(), getFileSystem());
          return "after touch(fs, " + srcPath + ")" + getMetricSummary();
        },
        with(DIRECTORIES_CREATED, 0),
        with(DIRECTORIES_DELETED, 0),
        // keeping: no delete operations.
        withWhenKeeping(getDeleteMarkerStatistic(), 0),
        withWhenKeeping(FAKE_DIRECTORIES_DELETED, 0),
        // delete all possible fake dirs above the file
        withWhenDeleting(FAKE_DIRECTORIES_DELETED,
            directories),
        withWhenDeleting(getDeleteMarkerStatistic(),
            isBulkDelete() ? 1: directories));
  }

  @Test
  public void testRenameSameDirectory() throws Throwable {
    describe("rename a file to the same directory");

    Path baseDir = dir(methodPath());
    final Path sourceFile = file(new Path(baseDir, "source.txt"));

    // create a new source file.
    // Explicitly use a new path object to guarantee that the parent paths
    // are different object instances and so equals() rather than ==
    // is
    Path parent2 = sourceFile.getParent();
    Path destFile = new Path(parent2, "dest");
    verifyMetrics(() ->
            execRename(sourceFile, destFile),
        always(RENAME_SINGLE_FILE_SAME_DIR),
        with(OBJECT_COPY_REQUESTS, 1),
        with(DIRECTORIES_CREATED, 0),
        with(OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST),
        with(FAKE_DIRECTORIES_DELETED, 0));
  }

  @Test
  public void testCostOfRootFileRename() throws Throwable {
    describe("assert that a root file rename doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = file(new Path("/src-" + uuid));
    Path dest = new Path("/dest-" + uuid);
    try {
      verifyMetrics(() -> {
            fs.rename(src, dest);
            return "after fs.rename(/src,/dest) " + getMetricSummary();
          },
          always(FILE_STATUS_FILE_PROBE
              .plus(GET_FILE_STATUS_FNFE)
              .plus(COPY_OP)),
          // here we expect there to be no fake directories
          with(DIRECTORIES_CREATED, 0),
          // one for the renamed file only
          with(OBJECT_DELETE_REQUEST,
              DELETE_OBJECT_REQUEST),
          // no directories are deleted: This is root
          with(DIRECTORIES_DELETED, 0),
          // no fake directories are deleted: This is root
          with(FAKE_DIRECTORIES_DELETED, 0),
          with(FILES_DELETED, 1));
    } finally {
      fs.delete(src, false);
      fs.delete(dest, false);
    }
  }

  @Test
  public void testCostOfRootFileDelete() throws Throwable {
    describe("assert that a root file delete doesn't"
        + " do much in terms of parent dir operations");
    S3AFileSystem fs = getFileSystem();

    // unique name, so that even when run in parallel tests, there's no conflict
    String uuid = UUID.randomUUID().toString();
    Path src = file(new Path("/src-" + uuid));
    try {
      // delete that destination file, assert only the file delete was issued
      verifyMetrics(() -> {
            fs.delete(src, false);
            return "after fs.delete(/dest) " + getMetricSummary();
          },
          with(DIRECTORIES_CREATED, 0),
          with(DIRECTORIES_DELETED, 0),
          with(FAKE_DIRECTORIES_DELETED, 0),
          with(FILES_DELETED, 1),
          with(OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST),
          always(FILE_STATUS_FILE_PROBE)); /* no need to look at parent. */

    } finally {
      fs.delete(src, false);
    }
  }

}
