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


import java.io.FileNotFoundException;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.setPerformanceFlags;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Use metrics to assert about the cost of file API calls.
 * <p>
 * There's extra complexity related to bulk/non-bulk delete calls.
 * If bulk deletes are disabled, many more requests are made to delete
 * parent directories. The counters of objects deleted are constant
 * irrespective of the delete mode.
 */
public class ITestS3ADeleteCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ADeleteCost.class);

  @Override
  public Configuration createConfiguration() {
    return setPerformanceFlags(
        super.createConfiguration(),
        "");
  }

  @Override
  public void teardown() throws Exception {
    // do this ourselves to avoid audits teardown failing
    // when surplus markers are found
    deleteTestDirInTeardown();
    super.teardown();
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
    Path simpleFile = file(new Path(dir, "simple.txt"));

    boolean bulkDelete = isBulkDelete();
    verifyMetrics(() -> {
          fs.delete(simpleFile, false);
          return "after fs.delete(simpleFile) " + getMetricSummary();
        },
        probe(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        with(OBJECT_LIST_REQUEST,
            FILESTATUS_FILE_PROBE_L + FILESTATUS_DIR_PROBE_L),
        with(DIRECTORIES_DELETED, 0),
        with(FILES_DELETED, 1),

        // a single DELETE call is made to delete the object
        probe(bulkDelete, OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST),
        probe(!bulkDelete, OBJECT_DELETE_REQUEST,
            DELETE_OBJECT_REQUEST + DELETE_MARKER_REQUEST),

        // keeping: create no parent dirs or delete parents
        with(DIRECTORIES_CREATED, 0),
        with(OBJECT_BULK_DELETE_REQUEST, 0)
    );

    // there is an empty dir for a parent
    S3AFileStatus status = verifyInnerGetFileStatus(dir, true,
        StatusProbeEnum.ALL, GET_FILE_STATUS_ON_DIR);
    assertEmptyDirStatus(status, Tristate.TRUE);
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

    verifyMetrics(() -> {
      fs.delete(file1, false);
      return "after fs.delete(file1) " + getMetricSummary();
    },
        // delete file.
        probe(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        with(OBJECT_LIST_REQUEST,
            FILESTATUS_FILE_PROBE_L + FILESTATUS_DIR_PROBE_L),
        with(DIRECTORIES_DELETED, 0),
        with(FILES_DELETED, 1),

        // no need to create a parent
        with(DIRECTORIES_CREATED, 0),

        // keeping: create no parent dirs or delete parents
        with(OBJECT_DELETE_REQUEST, DELETE_OBJECT_REQUEST));
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
    final Statistic stat = getDeleteMarkerStatistic();
    verifyMetrics(() -> {
      mkdirs(subDir);
      return "after mkdir(subDir) " + getMetricSummary();
    },
        with(DIRECTORIES_CREATED, 1),
        with(DIRECTORIES_DELETED, 0),
        with(stat, 0),
        with(FAKE_DIRECTORIES_DELETED, 0));

    LOG.info("About to delete {}", parent);
    // now delete the deep tree.
    verifyMetrics(() -> {
      fs.delete(parent, true);
      return "deleting parent dir " + parent + " " + getMetricSummary();
    },

        // the parent dir marker needs deletion alongside
        // the subdir one.
        with(OBJECT_DELETE_OBJECTS, dirsCreated));

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

    final Statistic stat = getDeleteMarkerStatistic();
    verifyMetrics(() -> {
      final Path srcPath = new Path(srcDir, "source.txt");
      file(srcPath);
      LOG.info("Metrics: {}\n{}", getMetricSummary(), getFileSystem());
      return "after touch(fs, " + srcPath + ")" + getMetricSummary();
    },
        with(DIRECTORIES_CREATED, 0),
        with(DIRECTORIES_DELETED, 0),
        // no delete operations.
        with(stat, 0),
        with(FAKE_DIRECTORIES_DELETED, 0));
  }

}
