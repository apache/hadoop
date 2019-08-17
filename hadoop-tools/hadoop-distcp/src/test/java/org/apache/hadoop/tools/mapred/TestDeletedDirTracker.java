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

package org.apache.hadoop.tools.mapred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.CopyListingFileStatus;

/**
 * Unit tests of the deleted directory tracker.
 */
@SuppressWarnings("RedundantThrows")
public class TestDeletedDirTracker extends Assert {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDeletedDirTracker.class);

  public static final Path ROOT = new Path("hdfs://namenode/");

  public static final Path DIR1 = new Path(ROOT, "dir1");

  public static final Path FILE0 = new Path(ROOT, "file0");

  public static final Path DIR1_FILE1 = new Path(DIR1, "file1");

  public static final Path DIR1_FILE2 = new Path(DIR1, "file2");

  public static final Path DIR1_DIR3 = new Path(DIR1, "dir3");

  public static final Path DIR1_DIR3_DIR4 = new Path(DIR1_DIR3, "dir4");

  public static final Path DIR1_DIR3_DIR4_FILE_3 =
      new Path(DIR1_DIR3_DIR4, "file1");


  private DeletedDirTracker tracker;

  @Before
  public void setup() {
    tracker = new DeletedDirTracker(1000);
  }

  @After
  public void teardown() {
    LOG.info(tracker.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoRootDir() throws Throwable {
    shouldDelete(ROOT, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoRootFile() throws Throwable {
    shouldDelete(dirStatus(ROOT));
  }

  @Test
  public void testFileInRootDir() throws Throwable {
    expectShouldDelete(FILE0, false);
    expectShouldDelete(FILE0, false);
  }

  @Test
  public void testDeleteDir1() throws Throwable {
    expectShouldDelete(DIR1, true);
    expectShouldNotDelete(DIR1, true);
    expectShouldNotDelete(DIR1_FILE1, false);
    expectNotCached(DIR1_FILE1);
    expectShouldNotDelete(DIR1_DIR3, true);
    expectCached(DIR1_DIR3);
    expectShouldNotDelete(DIR1_FILE2, false);
    expectShouldNotDelete(DIR1_DIR3_DIR4_FILE_3, false);
    expectShouldNotDelete(DIR1_DIR3_DIR4, true);
    expectShouldNotDelete(DIR1_DIR3_DIR4, true);
  }

  @Test
  public void testDeleteDirDeep() throws Throwable {
    expectShouldDelete(DIR1, true);
    expectShouldNotDelete(DIR1_DIR3_DIR4_FILE_3, false);
  }

  @Test
  public void testDeletePerfectCache() throws Throwable {
    // run a larger scale test. Also use the ordering we'd expect for a sorted
    // listing, which we implement by sorting the paths
    List<CopyListingFileStatus> statusList = buildStatusList();
    // cache is bigger than the status list
    tracker = new DeletedDirTracker(statusList.size());

    AtomicInteger deletedFiles = new AtomicInteger(0);
    AtomicInteger deletedDirs = new AtomicInteger(0);
    deletePaths(statusList, deletedFiles, deletedDirs);
    assertEquals(0, deletedFiles.get());
  }

  @Test
  public void testDeleteFullCache() throws Throwable {
    // run a larger scale test. Also use the ordering we'd expect for a sorted
    // listing, which we implement by sorting the paths
    AtomicInteger deletedFiles = new AtomicInteger(0);
    AtomicInteger deletedDirs = new AtomicInteger(0);
    deletePaths(buildStatusList(), deletedFiles, deletedDirs);
    assertEquals(0, deletedFiles.get());
  }

  @Test
  public void testDeleteMediumCache() throws Throwable {
    tracker = new DeletedDirTracker(100);
    AtomicInteger deletedFiles = new AtomicInteger(0);
    AtomicInteger deletedDirs = new AtomicInteger(0);
    deletePaths(buildStatusList(), deletedFiles, deletedDirs);
    assertEquals(0, deletedFiles.get());
  }

  @Test
  public void testDeleteFullSmallCache() throws Throwable {
    tracker = new DeletedDirTracker(10);
    AtomicInteger deletedFiles = new AtomicInteger(0);
    AtomicInteger deletedDirs = new AtomicInteger(0);
    deletePaths(buildStatusList(), deletedFiles, deletedDirs);
    assertEquals(0, deletedFiles.get());
  }

  protected void deletePaths(final List<CopyListingFileStatus> statusList,
      final AtomicInteger deletedFiles, final AtomicInteger deletedDirs) {
    for (CopyListingFileStatus status : statusList) {
      if (shouldDelete(status)) {
        AtomicInteger r = status.isDirectory() ? deletedDirs : deletedFiles;
        r.incrementAndGet();
        LOG.info("Delete {}", status.getPath());
      }
    }

    LOG.info("After proposing to delete {} paths, {} directories and {} files"
            + " were explicitly deleted from a cache {}",
        statusList.size(), deletedDirs, deletedFiles, tracker);
  }

  /**
   * Build a large YMD status list; 30 * 12 * 10 directories,
   * each with 24 files.
   * @return a sorted list.
   */
  protected List<CopyListingFileStatus> buildStatusList() {
    List<CopyListingFileStatus> statusList = new ArrayList<>();
    // recursive create of many files
    for (int y = 0; y <= 20; y++) {
      Path yp = new Path(String.format("YEAR=%d", y));
      statusList.add(dirStatus(yp));
      for (int m = 1; m <= 12; m++) {
        Path ymp = new Path(yp, String.format("MONTH=%d", m));
        statusList.add(dirStatus(ymp));
        for (int d = 1; d < 30; d++) {
          Path dir = new Path(ymp, String.format("DAY=%02d", d));
          statusList.add(dirStatus(dir));
          for (int h = 0; h < 24; h++) {
            statusList.add(fileStatus(new Path(dir,
                String.format("%02d00.avro", h))));
          }
        }
      }
      // sort on paths.
      Collections.sort(statusList,
          (l, r) -> l.getPath().compareTo(r.getPath()));
    }
    return statusList;
  }


  private void expectShouldDelete(final Path path, boolean isDir) {
    expectShouldDelete(newStatus(path, isDir));
  }

  private void expectShouldDelete(CopyListingFileStatus status) {
    assertTrue("Expected shouldDelete of " + status.getPath(),
        shouldDelete(status));
  }

  private boolean shouldDelete(final Path path, final boolean isDir) {
    return shouldDelete(newStatus(path, isDir));
  }

  private boolean shouldDelete(final CopyListingFileStatus status) {
    return tracker.shouldDelete(status);
  }

  private void expectShouldNotDelete(final Path path, boolean isDir) {
    expectShouldNotDelete(newStatus(path, isDir));
  }

  private void expectShouldNotDelete(CopyListingFileStatus status) {
    assertFalse("Expected !shouldDelete of " + status.getPath()
            + " but got true",
        shouldDelete(status));
  }

  private CopyListingFileStatus newStatus(final Path path,
      final boolean isDir) {
    return new CopyListingFileStatus(new FileStatus(0, isDir, 0, 0, 0, path));
  }

  private CopyListingFileStatus dirStatus(final Path path) {
    return newStatus(path, true);
  }

  private CopyListingFileStatus fileStatus(final Path path) {
    return newStatus(path, false);
  }

  private void expectCached(final Path path) {
    assertTrue("Path " + path + " is not in the cache of " + tracker,
        tracker.isContained(path));
  }

  private void expectNotCached(final Path path) {
    assertFalse("Path " + path + " is in the cache of " + tracker,
        tracker.isContained(path));
  }

}
