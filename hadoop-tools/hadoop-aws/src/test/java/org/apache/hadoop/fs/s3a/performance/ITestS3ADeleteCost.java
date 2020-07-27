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


import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;

import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.performance.HeadListCosts.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;


/**
 * Use metrics to assert about the cost of file API calls.
 * Parameterized on guarded vs raw. and directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3ADeleteCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ADeleteCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw-keep-markers", false, true, false},
        {"raw-delete-markers", false, false, false},
        {"nonauth-keep-markers", true, true, false},
        {"auth-delete-markers", true, false, true}
    });
  }

  public ITestS3ADeleteCost(final String name,
      final boolean s3guard,
      final boolean keepMarkers,
      final boolean authoritative) {
    super(s3guard, keepMarkers, authoritative);
  }


  /**
   * This creates a directory with a child and then deletes it.
   * The parent dir must be found and declared as empty.
   */
  @Test
  public void testDeleteFile() throws Throwable {
    describe("performing getFileStatus on newly emptied directory");
    S3AFileSystem fs = getFileSystem();
    // creates the marker
    Path dir = dir(methodPath());
    // file creation may have deleted that marker, but it may
    // still be there
    Path simpleFile = file(new Path(dir, "simple.txt"));

    boolean rawAndKeeping = isRaw() && isDeleting();
    boolean rawAndDeleting = isRaw() && isDeleting();
    verifyMetrics(() -> {
          fs.delete(simpleFile, false);
          return "after fs.delete(simpleFile) " + getMetricSummary();
        },
        // delete file. For keeping: that's it
        probe(rawAndKeeping, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        // if deleting markers, look for the parent too
        probe(rawAndDeleting, OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H + FILESTATUS_DIR_PROBE_H),
        raw(OBJECT_LIST_REQUESTS,
            FILESTATUS_FILE_PROBE_L + FILESTATUS_DIR_PROBE_L),
        always(DIRECTORIES_DELETED, 0),
        always(FILES_DELETED, 1),

        // keeping: create no parent dirs or delete parents
        keeping(DIRECTORIES_CREATED, 0),
        keeping(OBJECT_DELETE_REQUESTS, DELETE_OBJECT_REQUEST),

        // deleting: create a parent and delete any of its parents
        deleting(DIRECTORIES_CREATED, 1),
        deleting(OBJECT_DELETE_REQUESTS,
            DELETE_OBJECT_REQUEST
                + DELETE_MARKER_REQUEST)
    );
    // there is an empty dir for a parent
    S3AFileStatus status = verifyRawInnerGetFileStatus(dir, true,
        StatusProbeEnum.ALL, GETFILESTATUS_DIR_H, GETFILESTATUS_DIR_L);
    assertEmptyDirStatus(status, Tristate.TRUE);
  }

  @Test
  public void testDirMarkersSubdir() throws Throwable {
    describe("verify cost of deep subdir creation");

    Path subDir = new Path(methodPath(), "1/2/3/4/5/6");
    // one dir created, possibly a parent removed
    verifyMetrics(() -> {
          mkdirs(subDir);
          return "after mkdir(subDir) " + getMetricSummary();
        },
        always(DIRECTORIES_CREATED, 1),
        always(DIRECTORIES_DELETED, 0),
        keeping(OBJECT_DELETE_REQUESTS, 0),
        keeping(FAKE_DIRECTORIES_DELETED, 0),
        deleting(OBJECT_DELETE_REQUESTS, DELETE_MARKER_REQUEST),
        // delete all possible fake dirs above the subdirectory
        deleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(subDir) - 1));
  }

  @Test
  public void testDirMarkersFileCreation() throws Throwable {
    describe("verify cost of file creation");

    Path srcBaseDir = dir(methodPath());

    Path srcDir = dir(new Path(srcBaseDir, "1/2/3/4/5/6"));

    // creating a file should trigger demise of the src dir marker
    // unless markers are being kept

    verifyMetrics(() -> {
          file(new Path(srcDir, "source.txt"));
          return "after touch(fs, srcFilePath) " + getMetricSummary();
        },
        always(DIRECTORIES_CREATED, 0),
        always(DIRECTORIES_DELETED, 0),
        // keeping: no delete operations.
        keeping(OBJECT_DELETE_REQUESTS, 0),
        keeping(FAKE_DIRECTORIES_DELETED, 0),
        // delete all possible fake dirs above the file
        deleting(OBJECT_DELETE_REQUESTS, 1),
        deleting(FAKE_DIRECTORIES_DELETED, directoriesInPath(srcDir)));
  }

}
