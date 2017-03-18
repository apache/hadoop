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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.*;

/**
 * Test the performance of glob operations.
 */
public class ITestS3AGlobPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3AGlobPerformance.class);

  @Test
  public void testGlobOperations() throws Throwable {
    describe("Test recursive list operations");
    final Path scaleTestDir = getTestPath();
    final Path listDir = new Path(scaleTestDir, "lists");
    S3AFileSystem fs = getFileSystem();

    // scale factor.
    int scale = getConf().getInt(KEY_DIRECTORY_COUNT, DEFAULT_DIRECTORY_COUNT);
    int width = scale;
    int depth = scale;
    int files = scale;
    MetricDiff metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    MetricDiff listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
    MetricDiff listContinueRequests =
        new MetricDiff(fs, OBJECT_CONTINUE_LIST_REQUESTS);
    MetricDiff listStatusCalls = new MetricDiff(fs, INVOCATION_LIST_FILES);
    MetricDiff globStatusCalls = new MetricDiff(fs, INVOCATION_GLOB_STATUS);
    MetricDiff getFileStatusCalls =
        new MetricDiff(fs, INVOCATION_GET_FILE_STATUS);
    NanoTimer createTimer = new NanoTimer();
    TreeScanResults created =
        createSubdirs(fs, listDir, depth, width, files, 0);
    // add some empty directories
    int emptyDepth = 1 * scale;
    int emptyWidth = 3 * scale;

    created.add(createSubdirs(fs, listDir, emptyDepth, emptyWidth, 0,
        0, "empty", "f-", ""));
    createTimer.end("Time to create %s", created);
    LOG.info("Time per operation: {}",
        toHuman(createTimer.nanosPerOperation(created.totalCount())));
    printThenReset(LOG,
        metadataRequests,
        globStatusCalls,
        listRequests,
        listContinueRequests,
        listStatusCalls,
        getFileStatusCalls);

    try {
      // Scan the directory via an explicit tree walk.
      // This is the baseline for any listing speedups.
      describe("Listing files via treewalk");
      NanoTimer treeWalkTimer = new NanoTimer();
      TreeScanResults treewalkResults = treeWalk(fs, listDir);
      treeWalkTimer.end("List status via treewalk of %s", created);

      printThenReset(LOG,
          metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);
      Path globPattern = new Path(listDir, "{*/*.txt,*.txt}");

      TreeScanResults globAll = compareGlobs(globPattern);
      treewalkResults.assertFieldsEquivalent(
          "Files found in s3 glob(" + globPattern + ")", globAll,
          treewalkResults.getFiles(),
          globAll.getFiles());

      compareGlobs(new Path(listDir, "*.txt"));
      compareGlobs(new Path(listDir, "*"));


    } finally {
      describe("deletion");
      // deletion at the end of the run
      NanoTimer deleteTimer = new NanoTimer();
      fs.delete(listDir, true);
      deleteTimer.end("Deleting directory tree");
      printThenReset(LOG,
          metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);
    }
  }

  private TreeScanResults compareGlobs(Path globPattern) throws IOException {
    S3AFileSystem fs = getFileSystem();

    MetricDiff metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    MetricDiff listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
    MetricDiff listContinueRequests =
        new MetricDiff(fs, OBJECT_CONTINUE_LIST_REQUESTS);
    MetricDiff listStatusCalls = new MetricDiff(fs, INVOCATION_LIST_FILES);
    MetricDiff globStatusCalls = new MetricDiff(fs, INVOCATION_GLOB_STATUS);
    MetricDiff getFileStatusCalls =
        new MetricDiff(fs, INVOCATION_GET_FILE_STATUS);

    describe("FileSystem.globStatus operation on %s", globPattern);
    TreeScanResults classicResults = classicGlob(globPattern);
    printThenReset(LOG,
        metadataRequests,
        globStatusCalls,
        listRequests,
        listContinueRequests,
        listStatusCalls,
        getFileStatusCalls);

    long getFileStatusCount = getFileStatusCalls.diff();
    long listRequestCount = listRequests.diff();
    long listStatusCount = listStatusCalls.diff();

    describe("S3A lobStatus operation on %s", globPattern);
    TreeScanResults s3aGlobResults = s3aGlob(globPattern);
    printThenReset(LOG,
        metadataRequests,
        globStatusCalls,
        listRequests,
        listContinueRequests,
        listStatusCalls,
        getFileStatusCalls);
    classicResults.assertEquivalent(s3aGlobResults);

    return s3aGlobResults;
  }

  /**
   * Baseline: classic glob operation. Timing is logged.
   * @param globPattern pattern to glob
   * @return the results of the scan
   * @throws IOException IO problems.
   */
  private TreeScanResults classicGlob(Path globPattern)
      throws IOException {
    NanoTimer timer = new NanoTimer();
    TreeScanResults results = new TreeScanResults(
        getFileSystem().globStatusClassic(globPattern, trueFilter));
    timer.end("Classic Glob of %s: %s", globPattern, results);
    return results;
  }

  /**
   * Optimized S3A glob operation. Timing is logged.
   * @param globPattern pattern to glob
   * @return the results of the scan
   * @throws IOException IO problems.
   */
  private TreeScanResults s3aGlob(Path globPattern)
      throws IOException {
    NanoTimer timer = new NanoTimer();
    TreeScanResults results = new TreeScanResults(
        getFileSystem().globStatus(globPattern, trueFilter));
    timer.end("S3A Glob of %s: %s", globPattern, results);
    return results;
  }

  /**
   * Accept all paths.
   */
  private static class AcceptAllPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  }

  private static final PathFilter trueFilter = new AcceptAllPathFilter();



}
