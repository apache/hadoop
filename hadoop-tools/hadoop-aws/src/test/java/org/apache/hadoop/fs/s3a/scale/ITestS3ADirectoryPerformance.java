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
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

/**
 * Test the performance of listing files/directories.
 */
public class ITestS3ADirectoryPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestS3ADirectoryPerformance.class);

  @Test
  public void testListOperations() throws Throwable {
    describe("Test recursive list operations");
    final Path scaleTestDir = path("testListOperations");
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
        listRequests,
        listContinueRequests,
        listStatusCalls,
        getFileStatusCalls);

    describe("Listing files via treewalk");
    try {
      // Scan the directory via an explicit tree walk.
      // This is the baseline for any listing speedups.
      NanoTimer treeWalkTimer = new NanoTimer();
      TreeScanResults treewalkResults = treeWalk(fs, listDir);
      treeWalkTimer.end("List status via treewalk of %s", created);

      printThenReset(LOG,
          metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);
      assertEquals("Files found in listFiles(recursive=true) " +
              " created=" + created + " listed=" + treewalkResults,
          created.getFileCount(), treewalkResults.getFileCount());

      describe("Listing files via listFiles(recursive=true)");
      // listFiles() does the recursion internally
      NanoTimer listFilesRecursiveTimer = new NanoTimer();

      TreeScanResults listFilesResults = new TreeScanResults(
          fs.listFiles(listDir, true));

      listFilesRecursiveTimer.end("listFiles(recursive=true) of %s", created);
      assertEquals("Files found in listFiles(recursive=true) " +
          " created=" + created  + " listed=" + listFilesResults,
          created.getFileCount(), listFilesResults.getFileCount());

      // only two list operations should have taken place
      print(LOG,
          metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);
      if (!fs.hasMetadataStore()) {
        assertEquals(listRequests.toString(), 2, listRequests.diff());
      }
      reset(metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);

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

  @Test
  public void testTimeToStatEmptyDirectory() throws Throwable {
    describe("Time to stat an empty directory");
    Path path = path("empty");
    getFileSystem().mkdirs(path);
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatNonEmptyDirectory() throws Throwable {
    describe("Time to stat a non-empty directory");
    Path path = path("dir");
    S3AFileSystem fs = getFileSystem();
    fs.mkdirs(path);
    touch(fs, new Path(path, "file"));
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatFile() throws Throwable {
    describe("Time to stat a simple file");
    Path path = path("file");
    touch(getFileSystem(), path);
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatRoot() throws Throwable {
    describe("Time to stat the root path");
    timeToStatPath(new Path("/"));
  }

  private void timeToStatPath(Path path) throws IOException {
    describe("Timing getFileStatus(\"%s\")", path);
    S3AFileSystem fs = getFileSystem();
    MetricDiff metadataRequests =
        new MetricDiff(fs, Statistic.OBJECT_METADATA_REQUESTS);
    MetricDiff listRequests =
        new MetricDiff(fs, Statistic.OBJECT_LIST_REQUESTS);
    long attempts = getOperationCount();
    NanoTimer timer = new NanoTimer();
    for (long l = 0; l < attempts; l++) {
      fs.getFileStatus(path);
    }
    timer.end("Time to execute %d getFileStatusCalls", attempts);
    LOG.info("Time per call: {}", toHuman(timer.nanosPerOperation(attempts)));
    LOG.info("metadata: {}", metadataRequests);
    LOG.info("metadata per operation {}", metadataRequests.diff() / attempts);
    LOG.info("listObjects: {}", listRequests);
    LOG.info("listObjects: per operation {}", listRequests.diff() / attempts);
  }

}
