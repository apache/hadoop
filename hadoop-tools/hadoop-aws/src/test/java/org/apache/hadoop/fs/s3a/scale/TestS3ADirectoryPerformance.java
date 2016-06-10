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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
public class TestS3ADirectoryPerformance extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestS3ADirectoryPerformance.class);

  @Test
  public void testListOperations() throws Throwable {
    describe("Test recursive list operations");
    final Path scaleTestDir = getTestPath();
    final Path listDir = new Path(scaleTestDir, "lists");

    // scale factor.
    int scale = getConf().getInt(KEY_DIRECTORY_COUNT, DEFAULT_DIRECTORY_COUNT);
    int width = scale;
    int depth = scale;
    int files = scale;
    MetricDiff metadataRequests = new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
    MetricDiff listRequests = new MetricDiff(fs, OBJECT_LIST_REQUESTS);
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
        listStatusCalls,
        getFileStatusCalls);

    try {
      // Scan the directory via an explicit tree walk.
      // This is the baseline for any listing speedups.
      MetricDiff treewalkMetadataRequests =
          new MetricDiff(fs, OBJECT_METADATA_REQUESTS);
      MetricDiff treewalkListRequests = new MetricDiff(fs,
          OBJECT_LIST_REQUESTS);
      MetricDiff treewalkListStatusCalls = new MetricDiff(fs,
          INVOCATION_LIST_FILES);
      MetricDiff treewalkGetFileStatusCalls =
          new MetricDiff(fs, INVOCATION_GET_FILE_STATUS);
      NanoTimer treeWalkTimer = new NanoTimer();
      TreeScanResults treewalkResults = treeWalk(fs, listDir);
      treeWalkTimer.end("List status via treewalk");

      print(LOG,
          treewalkMetadataRequests,
          treewalkListRequests,
          treewalkListStatusCalls,
          treewalkGetFileStatusCalls);
      assertEquals("Files found in listFiles(recursive=true) " +
              " created=" + created + " listed=" + treewalkResults,
          created.getFileCount(), treewalkResults.getFileCount());


      // listFiles() does the recursion internally
      NanoTimer listFilesRecursiveTimer = new NanoTimer();

      TreeScanResults listFilesResults = new TreeScanResults(
          fs.listFiles(listDir, true));

      listFilesRecursiveTimer.end("listFiles(recursive=true) of %s", created);
      assertEquals("Files found in listFiles(recursive=true) " +
          " created=" + created  + " listed=" + listFilesResults,
          created.getFileCount(), listFilesResults.getFileCount());

      treewalkListRequests.assertDiffEquals(listRequests);
      printThenReset(LOG,
          metadataRequests, listRequests,
          listStatusCalls, getFileStatusCalls);

      NanoTimer globStatusTimer = new NanoTimer();
      FileStatus[] globStatusFiles = fs.globStatus(listDir);
      globStatusTimer.end("Time to globStatus() %s", globStatusTimer);
      LOG.info("Time for glob status {} entries: {}",
          globStatusFiles.length,
          toHuman(createTimer.duration()));
      printThenReset(LOG,
          metadataRequests,
          listRequests,
          listStatusCalls,
          getFileStatusCalls);

    } finally {
      // deletion at the end of the run
      NanoTimer deleteTimer = new NanoTimer();
      fs.delete(listDir, true);
      deleteTimer.end("Deleting directory tree");
      printThenReset(LOG,
          metadataRequests, listRequests,
          listStatusCalls, getFileStatusCalls);
    }
  }

  @Test
  public void testTimeToStatEmptyDirectory() throws Throwable {
    describe("Time to stat an empty directory");
    Path path = new Path(getTestPath(), "empty");
    fs.mkdirs(path);
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatNonEmptyDirectory() throws Throwable {
    describe("Time to stat a non-empty directory");
    Path path = new Path(getTestPath(), "dir");
    fs.mkdirs(path);
    touch(fs, new Path(path, "file"));
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatFile() throws Throwable {
    describe("Time to stat a simple file");
    Path path = new Path(getTestPath(), "file");
    touch(fs, path);
    timeToStatPath(path);
  }

  @Test
  public void testTimeToStatRoot() throws Throwable {
    describe("Time to stat the root path");
    timeToStatPath(new Path("/"));
  }

  private void timeToStatPath(Path path) throws IOException {
    describe("Timing getFileStatus(\"%s\")", path);
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
