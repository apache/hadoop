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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.statistics.IOStatistics;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;

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
    MetricDiff listRequests = new MetricDiff(fs, Statistic.OBJECT_LIST_REQUEST);
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
        assertEquals(listRequests.toString(), 1, listRequests.diff());
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
  public void testMultiPagesListingPerformanceAndCorrectness()
          throws Throwable {
    describe("Check performance and correctness for multi page listing " +
            "using different listing api");
    final Path dir = methodPath();
    final int batchSize = 10;
    final int numOfPutRequests = 1000;
    final int eachFileProcessingTime = 10;
    final int numOfPutThreads = 50;
    Assertions.assertThat(numOfPutRequests % batchSize)
        .describedAs("Files put %d must be a multiple of list batch size %d",
            numOfPutRequests, batchSize)
        .isEqualTo(0);
    final Configuration conf =
            getConfigurationWithConfiguredBatchSize(batchSize);
    removeBaseAndBucketOverrides(conf, S3_METADATA_STORE_IMPL);
    final InputStream im = new InputStream() {
      @Override
      public int read() throws IOException {
        return -1;
      }
    };
    final List<String> originalListOfFiles = new ArrayList<>();
    List<Callable<PutObjectResult>> putObjectRequests = new ArrayList<>();
    ExecutorService executorService = Executors
            .newFixedThreadPool(numOfPutThreads);

    NanoTimer uploadTimer = new NanoTimer();
    S3AFileSystem fs = (S3AFileSystem) FileSystem.get(dir.toUri(), conf);
    try {
      assume("Test is only for raw fs", !fs.hasMetadataStore());
      fs.create(dir);
      for (int i=0; i<numOfPutRequests; i++) {
        Path file = new Path(dir, String.format("file-%03d", i));
        originalListOfFiles.add(file.toString());
        ObjectMetadata om = fs.newObjectMetadata(0L);
        PutObjectRequest put = new PutObjectRequest(fs.getBucket(),
                fs.pathToKey(file),
                im,
                om);
        putObjectRequests.add(() ->
                fs.getWriteOperationHelper().putObject(put));
      }
      executorService.invokeAll(putObjectRequests);
      uploadTimer.end("uploading %d files with a parallelism of %d",
              numOfPutRequests, numOfPutThreads);

      RemoteIterator<LocatedFileStatus> resIterator = fs.listFiles(dir, true);
      List<String> listUsingListFiles = new ArrayList<>();
      NanoTimer timeUsingListFiles = new NanoTimer();
      while(resIterator.hasNext()) {
        listUsingListFiles.add(resIterator.next().getPath().toString());
        Thread.sleep(eachFileProcessingTime);
      }
      timeUsingListFiles.end("listing %d files using listFiles() api with " +
                      "batch size of %d including %dms of processing time" +
                      " for each file",
              numOfPutRequests, batchSize, eachFileProcessingTime);

      Assertions.assertThat(listUsingListFiles)
              .describedAs("Listing results using listFiles() must" +
                      "match with original list of files")
              .hasSameElementsAs(originalListOfFiles)
              .hasSize(numOfPutRequests);
      List<String> listUsingListStatus = new ArrayList<>();
      NanoTimer timeUsingListStatus = new NanoTimer();
      FileStatus[] fileStatuses = fs.listStatus(dir);
      for(FileStatus fileStatus : fileStatuses) {
        listUsingListStatus.add(fileStatus.getPath().toString());
        Thread.sleep(eachFileProcessingTime);
      }
      timeUsingListStatus.end("listing %d files using listStatus() api with " +
                      "batch size of %d including %dms of processing time" +
                      " for each file",
              numOfPutRequests, batchSize, eachFileProcessingTime);
      Assertions.assertThat(listUsingListStatus)
              .describedAs("Listing results using listStatus() must" +
                      "match with original list of files")
              .hasSameElementsAs(originalListOfFiles)
              .hasSize(numOfPutRequests);
      // Validate listing using listStatusIterator().
      NanoTimer timeUsingListStatusItr = new NanoTimer();
      RemoteIterator<FileStatus> lsItr = fs.listStatusIterator(dir);
      List<String> listUsingListStatusItr = new ArrayList<>();
      while (lsItr.hasNext()) {
        listUsingListStatusItr.add(lsItr.next().getPath().toString());
        Thread.sleep(eachFileProcessingTime);
      }
      timeUsingListStatusItr.end("listing %d files using " +
                      "listStatusIterator() api with batch size of %d " +
                      "including %dms of processing time for each file",
              numOfPutRequests, batchSize, eachFileProcessingTime);
      Assertions.assertThat(listUsingListStatusItr)
              .describedAs("Listing results using listStatusIterator() must" +
                      "match with original list of files")
              .hasSameElementsAs(originalListOfFiles)
              .hasSize(numOfPutRequests);
      // now validate the statistics returned by the listing
      // to be non-null and containing list and continue counters.
      IOStatistics lsStats = retrieveIOStatistics(lsItr);
      String statsReport = ioStatisticsToString(lsStats);
      LOG.info("Listing Statistics: {}", statsReport);
      verifyStatisticCounterValue(lsStats, OBJECT_LIST_REQUEST, 1);
      long continuations = lookupCounterStatistic(lsStats,
          OBJECT_CONTINUE_LIST_REQUEST);
      // calculate expected #of continuations
      int expectedContinuations = numOfPutRequests / batchSize -1;
      Assertions.assertThat(continuations)
          .describedAs("%s in %s", OBJECT_CONTINUE_LIST_REQUEST, statsReport)
          .isEqualTo(expectedContinuations);
    } finally {
      executorService.shutdown();
      // delete in this FS so S3Guard is left out of it.
      fs.delete(dir, true);
      fs.close();
    }
  }

  private Configuration getConfigurationWithConfiguredBatchSize(int batchSize) {
    Configuration conf = new Configuration(getFileSystem().getConf());
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setInt(Constants.MAX_PAGING_KEYS, batchSize);
    return conf;
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
        new MetricDiff(fs, Statistic.OBJECT_LIST_REQUEST);
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
