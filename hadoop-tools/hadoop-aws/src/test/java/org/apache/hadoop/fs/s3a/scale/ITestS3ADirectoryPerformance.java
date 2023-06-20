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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.functional.RemoteIterators;

import org.junit.Test;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;
import static org.apache.hadoop.fs.s3a.Statistic.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupCounterStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
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
      assertEquals(listRequests.toString(), 1, listRequests.diff());
      reset(metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);

      describe("Get content summary for directory");

      NanoTimer getContentSummaryTimer = new NanoTimer();

      ContentSummary rootPathSummary = fs.getContentSummary(scaleTestDir);
      ContentSummary testPathSummary = fs.getContentSummary(listDir);

      getContentSummaryTimer.end("getContentSummary of %s", created);

      // only two list operations should have taken place
      print(LOG,
          metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);
      assertEquals(listRequests.toString(), 2, listRequests.diff());
      reset(metadataRequests,
          listRequests,
          listContinueRequests,
          listStatusCalls,
          getFileStatusCalls);

      assertTrue("Root directory count should be > test path",
          rootPathSummary.getDirectoryCount() > testPathSummary.getDirectoryCount());
      assertTrue("Root file count should be >= to test path",
          rootPathSummary.getFileCount() >= testPathSummary.getFileCount());
      assertEquals("Incorrect directory count", created.getDirCount() + 1,
          testPathSummary.getDirectoryCount());
      assertEquals("Incorrect file count", created.getFileCount(),
          testPathSummary.getFileCount());

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

  /**
   * This is quite a big test; it PUTs up a number of
   * files and then lists them in a filesystem set to ask for a small number
   * of files on each listing.
   * The standard listing API calls are all used, and then
   * delete() is invoked to verify that paged deletion works correctly too.
   */
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

    removeBaseAndBucketOverrides(conf,
        DIRECTORY_MARKER_POLICY);
    // force directory markers = keep to save delete requests on every
    // file created.
    conf.set(DIRECTORY_MARKER_POLICY, DIRECTORY_MARKER_POLICY_KEEP);
    S3AFileSystem fs = (S3AFileSystem) FileSystem.get(dir.toUri(), conf);

    final List<String> originalListOfFiles = new ArrayList<>();
    ExecutorService executorService = Executors
            .newFixedThreadPool(numOfPutThreads);

    NanoTimer uploadTimer = new NanoTimer();
    try {
      fs.create(dir);

      // create a span for the write operations
      final AuditSpan span = fs.getAuditSpanSource()
          .createSpan(OBJECT_PUT_REQUESTS.getSymbol(), dir.toString(), null);
      final WriteOperationHelper writeOperationHelper
          = fs.getWriteOperationHelper();
      final RequestFactory requestFactory
          = writeOperationHelper.getRequestFactory();
      List<CompletableFuture<PutObjectResult>> futures =
          new ArrayList<>(numOfPutRequests);

      for (int i=0; i<numOfPutRequests; i++) {
        Path file = new Path(dir, String.format("file-%03d", i));
        originalListOfFiles.add(file.toString());
        ObjectMetadata om = fs.newObjectMetadata(0L);
        PutObjectRequest put = requestFactory
            .newPutObjectRequest(fs.pathToKey(file), om,
                null, new FailingInputStream());
        futures.add(submit(executorService, () ->
            writeOperationHelper.putObject(put, PutObjectOptions.keepingDirs(), null)));
      }
      LOG.info("Waiting for PUTs to complete");
      waitForCompletion(futures);
      uploadTimer.end("uploading %d files with a parallelism of %d",
              numOfPutRequests, numOfPutThreads);

      RemoteIterator<LocatedFileStatus> resIterator = fs.listFiles(dir, true);
      List<String> listUsingListFiles = new ArrayList<>();
      NanoTimer timeUsingListFiles = new NanoTimer();
      RemoteIterators.foreach(resIterator, st -> {
        listUsingListFiles.add(st.getPath().toString());
        sleep(eachFileProcessingTime);
      });
      LOG.info("Listing Statistics: {}", ioStatisticsToPrettyString(
          retrieveIOStatistics(resIterator)));

      timeUsingListFiles.end("listing %d files using listFiles() api with " +
                      "batch size of %d including %dms of processing time" +
                      " for each file",
              numOfPutRequests, batchSize, eachFileProcessingTime);

      Assertions.assertThat(listUsingListFiles)
              .describedAs("Listing results using listFiles() must" +
                      " match with original list of files")
              .hasSameElementsAs(originalListOfFiles)
              .hasSize(numOfPutRequests);
      List<String> listUsingListStatus = new ArrayList<>();
      NanoTimer timeUsingListStatus = new NanoTimer();
      FileStatus[] fileStatuses = fs.listStatus(dir);
      for(FileStatus fileStatus : fileStatuses) {
        listUsingListStatus.add(fileStatus.getPath().toString());
        sleep(eachFileProcessingTime);
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
      List<String> listUsingListStatusItr = new ArrayList<>();
      RemoteIterator<FileStatus> lsItr = fs.listStatusIterator(dir);
      RemoteIterators.foreach(lsItr, st -> {
        listUsingListStatusItr.add(st.getPath().toString());
        sleep(eachFileProcessingTime);
      });
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
      String statsReport = ioStatisticsToPrettyString(lsStats);
      LOG.info("Listing Statistics: {}", statsReport);
      verifyStatisticCounterValue(lsStats, OBJECT_LIST_REQUEST, 1);
      long continuations = lookupCounterStatistic(lsStats,
          OBJECT_CONTINUE_LIST_REQUEST);
      // calculate expected #of continuations
      int expectedContinuations = numOfPutRequests / batchSize - 1;
      Assertions.assertThat(continuations)
          .describedAs("%s in %s", OBJECT_CONTINUE_LIST_REQUEST, statsReport)
          .isEqualTo(expectedContinuations);

      List<String> listUsingListLocatedStatus = new ArrayList<>();

      RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(dir);
      RemoteIterators.foreach(it, st -> {
        listUsingListLocatedStatus.add(st.getPath().toString());
        sleep(eachFileProcessingTime);
      });
      final IOStatistics llsStats = retrieveIOStatistics(it);
      LOG.info("Listing Statistics: {}", ioStatisticsToPrettyString(
          llsStats));
      verifyStatisticCounterValue(llsStats, OBJECT_CONTINUE_LIST_REQUEST,
          expectedContinuations);
      Assertions.assertThat(listUsingListLocatedStatus)
          .describedAs("Listing results using listLocatedStatus() must" +
              "match with original list of files")
          .hasSameElementsAs(originalListOfFiles);
      fs.delete(dir, true);
    } finally {
      executorService.shutdown();
      // in case the previous delete was not reached.
      fs.delete(dir, true);
      LOG.info("FS statistics {}",
          ioStatisticsToPrettyString(fs.getIOStatistics()));
      fs.close();
    }
  }

  /**
   * Input stream which always returns -1.
   */
  private static final class FailingInputStream  extends InputStream {
    @Override
    public int read() throws IOException {
      return -1;
    }
  }

  /**
   * Sleep briefly.
   * @param eachFileProcessingTime time to sleep.
   */
  private void sleep(final int eachFileProcessingTime) {
    try {
      Thread.sleep(eachFileProcessingTime);
    } catch (InterruptedException ignored) {
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
