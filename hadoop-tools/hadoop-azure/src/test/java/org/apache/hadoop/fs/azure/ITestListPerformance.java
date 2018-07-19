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

package org.apache.hadoop.fs.azure;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azure.integration.AbstractAzureScaleTest;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;

/**
 * Test list performance.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class ITestListPerformance extends AbstractAzureScaleTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestListPerformance.class);

  private static final Path TEST_DIR_PATH = new Path(
      "DirectoryWithManyFiles");

  private static final int NUMBER_OF_THREADS = 10;
  private static final int NUMBER_OF_FILES_PER_THREAD = 1000;

  private int threads;

  private int filesPerThread;

  private int expectedFileCount;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = getConfiguration();
    // fail fast
    threads = AzureTestUtils.getTestPropertyInt(conf,
        "fs.azure.scale.test.list.performance.threads", NUMBER_OF_THREADS);
    filesPerThread = AzureTestUtils.getTestPropertyInt(conf,
        "fs.azure.scale.test.list.performance.files", NUMBER_OF_FILES_PER_THREAD);
    expectedFileCount = threads * filesPerThread;
    LOG.info("Thread = {}, Files per Thread = {}, expected files = {}",
        threads, filesPerThread, expectedFileCount);
    conf.set("fs.azure.io.retry.max.retries", "1");
    conf.set("fs.azure.delete.threads", "16");
    createTestAccount();
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create(
        "itestlistperformance",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        null,
        true);
  }

  @Test
  public void test_0101_CreateDirectoryWithFiles() throws Exception {
    Assume.assumeFalse("Test path exists; skipping", fs.exists(TEST_DIR_PATH));

    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    CloudBlobContainer container = testAccount.getRealContainer();

    final String basePath = (fs.getWorkingDirectory().toUri().getPath() + "/" + TEST_DIR_PATH + "/").substring(1);

    ArrayList<Callable<Integer>> tasks = new ArrayList<>(threads);
    fs.mkdirs(TEST_DIR_PATH);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    for (int i = 0; i < threads; i++) {
      tasks.add(
          new Callable<Integer>() {
            public Integer call() {
              int written = 0;
              for (int j = 0; j < filesPerThread; j++) {
                String blobName = basePath + UUID.randomUUID().toString();
                try {
                  CloudBlockBlob blob = container.getBlockBlobReference(
                      blobName);
                  blob.uploadText("");
                  written ++;
                } catch (Exception e) {
                  LOG.error("Filed to write {}", blobName, e);
                  break;
                }
              }
              LOG.info("Thread completed with {} files written", written);
              return written;
            }
          }
      );
    }

    List<Future<Integer>> futures = executorService.invokeAll(tasks,
        getTestTimeoutMillis(), TimeUnit.MILLISECONDS);
    long elapsedMs = timer.elapsedTimeMs();
    LOG.info("time to create files: {} millis", elapsedMs);

    for (Future<Integer> future : futures) {
      assertTrue("Future timed out", future.isDone());
      assertEquals("Future did not write all files timed out",
          filesPerThread, future.get().intValue());
    }
  }

  @Test
  public void test_0200_ListStatusPerformance() throws Exception {
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    FileStatus[] fileList = fs.listStatus(TEST_DIR_PATH);
    long elapsedMs = timer.elapsedTimeMs();
    LOG.info(String.format(
        "files=%1$d, elapsedMs=%2$d",
        fileList.length,
        elapsedMs));
    Map<Path, FileStatus> foundInList =new HashMap<>(expectedFileCount);

    for (FileStatus fileStatus : fileList) {
      foundInList.put(fileStatus.getPath(), fileStatus);
      LOG.info("{}: {}", fileStatus.getPath(),
          fileStatus.isDirectory() ? "dir" : "file");
    }
    assertEquals("Mismatch between expected files and actual",
        expectedFileCount, fileList.length);


    // now do a listFiles() recursive
    ContractTestUtils.NanoTimer initialStatusCallTimer
        = new ContractTestUtils.NanoTimer();
    RemoteIterator<LocatedFileStatus> listing
        = fs.listFiles(TEST_DIR_PATH, true);
    long initialListTime = initialStatusCallTimer.elapsedTimeMs();
    timer = new ContractTestUtils.NanoTimer();
    while (listing.hasNext()) {
      FileStatus fileStatus = listing.next();
      Path path = fileStatus.getPath();
      FileStatus removed = foundInList.remove(path);
      assertNotNull("Did not find "  + path + "{} in the previous listing",
          removed);
    }
    elapsedMs = timer.elapsedTimeMs();
    LOG.info("time for listFiles() initial call: {} millis;"
        + " time to iterate: {} millis", initialListTime, elapsedMs);
    assertEquals("Not all files from listStatus() were found in listFiles()",
        0, foundInList.size());

  }

  @Test
  public void test_0300_BulkDeletePerformance() throws Exception {
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.delete(TEST_DIR_PATH,true);
    long elapsedMs = timer.elapsedTimeMs();
    LOG.info("time for delete(): {} millis; {} nanoS per file",
        elapsedMs, timer.nanosPerOperation(expectedFileCount));
  }
}
