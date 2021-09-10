/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestGetContentSummary extends AbstractAbfsIntegrationTest {

  private static final int TEST_BUFFER_SIZE = 20;
  private static final int FILES_PER_DIRECTORY = 2;
  private static final int MAX_THREADS = 16;
  private static final int NUM_FILES_FOR_LIST_MAX_TEST =
      DEFAULT_AZURE_LIST_MAX_RESULTS + 10;
  private static final int NUM_CONCURRENT_CALLS = 8;

  private final String[] directories = {"/testFolder",
      "/testFolderII",
      "/testFolder/testFolder1",
      "/testFolder/testFolder2",
      "/testFolder/testFolder3",
      "/testFolder/testFolder2/testFolder4",
      "/testFolder/testFolder2/testFolder5",
      "/testFolder/testFolder3/testFolder6",
      "/testFolder/testFolder3/testFolder7"};

  private final byte[] b = new byte[TEST_BUFFER_SIZE];

  public ITestGetContentSummary() throws Exception {
    new Random().nextBytes(b);
  }

  @Test
  public void testFilesystemRoot()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    createDirectoryStructure();
    int fileCount = directories.length * FILES_PER_DIRECTORY;
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    verifyContentSummary(contentSummary, directories.length + 2, fileCount,
        directories.length * TEST_BUFFER_SIZE);
  }

  @Test
  public void testFileContentSummary() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path filePath = path("testFile");
    try (FSDataOutputStream out = fs.create(filePath)) {
      out.write(b);
    }
    ContentSummary contentSummary = fs.getContentSummary(filePath);
    verifyContentSummary(contentSummary, 0, 1, TEST_BUFFER_SIZE);
  }

  @Test
  public void testLeafDir() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testFolder = path("testFolder");
    fs.mkdirs(new Path(testFolder + "/testFolder2"));
    Path leafDir = new Path(testFolder + "/testFolder1/testFolder3");
    fs.mkdirs(leafDir);
    ContentSummary contentSummary = fs.getContentSummary(leafDir);
    verifyContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testIntermediateDirWithFilesOnly()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testFolder = path("testFolder");
    Path intermediateDir = new Path(testFolder + "/testFolder1");
    fs.mkdirs(intermediateDir);
    populateDirWithFiles(intermediateDir, FILES_PER_DIRECTORY);
    ContentSummary contentSummary =
        fs.getContentSummary(intermediateDir);
    verifyContentSummary(contentSummary, 0, FILES_PER_DIRECTORY,
        TEST_BUFFER_SIZE);
  }

  @Test
  public void testIntermediateDirWithFilesAndSubdirs()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path intermediateDir = new Path(path("/testFolder") + "/testFolder1");
    fs.mkdirs(intermediateDir);
    populateDirWithFiles(intermediateDir, FILES_PER_DIRECTORY);
    fs.mkdirs(new Path(intermediateDir + "/testFolder3"));
    fs.registerListener(
        new TracingHeaderValidator(getConfiguration().getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.GET_CONTENT_SUMMARY, true,
            0));
    ContentSummary contentSummary =
        fs.getContentSummary(intermediateDir);
    verifyContentSummary(contentSummary, 1, FILES_PER_DIRECTORY,
        TEST_BUFFER_SIZE);
  }

  @Test
  public void testDirOverListMaxResultsItems()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path pathToListMaxDir = new Path(path("/testFolder") + "/listMaxDir");
    fs.mkdirs(new Path(pathToListMaxDir + "/testFolder2"));
    populateDirWithFiles(pathToListMaxDir, NUM_FILES_FOR_LIST_MAX_TEST);
    verifyContentSummary(
        fs.getContentSummary(pathToListMaxDir), 1,
        NUM_FILES_FOR_LIST_MAX_TEST, TEST_BUFFER_SIZE);
  }

  @Test
  public void testInvalidPath() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    intercept(IOException.class, () -> fs.getContentSummary(new Path(
        "/nonExistentPath")));
  }

  @Test
  public void testConcurrentGetContentSummaryCalls()
      throws InterruptedException, ExecutionException, IOException {
    AzureBlobFileSystem fs = getFileSystem();
    ExecutorService executorService = new ThreadPoolExecutor(1, MAX_THREADS, 5,
        TimeUnit.SECONDS, new SynchronousQueue<>());
    CompletionService<ContentSummary> completionService =
        new ExecutorCompletionService<>(executorService);
    Path testPath = createDirectoryStructure();
    for (int i = 0; i < NUM_CONCURRENT_CALLS; i++) {
      completionService.submit(() -> fs.getContentSummary(new Path(
          testPath + "/testFolder")));
    }
    for (int i = 0; i < NUM_CONCURRENT_CALLS; i++) {
      ContentSummary contentSummary = completionService.take().get();
      verifyContentSummary(contentSummary, 7, 8 * FILES_PER_DIRECTORY,
          8 * TEST_BUFFER_SIZE);
    }
    executorService.shutdown();
  }

  private void verifyContentSummary(ContentSummary contentSummary,
      long expectedDirectoryCount, long expectedFileCount, long expectedByteCount) {
    System.out.println(contentSummary);
    System.out.println(expectedDirectoryCount + " : " + expectedFileCount +
        " : " + expectedByteCount);
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .describedAs("Incorrect directory count").isEqualTo(expectedDirectoryCount);
    Assertions.assertThat(contentSummary.getFileCount())
        .describedAs("Incorrect file count").isEqualTo(expectedFileCount);
    Assertions.assertThat(contentSummary.getLength())
        .describedAs("Incorrect length").isEqualTo(expectedByteCount);
    Assertions.assertThat(contentSummary.getSpaceConsumed())
        .describedAs("Incorrect value of space consumed").isEqualTo(expectedByteCount);
  }

  private Path createDirectoryStructure()
      throws IOException, ExecutionException, InterruptedException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path("testPath");
    for (String directory : directories) {
      Path dirPath = new Path(testPath + directory);
      fs.mkdirs(dirPath);
      populateDirWithFiles(dirPath, FILES_PER_DIRECTORY);
    }
    return testPath;
  }

  private void populateDirWithFiles(Path directory, int numFiles)
      throws ExecutionException, InterruptedException, IOException {
    final List<Future<Void>> tasks = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numFiles; i++) {
      final Path fileName = new Path(directory, String.format("test-%02d", i));
      tasks.add(es.submit(() -> {
        touch(fileName);
        return null;
      }));
    }
    for (Future<Void> task : tasks) {
      task.get();
    }
    try (FSDataOutputStream out = getFileSystem().append(
        new Path(directory + "/test-00"))) {
      out.write(b);
    }
    es.shutdownNow();
  }
}
