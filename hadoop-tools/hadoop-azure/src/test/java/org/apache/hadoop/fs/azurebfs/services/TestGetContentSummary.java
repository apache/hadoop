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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestGetContentSummary extends AbstractAbfsIntegrationTest {

  private static final int TEST_BUFFER_SIZE = 20;
  private static final int FILES_PER_DIRECTORY = 2;
  private static final int MAX_THREADS = 16;
  private static final int NUM_FILES_FOR_LIST_MAX_TEST =
      DEFAULT_AZURE_LIST_MAX_RESULTS + 10;

  private final String[] directories = {"/testFolder",
      "/testFolder/testFolder1",
      "/testFolder/testFolder2",
      "/testFolder/testFolder3",
      "/testFolderII",
      "/testFolder/testFolder2/testFolder4",
      "/testFolder/testFolder2/testFolder5",
      "/testFolder/testFolder3/testFolder6",
      "/testFolder/testFolder3/testFolder7",
      "/testFolder/testFolder3/testFolder6/leafDir",
      "/testFolderII/listMaxDir",
      "/testFolderII/listMaxDir/" + DEFAULT_AZURE_LIST_MAX_RESULTS/2 + "_mid_folder"};
  private final Path pathToFile = new Path("/testFolder/test1");
  private final Path pathToListMaxDir = new Path("/testFolderII/listMaxDir");
  private final Path pathToLeafDir =
      new Path("/testFolder/testFolder3/testFolder6/leafDir");
  private final Path pathToIntermediateDirWithFilesOnly = new Path(
        "/testFolder/testFolder2/testFolder5");
  private final Path pathToIntermediateDirWithFilesAndSubdirs = new Path(
        "/testFolder/testFolder3");
  private final String[] dirsWithNonEmptyFiles = {"/testFolder", "/testFolder/testFolder1",
      "/testFolder/testFolder2/testFolder5", "/testFolder/testFolder3"};

  private final AzureBlobFileSystem fs = createFileSystem();
  private final byte[] b = new byte[TEST_BUFFER_SIZE];

  public TestGetContentSummary() throws Exception {
    createDirectoryStructure();
    new Random().nextBytes(b);
  }

  @Test
  public void testFilesystemRoot()
      throws IOException {
    int fileCount =
        (directories.length - 2) * FILES_PER_DIRECTORY + NUM_FILES_FOR_LIST_MAX_TEST;
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    verifyContentSummary(contentSummary, directories.length, fileCount,
        dirsWithNonEmptyFiles.length * FILES_PER_DIRECTORY * TEST_BUFFER_SIZE);
  }

  @Test
  public void testFileContentSummary() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(pathToFile);
    verifyContentSummary(contentSummary, 0, 1, TEST_BUFFER_SIZE);
  }

  @Test
  public void testLeafDir() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(pathToLeafDir);
    verifyContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testIntermediateDirWithFilesOnly() throws IOException {
    ContentSummary contentSummary =
        fs.getContentSummary(pathToIntermediateDirWithFilesOnly);
    verifyContentSummary(contentSummary, 0, FILES_PER_DIRECTORY,
        TEST_BUFFER_SIZE * FILES_PER_DIRECTORY);
  }

  @Test
  public void testIntermediateDirWithFilesAndSubdirs() throws IOException {
    ContentSummary contentSummary =
        fs.getContentSummary(pathToIntermediateDirWithFilesAndSubdirs);
    verifyContentSummary(contentSummary, 3, 3 * FILES_PER_DIRECTORY,
        TEST_BUFFER_SIZE * FILES_PER_DIRECTORY);
  }

  @Test
  public void testDirOverListMaxResultsItems()
      throws IOException {
    verifyContentSummary(
        fs.getContentSummary(pathToListMaxDir), 1,
        NUM_FILES_FOR_LIST_MAX_TEST + FILES_PER_DIRECTORY, 0);
  }

  @Test
  public void testInvalidPath() throws Exception {
    intercept(IOException.class, () -> fs.getContentSummary(new Path(
        "/nonExistentPath")));
  }

  @Test
  public void testConcurrentGetContentSummaryCalls()
          throws InterruptedException, ExecutionException {
    ExecutorService executorService = new ThreadPoolExecutor(1, MAX_THREADS,
        5, TimeUnit.SECONDS, new SynchronousQueue<>());
    ArrayList<Future<ContentSummary>> futures = new ArrayList<>();
    for (String directory : directories) {
      Future<ContentSummary> future = executorService.submit(
              () -> fs.getContentSummary(new Path(directory)));
      futures.add(future);
    }
    int[][] dirCS = {{8, 8 * FILES_PER_DIRECTORY, 8 * TEST_BUFFER_SIZE},
        {0, FILES_PER_DIRECTORY, 2 * TEST_BUFFER_SIZE},
        {2, 3 * FILES_PER_DIRECTORY, 2 * TEST_BUFFER_SIZE},
        {3, 3 * FILES_PER_DIRECTORY, 2 * TEST_BUFFER_SIZE},
        {2, NUM_FILES_FOR_LIST_MAX_TEST + 2 * FILES_PER_DIRECTORY, 0},
        {0, FILES_PER_DIRECTORY, 0},
        {0, FILES_PER_DIRECTORY, FILES_PER_DIRECTORY * TEST_BUFFER_SIZE},
        {1, FILES_PER_DIRECTORY, 0},
        {0, FILES_PER_DIRECTORY, 0},
        {0, 0, 0},
        {1, NUM_FILES_FOR_LIST_MAX_TEST + 2, 0},
        {0, FILES_PER_DIRECTORY, 0}};

    for (int i=0; i<directories.length; i++) {
      ContentSummary contentSummary = futures.get(i).get();
      verifyContentSummary(contentSummary, dirCS[i][0], dirCS[i][1], dirCS[i][2]);
    }
    executorService.shutdown();
  }

  private void verifyContentSummary(ContentSummary contentSummary,
      long expectedDirectoryCount, long expectedFileCount, long expectedByteCount) {
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .describedAs("Incorrect directory count").isEqualTo(expectedDirectoryCount);
    Assertions.assertThat(contentSummary.getFileCount())
        .describedAs("Incorrect file count").isEqualTo(expectedFileCount);
    Assertions.assertThat(contentSummary.getLength())
        .describedAs("Incorrect length").isEqualTo(expectedByteCount);
    Assertions.assertThat(contentSummary.getSpaceConsumed())
        .describedAs("Incorrect value of space consumed").isEqualTo(expectedByteCount);
  }

  private void createDirectoryStructure()
      throws IOException, ExecutionException, InterruptedException {
    for (String directory : directories) {
      Path dirPath = new Path(directory);
      fs.mkdirs(dirPath);
      if (!(dirPath.equals(pathToLeafDir) || dirPath
          .equals(pathToListMaxDir))) {
        populateDirWithFiles(dirPath, FILES_PER_DIRECTORY);
      }
    }
    for (String dir : dirsWithNonEmptyFiles) {
      for (int i = 0; i < FILES_PER_DIRECTORY; i++) {
        FSDataOutputStream out = fs.append(new Path(dir + "/test" + i));
        out.write(b);
        out.close();
      }
    }
    populateDirWithFiles(pathToListMaxDir, NUM_FILES_FOR_LIST_MAX_TEST);
  }

  private void populateDirWithFiles(Path directory, int numFiles)
      throws ExecutionException, InterruptedException {
    final List<Future<Void>> tasks = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < numFiles; i++) {
      final Path fileName = new Path(directory + "/test" + i);
      tasks.add(es.submit(() -> {
        touch(fileName);
        return null;
      }));
    }
    for (Future<Void> task : tasks) {
      task.get();
    }
    es.shutdownNow();
  }
}
