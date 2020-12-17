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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_AZURE_LIST_MAX_RESULTS;

public class TestGetContentSummary extends AbstractAbfsIntegrationTest {

  private final String[] directories = {"/testFolder",
      "/testFolder/testFolder1",
      "/testFolder/testFolder2", "/testFolder/testFolder3", "/testFolderII",
      "/testFolder/testFolder2/testFolder4",
      "/testFolder/testFolder2/testFolder5",
      "/testFolder/testFolder3/testFolder6",
      "/testFolder/testFolder3/testFolder7",
      "/testFolder/testFolder3/testFolder6/leafDir",
      "/testFolderII/listMaxDir",
      "/testFolderII/listMaxDir/zFolder"};
  //thread poll should not get interrupted before zFolder is put in queue

  private final Path pathToFile = new Path("/testFolder/test1");;
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
  private final int testBufferSize = 20;
  private final int filesPerDirectory = 2;
  private final int numFilesForListMaxTest =
      DEFAULT_AZURE_LIST_MAX_RESULTS + 100;
  private final byte[] b = new byte[testBufferSize];

  public TestGetContentSummary() throws Exception {
    createDirectoryStructure();
    new Random().nextBytes(b);
  }

  @Test
  public void testFilesystemRoot()
      throws IOException {
    int fileCount =
        (directories.length - 2) * filesPerDirectory + numFilesForListMaxTest;
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    checkContentSummary(contentSummary, directories.length, fileCount,
        dirsWithNonEmptyFiles.length * filesPerDirectory * testBufferSize);
  }

  @Test
  public void testFileContentSummary() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(pathToFile);
    checkContentSummary(contentSummary, 0, 1, testBufferSize);
  }

  @Test
  public void testLeafDir() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(pathToLeafDir);
    checkContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testIntermediateDirWithFilesOnly() throws IOException {
    ContentSummary contentSummary =
        fs.getContentSummary(pathToIntermediateDirWithFilesOnly);
    checkContentSummary(contentSummary, 0, filesPerDirectory,
        testBufferSize * filesPerDirectory);
  }

  @Test
  public void testIntermediateDirWithFilesAndSubdirs() throws IOException {
    ContentSummary contentSummary =
        fs.getContentSummary(pathToIntermediateDirWithFilesAndSubdirs);
    checkContentSummary(contentSummary, 3, 3 * filesPerDirectory,
        testBufferSize * filesPerDirectory);
  }

  @Test
  public void testDirOverListMaxResultsItems()
      throws IOException, ExecutionException, InterruptedException {
    checkContentSummary(
        fs.getContentSummary(pathToListMaxDir), 1,
        numFilesForListMaxTest + filesPerDirectory, 0);
  }

  private void checkContentSummary(ContentSummary contentSummary,
      long directoryCount, long fileCount, long byteCount) {
    Assertions.assertThat(contentSummary.getDirectoryCount())
        .describedAs("Incorrect directory count").isEqualTo(directoryCount);
    Assertions.assertThat(contentSummary.getFileCount())
        .describedAs("Incorrect file count").isEqualTo(fileCount);
    Assertions.assertThat(contentSummary.getLength())
        .describedAs("Incorrect length").isEqualTo(byteCount);
    Assertions.assertThat(contentSummary.getSpaceConsumed())
        .describedAs("Incorrect value of space consumed").isEqualTo(byteCount);
  }

  private void createDirectoryStructure()
      throws IOException, ExecutionException, InterruptedException {
    for (String directory : directories) {
      Path dirPath = new Path(directory);
      fs.mkdirs(dirPath);
      if (!(dirPath.equals(pathToLeafDir) || dirPath
          .equals(pathToListMaxDir))) {
        populateDirWithFiles(dirPath, filesPerDirectory);
      }
    }
    for (String dir : dirsWithNonEmptyFiles) {
      for (int i = 0; i < filesPerDirectory; i++) {
        FSDataOutputStream out = fs.append(new Path(dir + "/test" + i));
        out.write(b);
        out.close();
      }
    }
    populateDirWithFiles(pathToListMaxDir, numFilesForListMaxTest);
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
