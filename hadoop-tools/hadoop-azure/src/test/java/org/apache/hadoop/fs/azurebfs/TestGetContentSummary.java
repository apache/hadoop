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
  private final String[] directories = {"testFolder", "testFolder/testFolder1",
      "testFolder/testFolder2", "testFolder/testFolder3", "testFolderII",
      "testFolder/testFolder2/testFolder4",
      "testFolder/testFolder2/testFolder5",
      "testFolder/testFolder3/testFolder6",
      "testFolder/testFolder3/testFolder7"};
  private final int filesPerDirectory = 2;
  private final AzureBlobFileSystem fs = createFileSystem();
  private final byte[] b = new byte[20];

  public TestGetContentSummary() throws Exception {
    createDirectoryStructure();
    new Random().nextBytes(b);
  }

  @Test
  public void testFilesystemRoot() throws IOException {
    ContentSummary contentSummary = fs.getContentSummary(new Path("/"));
    checkContentSummary(contentSummary, directories.length,
        directories.length * filesPerDirectory, 0);
  }

  @Test
  public void testFileContentSummary() throws IOException {
    Path filePath = new Path("/testFolderII/testFile");
    FSDataOutputStream out = fs.create(filePath);
    out.write(b);
    out.close();
    ContentSummary contentSummary = fs.getContentSummary(filePath);
    checkContentSummary(contentSummary, 0, 1, 20);
  }

  @Test
  public void testLeafDir() throws IOException {
    Path pathToLeafDir = new Path(
        "/testFolder/testFolder2/testFolder4" + "/leafDir");
    fs.mkdirs(pathToLeafDir);
    ContentSummary contentSummary = fs.getContentSummary(pathToLeafDir);
    checkContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testIntermediateDirWithFilesOnly() throws IOException {
    String dirPath = "/testFolder/testFolder3/testFolder6";
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dirPath + "/test" + i));
      out.write(b);
      out.close();
    }
    ContentSummary contentSummary = fs.getContentSummary(new Path(dirPath));
    checkContentSummary(contentSummary, 0, filesPerDirectory,
        20 * filesPerDirectory);
  }

  @Test
  public void testIntermediateDirWithFilesAndSubdirs() throws IOException {
    Path dirPath = new Path("/testFolder/testFolder3");
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dirPath + "/test" + i));
      out.write(b);
      out.close();
    }
    Path dir2Path = new Path("/testFolder/testFolder3/testFolder6");
    for (int i = 0; i < filesPerDirectory; i++) {
      FSDataOutputStream out = fs.append(new Path(dir2Path + "/test" + i));
      out.write(b);
      out.close();
    }
    ContentSummary contentSummary = fs.getContentSummary(dirPath);
    checkContentSummary(contentSummary, 2, 3 * filesPerDirectory, 20 * 2 * 2);
  }

  @Test
  public void testEmptyDir() throws IOException {
    Path pathToEmptyDir = new Path("/testFolder/emptyDir");
    fs.mkdirs(pathToEmptyDir);
    ContentSummary contentSummary = fs.getContentSummary(pathToEmptyDir);
    checkContentSummary(contentSummary, 0, 0, 0);
  }

  @Test
  public void testDirOverListMaxResultsItems()
      throws IOException, ExecutionException, InterruptedException {
    Path pathToDir = new Path("/testFolder/testFolder2/maxListDir");
    fs.mkdirs(pathToDir);
    populateDirWithFiles(pathToDir, DEFAULT_AZURE_LIST_MAX_RESULTS + 100);
    FSDataOutputStream out = fs.append(new Path(pathToDir + "/test0"));
    out.write(b);
    out.close();
    checkContentSummary(
        fs.getContentSummary(new Path("/testFolder" + "/testFolder2")), 3,
        DEFAULT_AZURE_LIST_MAX_RESULTS + 100 + filesPerDirectory * 3, 20);
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
      Path dirPath = new Path("/" + directory);
      fs.mkdirs(dirPath);
      populateDirWithFiles(dirPath, filesPerDirectory);
    }
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
