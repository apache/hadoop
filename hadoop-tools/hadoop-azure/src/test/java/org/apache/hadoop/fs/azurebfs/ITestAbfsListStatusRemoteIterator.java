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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.services.AbfsListStatusRemoteIterator;
import org.apache.hadoop.fs.azurebfs.services.ListingSupport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;

/**
 * Test ListStatusRemoteIterator operation.
 */
public class ITestAbfsListStatusRemoteIterator extends AbstractAbfsIntegrationTest {

  private static final int TEST_FILES_NUMBER = 1000;

  public ITestAbfsListStatusRemoteIterator() throws Exception {
  }

  @Test
  public void testAbfsIteratorWithHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    final List<String> fileNames = createFilesUnderDirectory(TEST_FILES_NUMBER,
        testDir, "testListPath");

    ListingSupport listngSupport = Mockito.spy(getFileSystem().getAbfsStore());
    RemoteIterator<FileStatus> fsItr = new AbfsListStatusRemoteIterator(
        getFileSystem().getFileStatus(testDir), listngSupport);
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should be instance of "
            + "AbfsListStatusRemoteIterator by default")
        .isInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    while (fsItr.hasNext()) {
      FileStatus fileStatus = fsItr.next();
      String pathStr = fileStatus.getPath().toString();
      fileNames.remove(pathStr);
      itrCount++;
    }
    Assertions.assertThat(itrCount)
        .describedAs("Number of iterations should be equal to the files "
            + "created")
        .isEqualTo(TEST_FILES_NUMBER);
    Assertions.assertThat(fileNames.size())
        .describedAs("After removing every iterm found from the iterator, "
            + "there should be no more elements in the fileNames")
        .isEqualTo(0);
    int minNumberOfInvokations = TEST_FILES_NUMBER / 10;
    verify(listngSupport, Mockito.atLeast(minNumberOfInvokations))
        .listStatus(any(Path.class), nullable(String.class),
            anyList(), anyBoolean(),
            nullable(String.class));
  }

  @Test
  public void testAbfsIteratorWithoutHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    final List<String> fileNames = createFilesUnderDirectory(TEST_FILES_NUMBER,
        testDir, "testListPath");

    ListingSupport listngSupport = Mockito.spy(getFileSystem().getAbfsStore());
    RemoteIterator<FileStatus> fsItr = new AbfsListStatusRemoteIterator(
        getFileSystem().getFileStatus(testDir), listngSupport);
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should be instance of "
            + "AbfsListStatusRemoteIterator by default")
        .isInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      FileStatus fileStatus = fsItr.next();
      String pathStr = fileStatus.getPath().toString();
      fileNames.remove(pathStr);
      itrCount++;
    }
    Assertions.assertThatThrownBy(() -> fsItr.next())
        .describedAs(
            "next() should throw NoSuchElementException since next has been "
                + "called " + TEST_FILES_NUMBER + " times")
        .isInstanceOf(NoSuchElementException.class);
    Assertions.assertThat(itrCount)
        .describedAs("Number of iterations should be equal to the files "
            + "created")
        .isEqualTo(TEST_FILES_NUMBER);
    Assertions.assertThat(fileNames.size())
        .describedAs("After removing every iterm found from the iterator, "
            + "there should be no more elements in the fileNames")
        .isEqualTo(0);
    int minNumberOfInvokations = TEST_FILES_NUMBER / 10;
    verify(listngSupport, Mockito.atLeast(minNumberOfInvokations))
        .listStatus(any(Path.class), nullable(String.class),
            anyList(), anyBoolean(),
            nullable(String.class));
  }

  @Test
  public void testWithAbfsIteratorDisabled() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    setEnableAbfsIterator(false);
    final List<String> fileNames = createFilesUnderDirectory(TEST_FILES_NUMBER,
        testDir, "testListPath");

    RemoteIterator<FileStatus> fsItr =
        getFileSystem().listStatusIterator(testDir);
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should not be instance of "
            + "AbfsListStatusRemoteIterator when it is disabled")
        .isNotInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    while (fsItr.hasNext()) {
      FileStatus fileStatus = fsItr.next();
      String pathStr = fileStatus.getPath().toString();
      fileNames.remove(pathStr);
      itrCount++;
    }
    Assertions.assertThat(itrCount)
        .describedAs("Number of iterations should be equal to the files "
            + "created")
        .isEqualTo(TEST_FILES_NUMBER);
    Assertions.assertThat(fileNames.size())
        .describedAs("After removing every iterm found from the iterator, "
            + "there should be no more elements in the fileNames")
        .isEqualTo(0);
  }

  @Test
  public void testWithAbfsIteratorDisabledWithoutHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    setEnableAbfsIterator(false);
    final List<String> fileNames = createFilesUnderDirectory(TEST_FILES_NUMBER,
        testDir, "testListPath");

    RemoteIterator<FileStatus> fsItr =
        getFileSystem().listStatusIterator(testDir);
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should not be instance of "
            + "AbfsListStatusRemoteIterator when it is disabled")
        .isNotInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      FileStatus fileStatus = fsItr.next();
      String pathStr = fileStatus.getPath().toString();
      fileNames.remove(pathStr);
      itrCount++;
    }
    Assertions.assertThatThrownBy(() -> fsItr.next())
        .describedAs(
            "next() should throw NoSuchElementException since next has been "
                + "called " + TEST_FILES_NUMBER + " times")
        .isInstanceOf(NoSuchElementException.class);
    Assertions.assertThat(itrCount)
        .describedAs("Number of iterations should be equal to the files "
            + "created")
        .isEqualTo(TEST_FILES_NUMBER);
    Assertions.assertThat(fileNames.size())
        .describedAs("After removing every iterm found from the iterator, "
            + "there should be no more elements in the fileNames")
        .isEqualTo(0);
  }

  @Test
  public void testNextWhenNoMoreElementsPresent() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    RemoteIterator fsItr =
        new AbfsListStatusRemoteIterator(getFileSystem().getFileStatus(testDir),
            getFileSystem().getAbfsStore());
    fsItr = Mockito.spy(fsItr);
    Mockito.doReturn(false).when(fsItr).hasNext();

    RemoteIterator<FileStatus> finalFsItr = fsItr;
    Assertions.assertThatThrownBy(() -> finalFsItr.next())
        .describedAs(
        "next() should throw NoSuchElementException if hasNext() return "
            + "false")
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testHasNextForEmptyDir() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    RemoteIterator<FileStatus> fsItr = getFileSystem()
        .listStatusIterator(testDir);
    Assertions.assertThat(fsItr.hasNext())
        .describedAs("hasNext returns false for empty directory")
        .isFalse();
  }

  @Test
  public void testHasNextForFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String testFileName = "testFile";
    Path testFile = new Path(testFileName);
    getFileSystem().create(testFile);
    setPageSize(10);
    RemoteIterator<FileStatus> fsItr = fs.listStatusIterator(testFile);
    Assertions.assertThat(fsItr.hasNext())
        .describedAs("hasNext returns true for file").isTrue();
    Assertions.assertThat(fsItr.next().getPath().toString())
        .describedAs("next returns the file itself")
        .endsWith(testFileName);
  }

  @Test
  public void testIOException() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    getFileSystem().mkdirs(testDir);

    String exceptionMessage = "test exception";
    ListingSupport lsSupport =getMockListingSupport(exceptionMessage);
    RemoteIterator fsItr =
        new AbfsListStatusRemoteIterator(getFileSystem().getFileStatus(testDir),
        lsSupport);

    Assertions.assertThatThrownBy(() -> fsItr.next())
        .describedAs(
        "When ioException is not null and queue is empty exception should be "
            + "thrown")
        .isInstanceOf(IOException.class)
        .hasMessage(exceptionMessage);
  }

  @Test
  public void testNonExistingPath() throws Throwable {
    Path nonExistingDir = new Path("nonExistingPath");
    Assertions.assertThatThrownBy(
        () -> getFileSystem().listStatusIterator(nonExistingDir)).describedAs(
        "test the listStatusIterator call on a path which is not "
            + "present should result in FileNotFoundException")
        .isInstanceOf(FileNotFoundException.class);
  }

  private ListingSupport getMockListingSupport(String exceptionMessage) {
    return new ListingSupport() {
      @Override
      public FileStatus[] listStatus(Path path) throws IOException {
        return null;
      }

      @Override
      public FileStatus[] listStatus(Path path, String startFrom)
          throws IOException {
        return null;
      }

      @Override
      public String listStatus(Path path, String startFrom,
          List<FileStatus> fileStatuses, boolean fetchAll, String continuation)
          throws IOException {
        throw new IOException(exceptionMessage);
      }
    };
  }

  private Path createTestDirectory() throws IOException {
    String testDirectoryName = "testDirectory" + System.currentTimeMillis();
    Path testDirectory = new Path(testDirectoryName);
    getFileSystem().mkdirs(testDirectory);
    return testDirectory;
  }

  private void setEnableAbfsIterator(boolean shouldEnable) throws IOException {
    AzureBlobFileSystemStore abfsStore = getAbfsStore(getFileSystem());
    abfsStore.getAbfsConfiguration().setEnableAbfsListIterator(shouldEnable);
  }

  private void setPageSize(int pageSize) throws IOException {
    AzureBlobFileSystemStore abfsStore = getAbfsStore(getFileSystem());
    abfsStore.getAbfsConfiguration().setListMaxResults(pageSize);
  }

  private List<String> createFilesUnderDirectory(int numFiles, Path rootPath,
      String filenamePrefix)
      throws ExecutionException, InterruptedException, IOException {
    final List<Future<Void>> tasks = new ArrayList<>();
    final List<String> fileNames = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < numFiles; i++) {
        final Path filePath = new Path(rootPath, filenamePrefix + i);
        Callable<Void> callable = () -> {
          getFileSystem().create(filePath);
          fileNames.add(makeQualified(filePath).toString());
          return null;
        };
        tasks.add(es.submit(callable));
      }
      for (Future<Void> task : tasks) {
        task.get();
      }
    } finally {
      es.shutdownNow();
    }
    return fileNames;
  }

}
