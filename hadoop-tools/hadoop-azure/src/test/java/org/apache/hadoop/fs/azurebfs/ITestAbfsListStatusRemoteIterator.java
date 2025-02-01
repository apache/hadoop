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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.services.AbfsListStatusRemoteIterator;
import org.apache.hadoop.fs.azurebfs.services.ListingSupport;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.test.LambdaTestUtils;

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
  private static final Logger LOG = LoggerFactory.getLogger(
      ITestAbfsListStatusRemoteIterator.class);

  public ITestAbfsListStatusRemoteIterator() throws Exception {
  }

  @Test
  public void testAbfsIteratorWithHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    final List<String> fileNames = createFilesUnderDirectory(testDir);

    ListingSupport listingSupport = Mockito.spy(getFileSystem().getAbfsStore());
    RemoteIterator<FileStatus> fsItr = new AbfsListStatusRemoteIterator(testDir,
        listingSupport, getTestTracingContext(getFileSystem(), true));
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should be instance of "
            + "AbfsListStatusRemoteIterator by default")
        .isInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    while (fsItr.hasNext()) {
      FileStatus fileStatus = fsItr.next();
      verifyIteratorResultContent(fileStatus, fileNames);
      itrCount++;
    }
    verifyIteratorResultCount(itrCount, fileNames);
    int minNumberOfInvocations = TEST_FILES_NUMBER / 10;
    verify(listingSupport, Mockito.atLeast(minNumberOfInvocations))
        .listStatus(any(Path.class), nullable(String.class),
            anyList(), anyBoolean(),
            nullable(String.class),
            any(TracingContext.class));
  }

  @Test
  public void testAbfsIteratorWithoutHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    final List<String> fileNames = createFilesUnderDirectory(testDir);

    ListingSupport listingSupport = Mockito.spy(getFileSystem().getAbfsStore());
    RemoteIterator<FileStatus> fsItr = new AbfsListStatusRemoteIterator(testDir,
        listingSupport, getTestTracingContext(getFileSystem(), true));
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should be instance of "
            + "AbfsListStatusRemoteIterator by default")
        .isInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      FileStatus fileStatus = fsItr.next();
      verifyIteratorResultContent(fileStatus, fileNames);
      itrCount++;
    }
    LambdaTestUtils.intercept(NoSuchElementException.class, fsItr::next);
    verifyIteratorResultCount(itrCount, fileNames);
    int minNumberOfInvocations = TEST_FILES_NUMBER / 10;
    verify(listingSupport, Mockito.atLeast(minNumberOfInvocations))
        .listStatus(any(Path.class), nullable(String.class),
            anyList(), anyBoolean(),
            nullable(String.class),
            any(TracingContext.class));
  }

  @Test
  public void testWithAbfsIteratorDisabled() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    disableAbfsIterator();
    final List<String> fileNames = createFilesUnderDirectory(testDir);

    RemoteIterator<FileStatus> fsItr =
        getFileSystem().listStatusIterator(testDir);
    Assertions.assertThat(fsItr)
        .describedAs("RemoteIterator should not be instance of "
            + "AbfsListStatusRemoteIterator when it is disabled")
        .isNotInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount = 0;
    while (fsItr.hasNext()) {
      FileStatus fileStatus = fsItr.next();
      verifyIteratorResultContent(fileStatus, fileNames);
      itrCount++;
    }
    verifyIteratorResultCount(itrCount, fileNames);
  }

  @Test
  public void testWithAbfsIteratorDisabledWithoutHasNext() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    disableAbfsIterator();
    final List<String> fileNames = createFilesUnderDirectory(testDir);

    RemoteIterator<FileStatus> fsItr = getFileSystem().listStatusIterator(
        testDir);
    Assertions.assertThat(fsItr).describedAs(
            "RemoteIterator should not be instance of "
                + "AbfsListStatusRemoteIterator when it is disabled")
        .isNotInstanceOf(AbfsListStatusRemoteIterator.class);
    int itrCount;
    for (itrCount = 0; itrCount < TEST_FILES_NUMBER; itrCount++) {
      FileStatus fileStatus = fsItr.next();
      verifyIteratorResultContent(fileStatus, fileNames);
    }
    LambdaTestUtils.intercept(NoSuchElementException.class, fsItr::next);
    verifyIteratorResultCount(itrCount, fileNames);
  }

  @Test
  public void testNextWhenNoMoreElementsPresent() throws Exception {
    Path testDir = createTestDirectory();
    setPageSize(10);
    RemoteIterator<FileStatus> fsItr = new AbfsListStatusRemoteIterator(testDir,
        getFileSystem().getAbfsStore(),
        getTestTracingContext(getFileSystem(), true));
    fsItr = Mockito.spy(fsItr);
    Mockito.doReturn(false).when(fsItr).hasNext();

    LambdaTestUtils.intercept(NoSuchElementException.class, fsItr::next);
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
    Path testFile = path("testFile");
    String testFileName = testFile.toString();
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
    ListingSupport lsSupport = getMockListingSupport(exceptionMessage);

    LambdaTestUtils.intercept(IOException.class,
        () -> new AbfsListStatusRemoteIterator(testDir, lsSupport,
            getTestTracingContext(getFileSystem(), true)));
  }

  @Test
  public void testNonExistingPath() throws Exception {
    Path nonExistingDir = new Path("nonExistingPath");
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> getFileSystem().listStatusIterator(nonExistingDir));
  }

  private void verifyIteratorResultContent(FileStatus fileStatus,
      List<String> fileNames) {
    assertPathDns(fileStatus.getPath());
    String pathStr = fileStatus.getPath().toString();
    Assert.assertTrue(
        String.format("Could not remove path %s from filenames %s", pathStr,
            fileNames), fileNames.remove(pathStr));
  }

  private void verifyIteratorResultCount(int itrCount, List<String> fileNames) {
    Assertions.assertThat(itrCount).describedAs(
            "Number of iterations should be equal to the files created")
        .isEqualTo(TEST_FILES_NUMBER);
    Assertions.assertThat(fileNames)
        .describedAs("After removing every item found from the iterator, "
            + "there should be no more elements in the fileNames")
        .hasSize(0);
  }

  private ListingSupport getMockListingSupport(String exceptionMessage) {
    return new ListingSupport() {
      @Override
      public FileStatus[] listStatus(Path path, TracingContext tracingContext) {
        return null;
      }

      @Override
      public FileStatus[] listStatus(Path path, String startFrom, TracingContext tracingContext) {
        return null;
      }

      @Override
      public String listStatus(Path path, String startFrom,
          List<FileStatus> fileStatuses, boolean fetchAll,
          String continuation, TracingContext tracingContext)
          throws IOException {
        throw new IOException(exceptionMessage);
      }
    };
  }

  private Path createTestDirectory() throws IOException {
    Path testDirectory = path("testDirectory");
    getFileSystem().mkdirs(testDirectory);
    return testDirectory;
  }

  private void disableAbfsIterator() throws IOException {
    AzureBlobFileSystemStore abfsStore = getAbfsStore(getFileSystem());
    abfsStore.getAbfsConfiguration().setEnableAbfsListIterator(false);
  }

  private void setPageSize(int pageSize) throws IOException {
    AzureBlobFileSystemStore abfsStore = getAbfsStore(getFileSystem());
    abfsStore.getAbfsConfiguration().setListMaxResults(pageSize);
  }

  private List<String> createFilesUnderDirectory(Path rootPath)
      throws ExecutionException, InterruptedException, IOException {
    final List<Future<Void>> tasks = new ArrayList<>();
    final List<String> fileNames = Collections.synchronizedList(new ArrayList<>());
    ExecutorService es = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < ITestAbfsListStatusRemoteIterator.TEST_FILES_NUMBER; i++) {
        Path filePath = makeQualified(new Path(rootPath, "testListPath" + i));
        tasks.add(es.submit(() -> {
          touch(filePath);
          synchronized (fileNames) {
            Assert.assertTrue(fileNames.add(filePath.toString()));
          }
          return null;
        }));
      }
      for (Future<Void> task : tasks) {
        task.get();
      }
    } finally {
      es.shutdownNow();
    }
    LOG.debug(fileNames.toString());
    Assertions.assertThat(fileNames)
        .describedAs("File creation incorrect or fileNames not added to list")
        .hasSize(ITestAbfsListStatusRemoteIterator.TEST_FILES_NUMBER);
    return fileNames;
  }

}
