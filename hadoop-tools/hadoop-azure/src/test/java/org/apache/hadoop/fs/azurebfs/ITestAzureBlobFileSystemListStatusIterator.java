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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.services.ListStatusRemoteIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatusIterator
    extends AbstractAbfsIntegrationTest {

  private static final int TEST_FILES_NUMBER = 1000;

  public ITestAzureBlobFileSystemListStatusIterator() throws Exception {
    super();
  }

  @Test
  public void testListPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPath = "testRoot1";
    final List<String> fileNames = createFiles(TEST_FILES_NUMBER, rootPath,
        "testListPath");
    AzureBlobFileSystemStore abfsStore = getAbfsStore(fs);
    abfsStore.getAbfsConfiguration().setListMaxResults(10);
    RemoteIterator<FileStatus> fsIt = fs.listStatusIterator(new Path(rootPath));
    int itrCount = 0;
    while (fsIt.hasNext()) {
      FileStatus fileStatus = fsIt.next();
      String pathStr = fileStatus.getPath().toString();
      fileNames.remove(pathStr);
      itrCount++;
    }
    assertEquals(TEST_FILES_NUMBER, itrCount);
    assertEquals(0, fileNames.size());
  }

  @Test
  public void testNextWhenNoMoreElementsPresent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPathStr = "testRoot2";
    Path rootPath = new Path(rootPathStr);
    getFileSystem().mkdirs(rootPath);
    RemoteIterator<FileStatus> fsItr = fs.listStatusIterator(rootPath);
    fsItr = Mockito.spy(fsItr);
    Mockito.doReturn(false).when(fsItr).hasNext();

    RemoteIterator<FileStatus> finalFsItr = fsItr;
    Assertions.assertThatThrownBy(() -> finalFsItr.next()).describedAs(
        "next() should throw NoSuchElementException if hasNext() return "
            + "false").isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testHasNextForEmptyDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPathStr = "testRoot3";
    Path rootPath = new Path(rootPathStr);
    getFileSystem().mkdirs(rootPath);
    RemoteIterator<FileStatus> fsItr = fs.listStatusIterator(rootPath);
    Assertions.assertThat(fsItr.hasNext())
        .describedAs("hasNext returns false for empty directory").isFalse();
  }

  @Test
  public void testHasNextForFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPathStr = "testRoot4";
    Path rootPath = new Path(rootPathStr);
    getFileSystem().create(rootPath);
    RemoteIterator<FileStatus> fsItr = fs.listStatusIterator(rootPath);
    Assertions.assertThat(fsItr.hasNext())
        .describedAs("hasNext returns true for file").isTrue();
  }

  @Test
  public void testHasNextForIOException() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPathStr = "testRoot5";
    Path rootPath = new Path(rootPathStr);
    getFileSystem().mkdirs(rootPath);
    ListStatusRemoteIterator fsItr = (ListStatusRemoteIterator) fs
        .listStatusIterator(rootPath);
    Thread.sleep(1000);

    String exceptionMessage = "test exception";
    setPrivateField(fsItr, ListStatusRemoteIterator.class, "ioException",
        new IOException(exceptionMessage));
    setPrivateFinalField(fsItr, ListStatusRemoteIterator.class,
        "iteratorsQueue", new ArrayBlockingQueue<Iterator>(1));

    Assertions.assertThatThrownBy(() -> fsItr.hasNext()).describedAs(
        "When ioException is not null and queue is empty exception should be "
            + "thrown").isInstanceOf(IOException.class)
        .hasMessage(exceptionMessage);
  }

  private void setPrivateField(Object obj, Class classObj, String fieldName,
      Object value) throws NoSuchFieldException, IllegalAccessException {
    Field field = classObj.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, value);
  }

  private void setPrivateFinalField(Object obj, Class classObj,
      String fieldName, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = classObj.getDeclaredField(fieldName);
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(obj, value);
  }

  private List<String> createFiles(int numFiles, String rootPathStr,
      String filenamePrefix)
      throws ExecutionException, InterruptedException, IOException {
    final List<Future<Void>> tasks = new ArrayList<>();
    final List<String> fileNames = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    final Path rootPath = new Path(rootPathStr);
    for (int i = 0; i < numFiles; i++) {
      final Path filePath = new Path(rootPath, filenamePrefix + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          getFileSystem().create(filePath);
          fileNames.add(makeQualified(filePath).toString());
          return null;
        }
      };
      tasks.add(es.submit(callable));
    }
    for (Future<Void> task : tasks) {
      task.get();
    }
    es.shutdownNow();
    return fileNames;
  }

  private AzureBlobFileSystemStore getAbfsStore(FileSystem fs)
      throws NoSuchFieldException, IllegalAccessException {
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) fs;
    Field abfsStoreField = AzureBlobFileSystem.class
        .getDeclaredField("abfsStore");
    abfsStoreField.setAccessible(true);
    return (AzureBlobFileSystemStore) abfsStoreField.get(abfs);
  }

}
