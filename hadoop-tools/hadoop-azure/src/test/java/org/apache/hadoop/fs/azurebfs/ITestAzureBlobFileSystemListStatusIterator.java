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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatusIterator
    extends AbstractAbfsIntegrationTest {

  private static final int TEST_FILES_NUMBER = 100;

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

  @Test(expected = NoSuchElementException.class)
  public void testNextWhenNoMoreElementsPresent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String rootPathStr = "testRoot2";
    Path rootPath = new Path(rootPathStr);
    getFileSystem().create(rootPath);
    RemoteIterator<FileStatus> fsItr = fs.listStatusIterator(rootPath);
    fsItr = Mockito.spy(fsItr);
    Mockito.doReturn(false).when(fsItr).hasNext();
    fsItr.next();
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
