/**
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

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.rename;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatusIterator extends
    AbstractAbfsIntegrationTest {
  private static final int TEST_FILES_NUMBER = 100;

  public ITestAzureBlobFileSystemListStatusIterator() throws Exception {
    super();
  }

  @Test
  public void testListPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      final Path fileName = new Path("testRoot/test" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          touch(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();

    AzureBlobFileSystemStore abfsStore = getAbfsStore(fs);
    abfsStore.getAbfsConfiguration().setListMaxResults(10);
    RemoteIterator<FileStatus> fsIt = fs.listStatusIterator(new Path(
        "user/bith/testRoot"));
    while(fsIt.hasNext()){
      FileStatus a = fsIt.next();
      System.out.println(a.getPath().toString().substring(50));
    }
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
