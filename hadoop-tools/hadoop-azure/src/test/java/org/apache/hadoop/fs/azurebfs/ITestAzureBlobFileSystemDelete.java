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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test delete operation.
 */
public class ITestAzureBlobFileSystemDelete extends
    AbstractAbfsIntegrationTest {
  public ITestAzureBlobFileSystemDelete() {
    super();
  }

  @Test
  public void testDeleteRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    fs.mkdirs(new Path("/testFolder0"));
    fs.mkdirs(new Path("/testFolder1"));
    fs.mkdirs(new Path("/testFolder2"));
    touch(new Path("/testFolder1/testfile"));
    touch(new Path("/testFolder1/testfile2"));
    touch(new Path("/testFolder1/testfile3"));

    Path root = new Path("/");
    FileStatus[] ls = fs.listStatus(root);
    assertEquals(3, ls.length);

    fs.delete(root, true);
    ls = fs.listStatus(root);
    assertEquals("listing size", 0, ls.length);
  }

  @Test()
  public void testOpenFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("/testFile");
    touch(testfile);
    assertDeleted(fs, testfile, false);

    intercept(FileNotFoundException.class,
        () -> fs.open(testfile));
  }

  @Test
  public void testEnsureFileIsDeleted() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testfile = new Path("testfile");
    touch(testfile);
    assertDeleted(fs, testfile, false);
    assertPathDoesNotExist(fs, "deleted", testfile);
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dir = new Path("testfile");
    fs.mkdirs(dir);
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);
  }

  @Test
  public void testDeleteFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
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
    Path dir = new Path("/test");
    // first try a non-recursive delete, expect failure
    intercept(FileAlreadyExistsException.class,
        () -> fs.delete(dir, false));
    assertDeleted(fs, dir, true);
    assertPathDoesNotExist(fs, "deleted", dir);

  }
}
