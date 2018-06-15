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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertEquals;

/**
 * Test delete operation.
 */
public class ITestAzureBlobFileSystemDelete extends DependencyInjectedTest {
  public ITestAzureBlobFileSystemDelete() {
    super();
  }

  @Test
  public void testDeleteRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    fs.mkdirs(new Path("/testFolder0"));
    fs.mkdirs(new Path("/testFolder1"));
    fs.mkdirs(new Path("/testFolder2"));
    fs.create(new Path("/testFolder1/testfile"));
    fs.create(new Path("/testFolder1/testfile2"));
    fs.create(new Path("/testFolder1/testfile3"));

    FileStatus[] ls = fs.listStatus(new Path("/"));
    assertEquals(4, ls.length); // and user dir

    fs.delete(new Path("/"), true);
    ls = fs.listStatus(new Path("/"));
    assertEquals(0, ls.length);
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("/testFile"));
    fs.delete(new Path("/testFile"), false);

    fs.open(new Path("/testFile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testEnsureFileIsDeleted() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("testfile"));
    fs.delete(new Path("testfile"), false);

    fs.getFileStatus(new Path("testfile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testDeleteDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testfile"));
    fs.mkdirs(new Path("testfile/test1"));
    fs.mkdirs(new Path("testfile/test1/test2"));

    fs.delete(new Path("testfile"), true);
    fs.getFileStatus(new Path("testfile"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testDeleteFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final List<Future> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          fs.create(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    fs.delete(new Path("/test"), true);
    fs.getFileStatus(new Path("/test"));
  }
}
