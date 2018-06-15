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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test listStatus operation.
 */
public class ITestAzureBlobFileSystemListStatus extends DependencyInjectedTest {
  private static final int TEST_FILES_NUMBER = 6000;
  public ITestAzureBlobFileSystemListStatus() {
    super();
  }

  @Test
  public void testListPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    final List<Future> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < TEST_FILES_NUMBER; i++) {
      final Path fileName = new Path("/test" + i);
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
    FileStatus[] files = fs.listStatus(new Path("/"));
    Assert.assertEquals(files.length, TEST_FILES_NUMBER + 1 /* user directory */);
  }

  @Test
  public void testListFileVsListDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("/testFile"));

    FileStatus[] testFiles = fs.listStatus(new Path("/testFile"));
    Assert.assertEquals(testFiles.length, 1);
    Assert.assertFalse(testFiles[0].isDirectory());
  }

  @Test
  public void testListFileVsListDir2() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("/testFolder"));
    fs.mkdirs(new Path("/testFolder/testFolder2"));
    fs.mkdirs(new Path("/testFolder/testFolder2/testFolder3"));
    fs.create(new Path("/testFolder/testFolder2/testFolder3/testFile"));

    FileStatus[] testFiles = fs.listStatus(new Path("/testFolder/testFolder2/testFolder3/testFile"));
    Assert.assertEquals(testFiles.length, 1);
    Assert.assertEquals(testFiles[0].getPath(), new Path(this.getTestUrl(),
        "/testFolder/testFolder2/testFolder3/testFile"));
    Assert.assertFalse(testFiles[0].isDirectory());
  }

  @Test(expected = FileNotFoundException.class)
  public void testListNonExistentDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.listStatus(new Path("/testFile/"));
  }

  @Test
  public void testListFiles() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("/test"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
    assertEquals(fileStatuses.length, 2);

    fs.mkdirs(new Path("/test/sub"));
    fileStatuses = fs.listStatus(new Path("/test"));
    assertEquals(fileStatuses.length, 1);
    assertEquals(fileStatuses[0].getPath().getName(), "sub");
    assertTrue(fileStatuses[0].isDirectory());
    assertEquals(fileStatuses[0].getLen(), 0);

    fs.create(new Path("/test/f"));
    fileStatuses = fs.listStatus(new Path("/test"));
    assertEquals(fileStatuses.length, 2);
    assertEquals(fileStatuses[0].getPath().getName(), "f");
    assertFalse(fileStatuses[0].isDirectory());
    assertEquals(fileStatuses[0].getLen(), 0);
    assertEquals(fileStatuses[1].getPath().getName(), "sub");
    assertTrue(fileStatuses[1].isDirectory());
    assertEquals(fileStatuses[1].getLen(), 0);
  }
}
