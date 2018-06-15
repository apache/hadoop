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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends DependencyInjectedTest {
  public ITestAzureBlobFileSystemRename() {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("testfile"));
    fs.rename(new Path("testfile"), new Path("testfile2"));

    FileStatus fileStatus = fs.getFileStatus(new Path("testfile2"));
    assertNotNull(fileStatus);

    fs.getFileStatus(new Path("testfile"));
  }

  @Test
  public void testRenameFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("/testSrc"));
    fs.create(new Path("/testSrc/file1"));

    fs.rename(new Path("/testSrc"), new Path("/testDst"));
    FileStatus[] fileStatus = fs.listStatus(new Path("/testDst"));
    assertNotNull(fileStatus);
  }

  @Test
  public void testRenameFileUsingUnicode() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    //known issue: ListStatus operation to folders/files whose name contains '?' will fail
    //This is because Auto rest client didn't encode '?' in the uri query parameters
    String[] folders1 = new String[]{"/%2c%26", "/ÖáΠ⇒", "/A +B", "/A~`!@#$%^&*()-_+={};:'>,<B"};
    String[] folders2 = new String[]{"/abcÖ⇒123", "/abcÖáΠ⇒123", "/B+ C", "/B~`!@#$%^&*()-_+={};:'>,<C"};
    String[] files = new String[]{"/%2c%27", "/中文", "/C +D", "/C~`!@#$%^&*()-_+={};:'>,<D"};

    for (int i = 0; i < 4; i++) {
      Path folderPath1 = new Path(folders1[i]);
      assertTrue(fs.mkdirs(folderPath1));
      assertTrue(fs.exists(folderPath1));

      Path filePath = new Path(folders1[i] + files[i]);
      fs.create(filePath);
      assertTrue(fs.exists(filePath));

      Path folderPath2 = new Path(folders2[i]);
      fs.rename(folderPath1, folderPath2);
      assertFalse(fs.exists(folderPath1));
      assertTrue(fs.exists(folderPath2));

      FileStatus[] fileStatus = fs.listStatus(folderPath2);
      assertEquals("/" + fileStatus[0].getPath().getName(), files[i]);
      assertNotNull(fileStatus);
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir"));
    fs.mkdirs(new Path("testDir/test1"));
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    Assert.assertTrue(fs.rename(new Path("testDir/test1"), new Path("testDir/test10")));
    fs.getFileStatus(new Path("testDir/test1"));
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameFirstLevelDirectory() throws Exception {
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
    fs.rename(new Path("/test"), new Path("/renamedDir"));

    FileStatus[] files = fs.listStatus(new Path("/renamedDir"));
    Assert.assertEquals(files.length, 1000);
    fs.getFileStatus(new Path("/test"));
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    boolean renamed = fs.rename(new Path("/"), new Path("/ddd"));
    assertFalse(renamed);

    renamed = fs.rename(new Path(fs.getUri().toString() + "/"), new Path(fs.getUri().toString() + "/s"));
    assertFalse(renamed);
  }
}
