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

import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertTrue;

/**
 * Test mkdir operation.
 */
public class ITestAzureBlobFileSystemMkDir extends DependencyInjectedTest {
  public ITestAzureBlobFileSystemMkDir() {
    super();
  }

  @Test
  public void testCreateDirWithExistingDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assertTrue(fs.mkdirs(new Path("testFolder")));
    assertTrue(fs.mkdirs(new Path("testFolder")));
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void createDirectoryUnderFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("testFile"));
    fs.mkdirs(new Path("testFile/TestDirectory"));
  }

  @Test
  public void testCreateDirectoryOverExistingFiles() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(new Path("/testPath"));
    FileAlreadyExistsException ex = intercept(
        FileAlreadyExistsException.class,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            fs.mkdirs(new Path("/testPath"));
            return null;
          }
        });

    assertTrue(ex instanceof FileAlreadyExistsException);

    fs.create(new Path("/testPath1/file1"));
    ex = intercept(
        FileAlreadyExistsException.class,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            fs.mkdirs(new Path("/testPath1/file1"));
            return null;
          }
        });

    assertTrue(ex instanceof FileAlreadyExistsException);
  }

  @Test
  public void testCreateRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assertTrue(fs.mkdirs(new Path("/")));
  }
}
