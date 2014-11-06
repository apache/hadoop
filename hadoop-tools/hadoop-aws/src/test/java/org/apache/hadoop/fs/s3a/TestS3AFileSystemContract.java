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

package org.apache.hadoop.fs.s3a;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

/**
 *  Tests a live S3 system. If your keys and bucket aren't specified, all tests
 *  are marked as passed.
 *
 *  This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 *  TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 *  properly making it impossible to skip the tests if we don't have a valid
 *  bucket.
 **/
public class TestS3AFileSystemContract extends FileSystemContractBaseTest {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestS3AFileSystemContract.class);
  public static final String TEST_FS_S3A_NAME = "test.fs.s3a.name";

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    fs = S3ATestUtils.createTestFileSystem(conf);
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(path("test"), true);
    }
    super.tearDown();
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // not supported
  }

  @Override
  public void testRenameFileAsExistingFile() throws Exception {
    if (!renameSupported()) return;

    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    createFile(dst);
    // s3 doesn't support rename option
    // rename-overwrites-dest is always allowed.
    rename(src, dst, true, false, true);
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) {
      return;
    }

    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertFalse("Nested file1 exists",
                fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
                fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
               fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
               fs.exists(path("/test/new/newdir/subdir/file2")));
  }

//  @Override
  public void testMoveDirUnderParent() throws Throwable {
    // not support because
    // Fails if dst is a directory that is not empty.
  }
}
