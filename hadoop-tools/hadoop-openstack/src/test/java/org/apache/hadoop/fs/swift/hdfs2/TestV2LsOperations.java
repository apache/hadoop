/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.hdfs2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.swift.SwiftFileSystemBaseTest;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.IOException;

public class TestV2LsOperations extends SwiftFileSystemBaseTest {

  private Path[] testDirs;

  /**
   * Setup creates dirs under test/hadoop
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //delete the test directory
    Path test = path("/test");
    fs.delete(test, true);
    mkdirs(test);
  }

  /**
   * Create subdirectories and files under test/ for those tests
   * that want them. Doing so adds overhead to setup and teardown,
   * so should only be done for those tests that need them.
   * @throws IOException on an IO problem
   */
  private void createTestSubdirs() throws IOException {
    testDirs = new Path[]{
      path("/test/hadoop/a"),
      path("/test/hadoop/b"),
      path("/test/hadoop/c/1"),
    };
    assertPathDoesNotExist("test directory setup", testDirs[0]);
    for (Path path : testDirs) {
      mkdirs(path);
    }
  }

  /**
   * To get this project to compile under Hadoop 1, this code needs to be
   * commented out
   *
   *
   * @param fs filesystem
   * @param dir dir
   * @param subdir subdir
   * @param recursive recurse?
   * @throws IOException IO problems
   */
  public static void assertListFilesFinds(FileSystem fs,
                                          Path dir,
                                          Path subdir,
                                          boolean recursive) throws IOException {
    RemoteIterator<LocatedFileStatus> iterator =
      fs.listFiles(dir, recursive);
    boolean found = false;
    int entries = 0;
    StringBuilder builder = new StringBuilder();
    while (iterator.hasNext()) {
      LocatedFileStatus next = iterator.next();
      entries++;
      builder.append(next.toString()).append('\n');
      if (next.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
               + " not found in directory " + dir + " : "
               + " entries=" + entries
               + " content"
               + builder.toString(),
               found);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListFilesRootDir() throws Throwable {
    Path dir = path("/");
    Path child = new Path(dir, "test");
    fs.delete(child, true);
    SwiftTestUtils.writeTextFile(fs, child, "text", false);
    assertListFilesFinds(fs, dir, child, false);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListFilesSubDir() throws Throwable {
    createTestSubdirs();
    Path dir = path("/test/subdir");
    Path child = new Path(dir, "text.txt");
    SwiftTestUtils.writeTextFile(fs, child, "text", false);
    assertListFilesFinds(fs, dir, child, false);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListFilesRecursive() throws Throwable {
    createTestSubdirs();
    Path dir = path("/test/recursive");
    Path child = new Path(dir, "hadoop/a/a.txt");
    SwiftTestUtils.writeTextFile(fs, child, "text", false);
    assertListFilesFinds(fs, dir, child, true);
  }

}
