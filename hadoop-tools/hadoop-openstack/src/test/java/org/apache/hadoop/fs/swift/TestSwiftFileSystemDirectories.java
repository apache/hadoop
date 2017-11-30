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

package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftFileStatus;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;

/**
 * Test swift-specific directory logic.
 * This class is HDFS-1 compatible; its designed to be subclasses by something
 * with HDFS2 extensions
 */
public class TestSwiftFileSystemDirectories extends SwiftFileSystemBaseTest {

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testZeroByteFilesAreDirectories() throws Exception {
    Path src = path("/test/testZeroByteFilesAreFiles");
    //create a zero byte file
    SwiftTestUtils.touch(fs, src);
    SwiftTestUtils.assertIsDirectory(fs, src);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testNoStatusForMissingDirectories() throws Throwable {
    Path missing = path("/test/testNoStatusForMissingDirectories");
    assertPathDoesNotExist("leftover?", missing);
    try {
      FileStatus[] statuses = fs.listStatus(missing);
      //not expected
      fail("Expected a FileNotFoundException, got the status " + statuses);
    } catch (FileNotFoundException expected) {
      //expected
    }
  }

  /**
   * test that a dir off root has a listStatus() call that
   * works as expected. and that when a child is added. it changes
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testDirectoriesOffRootHaveMatchingFileStatus() throws Exception {
    Path test = path("/test");
    fs.delete(test, true);
    mkdirs(test);
    assertExists("created test directory", test);
    FileStatus[] statuses = fs.listStatus(test);
    String statusString = statusToString(test.toString(), statuses);
    assertEquals("Wrong number of elements in file status " + statusString, 0,
                 statuses.length);

    Path src = path("/test/file");

    //create a zero byte file
    SwiftTestUtils.touch(fs, src);
    //stat it
    statuses = fs.listStatus(test);
    statusString = statusToString(test.toString(), statuses);
    assertEquals("Wrong number of elements in file status " + statusString, 1,
                 statuses.length);
    SwiftFileStatus stat = (SwiftFileStatus) statuses[0];
    assertTrue("isDir(): Not a directory: " + stat, stat.isDirectory());
    extraStatusAssertions(stat);
  }

  /**
   * test that a dir two levels down has a listStatus() call that
   * works as expected.
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testDirectoriesLowerDownHaveMatchingFileStatus() throws Exception {
    Path test = path("/test/testDirectoriesLowerDownHaveMatchingFileStatus");
    fs.delete(test, true);
    mkdirs(test);
    assertExists("created test sub directory", test);
    FileStatus[] statuses = fs.listStatus(test);
    String statusString = statusToString(test.toString(), statuses);
    assertEquals("Wrong number of elements in file status " + statusString,0,
                 statuses.length);
  }

  private String statusToString(String pathname,
                                FileStatus[] statuses) {
    assertNotNull(statuses);
    return SwiftTestUtils.dumpStats(pathname,statuses);
  }

  /**
   * method for subclasses to add extra assertions
   * @param stat status to look at
   */
  protected void extraStatusAssertions(SwiftFileStatus stat) {

  }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   *
   * @throws Exception on failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testMultiByteFilesAreFiles() throws Exception {
    Path src = path("/test/testMultiByteFilesAreFiles");
    SwiftTestUtils.writeTextFile(fs, src, "testMultiByteFilesAreFiles", false);
    assertIsFile(src);
    FileStatus status = fs.getFileStatus(src);
    assertFalse(status.isDirectory());
  }

}
