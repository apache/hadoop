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
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.assertListStatusFinds;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.cleanup;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.dumpStats;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.touch;

/**
 * Test the FileSystem#listStatus() operations
 */
public class TestSwiftFileSystemLsOperations extends SwiftFileSystemBaseTest {

  private Path[] testDirs;

    /**
   * Setup creates dirs under test/hadoop
   *
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

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListLevelTest() throws Exception {
    createTestSubdirs();
    FileStatus[] paths = fs.listStatus(path("/test"));
    assertEquals(dumpStats("/test", paths), 1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListLevelTestHadoop() throws Exception {
    createTestSubdirs();
    FileStatus[] paths;
    paths = fs.listStatus(path("/test/hadoop"));
    String stats = dumpStats("/test/hadoop", paths);
    assertEquals("Paths.length wrong in " + stats, 3, paths.length);
    assertEquals("Path element[0] wrong: " + stats, path("/test/hadoop/a"),
                 paths[0].getPath());
    assertEquals("Path element[1] wrong: " + stats, path("/test/hadoop/b"),
                 paths[1].getPath());
    assertEquals("Path element[2] wrong: " + stats, path("/test/hadoop/c"),
                 paths[2].getPath());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListStatusEmptyDirectory() throws Exception {
    createTestSubdirs();
    FileStatus[] paths;
    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(dumpStats("/test/hadoop/a", paths), 0,
                 paths.length);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListStatusFile() throws Exception {
    describe("Create a single file under /test;" +
             " assert that listStatus(/test) finds it");
    Path file = path("/test/filename");
    createFile(file);
    FileStatus[] pathStats = fs.listStatus(file);
    assertEquals(dumpStats("/test/", pathStats),
                 1,
                 pathStats.length);
    //and assert that the len of that ls'd path is the same as the original
    FileStatus lsStat = pathStats[0];
    assertEquals("Wrong file len in listing of " + lsStat,
      data.length, lsStat.getLen());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListEmptyRoot() throws Throwable {
    describe("Empty the root dir and verify that an LS / returns {}");
    cleanup("testListEmptyRoot", fs, "/test");
    cleanup("testListEmptyRoot", fs, "/user");
    FileStatus[] fileStatuses = fs.listStatus(path("/"));
    assertEquals("Non-empty root" + dumpStats("/", fileStatuses),
                 0,
                 fileStatuses.length);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListNonEmptyRoot() throws Throwable {
    Path test = path("/test");
    touch(fs, test);
    FileStatus[] fileStatuses = fs.listStatus(path("/"));
    String stats = dumpStats("/", fileStatuses);
    assertEquals("Wrong #of root children" + stats, 1, fileStatuses.length);
    FileStatus status = fileStatuses[0];
    assertEquals("Wrong path value" + stats,test, status.getPath());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListStatusRootDir() throws Throwable {
    Path dir = path("/");
    Path child = path("/test");
    touch(fs, child);
    assertListStatusFinds(fs, dir, child);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testListStatusFiltered() throws Throwable {
    Path dir = path("/");
    Path child = path("/test");
    touch(fs, child);
    FileStatus[] stats = fs.listStatus(dir, new AcceptAllFilter());
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(child)) {
        found = true;
      }
    }
    assertTrue("Path " + child
                      + " not found in directory " + dir + ":" + builder,
                      found);
  }

}
