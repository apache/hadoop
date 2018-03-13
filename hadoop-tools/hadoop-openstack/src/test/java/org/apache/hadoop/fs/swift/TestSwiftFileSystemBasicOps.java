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

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftBadRequestException;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.assertFileHasLength;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.readBytesToString;
import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.writeTextFile;


/**
 * Test basic filesystem operations.
 * Many of these are similar to those in {@link TestSwiftFileSystemContract}
 * -this is a JUnit4 test suite used to initially test the Swift
 * component. Once written, there's no reason not to retain these tests.
 */
public class TestSwiftFileSystemBasicOps extends SwiftFileSystemBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSwiftFileSystemBasicOps.class);

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testLsRoot() throws Throwable {
    Path path = new Path("/");
    FileStatus[] statuses = fs.listStatus(path);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testMkDir() throws Throwable {
    Path path = new Path("/test/MkDir");
    fs.mkdirs(path);
    //success then -so try a recursive operation
    fs.delete(path, true);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testDeleteNonexistentFile() throws Throwable {
    Path path = new Path("/test/DeleteNonexistentFile");
    assertFalse("delete returned true", fs.delete(path, false));
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPutFile() throws Throwable {
    Path path = new Path("/test/PutFile");
    Exception caught = null;
    writeTextFile(fs, path, "Testing a put to a file", false);
    assertDeleted(path, false);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPutGetFile() throws Throwable {
    Path path = new Path("/test/PutGetFile");
    try {
      String text = "Testing a put and get to a file "
              + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);

      String result = readBytesToString(fs, path, text.length());
      assertEquals(text, result);
    } finally {
      delete(fs, path);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPutDeleteFileInSubdir() throws Throwable {
    Path path =
            new Path("/test/PutDeleteFileInSubdir/testPutDeleteFileInSubdir");
    String text = "Testing a put and get to a file in a subdir "
            + System.currentTimeMillis();
    writeTextFile(fs, path, text, false);
    assertDeleted(path, false);
    //now delete the parent that should have no children
    assertDeleted(new Path("/test/PutDeleteFileInSubdir"), false);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRecursiveDelete() throws Throwable {
    Path childpath =
            new Path("/test/testRecursiveDelete");
    String text = "Testing a put and get to a file in a subdir "
            + System.currentTimeMillis();
    writeTextFile(fs, childpath, text, false);
    //now delete the parent that should have no children
    assertDeleted(new Path("/test"), true);
    assertFalse("child entry still present " + childpath, fs.exists(childpath));
  }

  private void delete(SwiftNativeFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, false)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  private void deleteR(SwiftNativeFileSystem fs, Path path) {
    try {
      if (!fs.delete(path, true)) {
        LOG.warn("Failed to delete " + path);
      }
    } catch (IOException e) {
      LOG.warn("deleting " + path, e);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testOverwrite() throws Throwable {
    Path path = new Path("/test/Overwrite");
    try {
      String text = "Testing a put to a file "
              + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);
      assertFileHasLength(fs, path, text.length());
      String text2 = "Overwriting a file "
              + System.currentTimeMillis();
      writeTextFile(fs, path, text2, true);
      assertFileHasLength(fs, path, text2.length());
      String result = readBytesToString(fs, path, text2.length());
      assertEquals(text2, result);
    } finally {
      delete(fs, path);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testOverwriteDirectory() throws Throwable {
    Path path = new Path("/test/testOverwriteDirectory");
    try {
      fs.mkdirs(path.getParent());
      String text = "Testing a put to a file "
              + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);
      assertFileHasLength(fs, path, text.length());
    } finally {
      delete(fs, path);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testFileStatus() throws Throwable {
    Path path = new Path("/test/FileStatus");
    try {
      String text = "Testing File Status "
              + System.currentTimeMillis();
      writeTextFile(fs, path, text, false);
      SwiftTestUtils.assertIsFile(fs, path);
    } finally {
      delete(fs, path);
    }
  }

  /**
   * Assert that a newly created directory is a directory
   *
   * @throws Throwable if not, or if something else failed
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testDirStatus() throws Throwable {
    Path path = new Path("/test/DirStatus");
    try {
      fs.mkdirs(path);
      assertIsDirectory(fs, path);
    } finally {
      delete(fs, path);
    }
  }

  /**
   * Assert that if a directory that has children is deleted, it is still
   * a directory
   *
   * @throws Throwable if not, or if something else failed
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testDirStaysADir() throws Throwable {
    Path path = new Path("/test/dirStaysADir");
    Path child = new Path(path, "child");
    try {
      //create the dir
      fs.mkdirs(path);
      //assert the parent has the directory nature
      assertIsDirectory(fs, path);
      //create the child dir
      writeTextFile(fs, child, "child file", true);
      //assert the parent has the directory nature
      assertIsDirectory(fs, path);
      //now rm the child
      delete(fs, child);
    } finally {
      deleteR(fs, path);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCreateMultilevelDir() throws Throwable {
    Path base = new Path("/test/CreateMultilevelDir");
    Path path = new Path(base, "1/2/3");
    fs.mkdirs(path);
    assertExists("deep multilevel dir not created", path);
    fs.delete(base, true);
    assertPathDoesNotExist("Multilevel delete failed", path);
    assertPathDoesNotExist("Multilevel delete failed", base);

  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCreateDirWithFileParent() throws Throwable {
    Path path = new Path("/test/CreateDirWithFileParent");
    Path child = new Path(path, "subdir/child");
    fs.mkdirs(path.getParent());
    try {
      //create the child dir
      writeTextFile(fs, path, "parent", true);
      try {
        fs.mkdirs(child);
      } catch (ParentNotDirectoryException expected) {
        LOG.debug("Expected Exception", expected);
      }
    } finally {
      fs.delete(path, true);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testLongObjectNamesForbidden() throws Throwable {
    StringBuilder buffer = new StringBuilder(1200);
    buffer.append("/");
    for (int i = 0; i < (1200 / 4); i++) {
      buffer.append(String.format("%04x", i));
    }
    String pathString = buffer.toString();
    Path path = new Path(pathString);
    try {
      writeTextFile(fs, path, pathString, true);
      //if we get here, problems.
      fs.delete(path, false);
      fail("Managed to create an object with a name of length "
              + pathString.length());
    } catch (SwiftBadRequestException e) {
      //expected
      //LOG.debug("Caught exception " + e, e);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testLsNonExistentFile() throws Exception {
    try {
      Path path = new Path("/test/hadoop/file");
      FileStatus[] statuses = fs.listStatus(path);
      fail("Should throw FileNotFoundException on " + path
              + " but got list of length " + statuses.length);
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testGetCanonicalServiceName() {
    Assert.assertNull(fs.getCanonicalServiceName());
  }


}
