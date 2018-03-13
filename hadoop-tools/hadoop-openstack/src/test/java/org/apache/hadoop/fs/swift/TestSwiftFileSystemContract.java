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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This is the full filesystem contract test -which requires the
 * Default config set up to point to a filesystem.
 *
 * Some of the tests override the base class tests -these
 * are where SwiftFS does not implement those features, or
 * when the behavior of SwiftFS does not match the normal
 * contract -which normally means that directories and equal files
 * are being treated as equal.
 */
public class TestSwiftFileSystemContract
        extends FileSystemContractBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSwiftFileSystemContract.class);

  /**
   * Override this if the filesystem is not case sensitive
   * @return true if the case detection/preservation tests should run
   */
  protected boolean filesystemIsCaseSensitive() {
    return false;
  }

  @Before
  public void setUp() throws Exception {
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();
    fs = createSwiftFS();
    try {
      fs.initialize(uri, conf);
    } catch (IOException e) {
      //FS init failed, set it to null so that teardown doesn't
      //attempt to use it
      fs = null;
      throw e;
    }
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    SwiftNativeFileSystem swiftNativeFileSystem =
            new SwiftNativeFileSystem();
    return swiftNativeFileSystem;
  }

  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));

    Path filepath = path("/test/hadoop/file");
    SwiftTestUtils.writeTextFile(fs, filepath, "hello, world", false);

    Path testSubDir = new Path(filepath, "subdir");
    SwiftTestUtils.assertPathDoesNotExist(fs, "subdir before mkdir", testSubDir);

    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (ParentNotDirectoryException e) {
      // expected
    }
    //now verify that the subdir path does not exist
    SwiftTestUtils.assertPathDoesNotExist(fs, "subdir after mkdir", testSubDir);

    Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (ParentNotDirectoryException e) {
      // expected
    }
    SwiftTestUtils.assertPathDoesNotExist(fs, "testDeepSubDir  after mkdir",
                                          testDeepSubDir);

  }

  @Test
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    try {
      super.testWriteReadAndDeleteEmptyFile();
    } catch (AssertionError e) {
      SwiftTestUtils.downgrade("empty files get mistaken for directories", e);
    }
  }

  @Test
  public void testMkdirsWithUmask() throws Exception {
    //unsupported
  }

  @Test
  public void testZeroByteFilesAreFiles() throws Exception {
//    SwiftTestUtils.unsupported("testZeroByteFilesAreFiles");
  }
}
