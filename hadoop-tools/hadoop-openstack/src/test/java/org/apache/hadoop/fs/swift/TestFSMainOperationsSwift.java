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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.apache.hadoop.fs.swift.SwiftTestConstants.SWIFT_TEST_TIMEOUT;
import java.io.IOException;
import java.net.URI;

public class TestFSMainOperationsSwift extends FSMainOperationsBaseTest {

  @Override
  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    //small blocksize for faster remote tests
    conf.setInt(SwiftProtocolConstants.SWIFT_BLOCKSIZE, 2);
    URI serviceURI = SwiftTestUtils.getServiceURI(conf);
    fSys = FileSystem.get(serviceURI, conf);
    super.setUp();
  }

  private Path wd = null;

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return fSys;
  }

  @Override
  protected Path getDefaultWorkingDirectory() throws IOException {
    if (wd == null) {
      wd = fSys.getWorkingDirectory();
    }
    return wd;
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = getTestRootPath(fSys, "test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testListStatusThrowsExceptionForUnreadableDir() {
    SwiftTestUtils.skip("unsupported");
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testFsStatus() throws Exception {
    super.testFsStatus();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWorkingDirectory() throws Exception {
    super.testWorkingDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testMkdirs() throws Exception {
    super.testMkdirs();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    super.testMkdirsFailsForSubdirectoryOfExistingFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGetFileStatusThrowsExceptionForNonExistentFile() throws
                                                                   Exception {
    super.testGetFileStatusThrowsExceptionForNonExistentFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testListStatusThrowsExceptionForNonExistentFile() throws
                                                                Exception {
    super.testListStatusThrowsExceptionForNonExistentFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testListStatus() throws Exception {
    super.testListStatus();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testListStatusFilterWithNoMatches() throws Exception {
    super.testListStatusFilterWithNoMatches();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testListStatusFilterWithSomeMatches() throws Exception {
    super.testListStatusFilterWithSomeMatches();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusNonExistentFile() throws Exception {
    super.testGlobStatusNonExistentFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusWithNoMatchesInPath() throws Exception {
    super.testGlobStatusWithNoMatchesInPath();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusSomeMatchesInDirectories() throws Exception {
    super.testGlobStatusSomeMatchesInDirectories();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusWithMultipleWildCardMatches() throws Exception {
    super.testGlobStatusWithMultipleWildCardMatches();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusWithMultipleMatchesOfSingleChar() throws Exception {
    super.testGlobStatusWithMultipleMatchesOfSingleChar();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithEmptyPathResults() throws Exception {
    super.testGlobStatusFilterWithEmptyPathResults();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithSomePathMatchesAndTrivialFilter() throws
                                                                        Exception {
    super.testGlobStatusFilterWithSomePathMatchesAndTrivialFilter();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter() throws
                                                                                Exception {
    super.testGlobStatusFilterWithMultipleWildCardMatchesAndTrivialFilter();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter() throws
                                                                               Exception {
    super.testGlobStatusFilterWithMultiplePathMatchesAndNonTrivialFilter();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter() throws
                                                                           Exception {
    super.testGlobStatusFilterWithNoMatchingPathsAndNonTrivialFilter();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter() throws
                                                                                 Exception {
    super.testGlobStatusFilterWithMultiplePathWildcardsAndNonTrivialFilter();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    super.testWriteReadAndDeleteEmptyFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    super.testWriteReadAndDeleteHalfABlock();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteReadAndDeleteOneBlock() throws Exception {
    super.testWriteReadAndDeleteOneBlock();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    super.testWriteReadAndDeleteOneAndAHalfBlocks();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    super.testWriteReadAndDeleteTwoBlocks();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testOverwrite() throws IOException {
    super.testOverwrite();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testWriteInNonExistentDirectory() throws IOException {
    super.testWriteInNonExistentDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testDeleteNonExistentFile() throws IOException {
    super.testDeleteNonExistentFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testDeleteRecursively() throws IOException {
    super.testDeleteRecursively();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testDeleteEmptyDirectory() throws IOException {
    super.testDeleteEmptyDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameNonExistentPath() throws Exception {
    super.testRenameNonExistentPath();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileToNonExistentDirectory() throws Exception {
    super.testRenameFileToNonExistentDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileToDestinationWithParentFile() throws Exception {
    super.testRenameFileToDestinationWithParentFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileToExistingParent() throws Exception {
    super.testRenameFileToExistingParent();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileToItself() throws Exception {
    super.testRenameFileToItself();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileAsExistingFile() throws Exception {
    super.testRenameFileAsExistingFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameFileAsExistingDirectory() throws Exception {
    super.testRenameFileAsExistingDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryToItself() throws Exception {
    super.testRenameDirectoryToItself();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryToNonExistentParent() throws Exception {
    super.testRenameDirectoryToNonExistentParent();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryAsNonExistentDirectory() throws Exception {
    super.testRenameDirectoryAsNonExistentDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryAsEmptyDirectory() throws Exception {
    super.testRenameDirectoryAsEmptyDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryAsNonEmptyDirectory() throws Exception {
    super.testRenameDirectoryAsNonEmptyDirectory();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testRenameDirectoryAsFile() throws Exception {
    super.testRenameDirectoryAsFile();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testInputStreamClosedTwice() throws IOException {
    super.testInputStreamClosedTwice();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testOutputStreamClosedTwice() throws IOException {
    super.testOutputStreamClosedTwice();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testGetWrappedInputStream() throws IOException {
    super.testGetWrappedInputStream();
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  @Override
  public void testCopyToLocalWithUseRawLocalFileSystemOption() throws
                                                               Exception {
    super.testCopyToLocalWithUseRawLocalFileSystemOption();
  }
}
