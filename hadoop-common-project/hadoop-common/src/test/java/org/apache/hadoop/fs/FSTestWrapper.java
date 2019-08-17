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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.test.GenericTestUtils;

/**
 * Abstraction of filesystem functionality with additional helper methods
 * commonly used in tests. This allows generic tests to be written which apply
 * to the two filesystem abstractions in Hadoop: {@link FileSystem} and
 * {@link FileContext}.
 */
public abstract class FSTestWrapper implements FSWrapper {

  //
  // Test helper methods taken from FileContextTestHelper
  //

  protected static final int DEFAULT_BLOCK_SIZE = 1024;
  protected static final int DEFAULT_NUM_BLOCKS = 2;

  protected String testRootDir = null;
  protected String absTestRootDir = null;

  public FSTestWrapper(String testRootDir) {
    // Use default test dir if not provided
    if (testRootDir == null || testRootDir.isEmpty()) {
      testRootDir = GenericTestUtils.getTestDir().getAbsolutePath();
    }
    // salt test dir with some random digits for safe parallel runs
    this.testRootDir = testRootDir + "/"
        + RandomStringUtils.randomAlphanumeric(10);
  }

  public static byte[] getFileData(int numOfBlocks, long blockSize) {
    byte[] data = new byte[(int) (numOfBlocks * blockSize)];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
    return data;
  }

  public Path getTestRootPath() {
    return makeQualified(new Path(testRootDir));
  }

  public Path getTestRootPath(String pathString) {
    return makeQualified(new Path(testRootDir, pathString));
  }

  // the getAbsolutexxx method is needed because the root test dir
  // can be messed up by changing the working dir.

  public String getAbsoluteTestRootDir() throws IOException {
    if (absTestRootDir == null) {
      Path testRootPath = new Path(testRootDir);
      if (testRootPath.isAbsolute()) {
        absTestRootDir = testRootDir;
      } else {
        absTestRootDir = getWorkingDirectory().toString() + "/"
            + testRootDir;
      }
    }
    return absTestRootDir;
  }

  public Path getAbsoluteTestRootPath() throws IOException {
    return makeQualified(new Path(getAbsoluteTestRootDir()));
  }

  abstract public FSTestWrapper getLocalFSWrapper()
      throws UnsupportedFileSystemException, IOException;

  abstract public Path getDefaultWorkingDirectory() throws IOException;

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  abstract public long createFile(Path path, int numBlocks,
      CreateOpts... options) throws IOException;

  abstract public long createFile(Path path, int numBlocks, int blockSize)
      throws IOException;

  abstract public long createFile(Path path) throws IOException;

  abstract public long createFile(String name) throws IOException;

  abstract public long createFileNonRecursive(String name) throws IOException;

  abstract public long createFileNonRecursive(Path path) throws IOException;

  abstract public void appendToFile(Path path, int numBlocks,
      CreateOpts... options) throws IOException;

  abstract public boolean exists(Path p) throws IOException;

  abstract public boolean isFile(Path p) throws IOException;

  abstract public boolean isDir(Path p) throws IOException;

  abstract public boolean isSymlink(Path p) throws IOException;

  abstract public void writeFile(Path path, byte b[]) throws IOException;

  abstract public byte[] readFile(Path path, int len) throws IOException;

  abstract public FileStatus containsPath(Path path, FileStatus[] dirList)
      throws IOException;

  abstract public FileStatus containsPath(String path, FileStatus[] dirList)
      throws IOException;

  enum fileType {
    isDir, isFile, isSymlink
  };

  abstract public void checkFileStatus(String path, fileType expectedType)
      throws IOException;

  abstract public void checkFileLinkStatus(String path, fileType expectedType)
      throws IOException;
}
