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
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.CreateOpts.BlockSize;

/**
 * Helper class for unit tests.
 */
public final class FileContextTestHelper {
  private static final String TEST_ROOT_DIR = System.getProperty("test.build.data",
      "build/test/data") + "/test";
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private static final int DEFAULT_NUM_BLOCKS = 2;
  private static String absTestRootDir = null;

  /** Hidden constructor */
  private FileContextTestHelper() {}
  
  public static int getDefaultBlockSize() {
    return DEFAULT_BLOCK_SIZE;
  }
  
  public static byte[] getFileData(int numOfBlocks, long blockSize) {
    byte[] data = new byte[(int) (numOfBlocks * blockSize)];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
    return data;
  }
  
  public static Path getTestRootPath(FileContext fc) {
    return fc.makeQualified(new Path(TEST_ROOT_DIR));
  }

  public static Path getTestRootPath(FileContext fc, String pathString) {
    return fc.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }
  
  public static String getAbsoluteTestRootDir(FileContext fc)
      throws IOException {
    if (absTestRootDir == null) {
      if (TEST_ROOT_DIR.startsWith("/")) {
        absTestRootDir = TEST_ROOT_DIR;
      } else {
        absTestRootDir = getDefaultWorkingDirectory(fc).toString() + "/"
            + TEST_ROOT_DIR;
      }
    }
    return absTestRootDir;
  }
  
  public static Path getTestRootDir(FileContext fc) throws IOException {
    return fc.makeQualified(new Path(getAbsoluteTestRootDir(fc)));
  }

  public static Path getDefaultWorkingDirectory(FileContext fc)
      throws IOException {
    return getTestRootPath(fc, "/user/" + System.getProperty("user.name"))
        .makeQualified(fc.getDefaultFileSystem().getUri(),
            fc.getWorkingDirectory());
  }

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  public static void createFile(FileContext fc, Path path, int numBlocks,
      CreateOpts... options) throws IOException {
    BlockSize blockSizeOpt = 
      (BlockSize) CreateOpts.getOpt(CreateOpts.BlockSize.class, options);
    long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue()
        : DEFAULT_BLOCK_SIZE;
    FSDataOutputStream out = 
      fc.create(path, EnumSet.of(CreateFlag.CREATE), options);
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  public static void createFile(FileContext fc, Path path, int numBlocks,
      int blockSize) throws IOException {
    createFile(fc, path, numBlocks, CreateOpts.blockSize(blockSize), 
        CreateOpts.createParent());
  }

  public static void createFile(FileContext fc, Path path) throws IOException {
    createFile(fc, path, DEFAULT_NUM_BLOCKS, CreateOpts.createParent());
  }

  public static Path createFile(FileContext fc, String name) throws IOException {
    Path path = getTestRootPath(fc, name);
    createFile(fc, path);
    return path;
  }
  
  public static void createFileNonRecursive(FileContext fc, Path path)
      throws IOException {
    createFile(fc, path, DEFAULT_NUM_BLOCKS, CreateOpts.donotCreateParent());
  } 
}
