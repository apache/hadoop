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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.CreateOpts.BlockSize;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;

/**
 * Helper class for unit tests.
 */
public final class FileContextTestHelper {
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private static final int DEFAULT_NUM_BLOCKS = 2;

  private final String testRootDir;
  private String absTestRootDir = null;

  /**
   * Create a context with test root relative to the test directory
   */
  public FileContextTestHelper() {
    this(GenericTestUtils.getRandomizedTestDir().getPath());
  }

  /**
   * Create a context with the given test root
   */
  public FileContextTestHelper(String testRootDir) {
    this.testRootDir = testRootDir;
  }
  
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
  
  public Path getTestRootPath(FileContext fc) {
    return fc.makeQualified(new Path(testRootDir));
  }

  public Path getTestRootPath(FileContext fc, String pathString) {
    return fc.makeQualified(new Path(testRootDir, pathString));
  }
  
  
  // the getAbsolutexxx method is needed because the root test dir
  // can be messed up by changing the working dir.

  public String getAbsoluteTestRootDir(FileContext fc) {
    if (absTestRootDir == null) {
      if (new Path(testRootDir).isAbsolute()) {
        absTestRootDir = testRootDir;
      } else {
        absTestRootDir = fc.getWorkingDirectory().toString() + "/"
            + new Path(testRootDir).toUri();
      }
    }
    return absTestRootDir;
  }
  
  public Path getAbsoluteTestRootPath(FileContext fc) {
    return fc.makeQualified(new Path(getAbsoluteTestRootDir(fc)));
  }

  public Path getDefaultWorkingDirectory(FileContext fc) {
    return getTestRootPath(fc, "/user/" + System.getProperty("user.name"))
        .makeQualified(fc.getDefaultFileSystem().getUri(),
            fc.getWorkingDirectory());
  }

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  public static long createFile(FileContext fc, Path path, int numBlocks,
      CreateOpts... options) throws IOException {
    BlockSize blockSizeOpt = CreateOpts.getOpt(CreateOpts.BlockSize.class, options);
    long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue()
        : DEFAULT_BLOCK_SIZE;
    FSDataOutputStream out = 
      fc.create(path, EnumSet.of(CreateFlag.CREATE), options);
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
    return data.length;
  }

  public static long  createFile(FileContext fc, Path path, int numBlocks,
      int blockSize) throws IOException {
    return createFile(fc, path, numBlocks, CreateOpts.blockSize(blockSize), 
        CreateOpts.createParent());
  }

  public static long createFile(FileContext fc, Path path) throws IOException {
    return createFile(fc, path, DEFAULT_NUM_BLOCKS, CreateOpts.createParent());
  }

  public long createFile(FileContext fc, String name) throws IOException {
    Path path = getTestRootPath(fc, name);
    return createFile(fc, path);
  }
  
  public long createFileNonRecursive(FileContext fc, String name)
  throws IOException {
    Path path = getTestRootPath(fc, name);
    return createFileNonRecursive(fc, path);
  }

  public static long createFileNonRecursive(FileContext fc, Path path)
      throws IOException {
    return createFile(fc, path, DEFAULT_NUM_BLOCKS, CreateOpts.donotCreateParent());
  }

  public static void appendToFile(FileContext fc, Path path, int numBlocks,
      CreateOpts... options) throws IOException {
    BlockSize blockSizeOpt = CreateOpts.getOpt(CreateOpts.BlockSize.class, options);
    long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue()
        : DEFAULT_BLOCK_SIZE;
    FSDataOutputStream out;
    out = fc.create(path, EnumSet.of(CreateFlag.APPEND));
    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  public static boolean exists(FileContext fc, Path p) throws IOException {
    return fc.util().exists(p);
  }
  
  public static boolean isFile(FileContext fc, Path p) throws IOException {
    try {
      return fc.getFileStatus(p).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public static boolean isDir(FileContext fc, Path p) throws IOException {
    try {
      return fc.getFileStatus(p).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  public static boolean isSymlink(FileContext fc, Path p) throws IOException {
    try {
      return fc.getFileLinkStatus(p).isSymlink();
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  public static void writeFile(FileContext fc, Path path, byte b[])
      throws IOException {
    FSDataOutputStream out = 
      fc.create(path,EnumSet.of(CreateFlag.CREATE), CreateOpts.createParent());
    out.write(b);
    out.close();
  }
  
  public static byte[] readFile(FileContext fc, Path path, int len)
      throws IOException {
    DataInputStream dis = fc.open(path);
    byte[] buffer = new byte[len];
    IOUtils.readFully(dis, buffer, 0, len);
    dis.close();
    return buffer;
  }

  public FileStatus containsPath(FileContext fc, Path path,
      FileStatus[] dirList) {
    return containsPath(getTestRootPath(fc, path.toString()), dirList);
  }
  
  public static FileStatus containsPath(Path path, FileStatus[] dirList) {
    for(int i = 0; i < dirList.length; i ++) { 
      if (path.equals(dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }
  
  public FileStatus containsPath(FileContext fc, String path,
      FileStatus[] dirList) {
    return containsPath(fc, new Path(path), dirList);
  }
  
  public enum fileType {isDir, isFile, isSymlink};
  
  public static void checkFileStatus(FileContext aFc, String path,
      fileType expectedType) throws IOException {
    FileStatus s = aFc.getFileStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(aFc.makeQualified(new Path(path)), s.getPath());
  }
  
  public static void checkFileLinkStatus(FileContext aFc, String path,
      fileType expectedType) throws IOException {
    FileStatus s = aFc.getFileLinkStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(aFc.makeQualified(new Path(path)), s.getPath());
  }
}
