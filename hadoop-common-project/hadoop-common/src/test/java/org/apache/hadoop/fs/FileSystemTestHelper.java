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
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Random;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Helper class for unit tests.
 */
public final class FileSystemTestHelper {
  // The test root is relative to the <wd>/build/test/data by default
  public static String TEST_ROOT_DIR = 
    System.getProperty("test.build.data", "target/test/data") + "/test";
  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private static final int DEFAULT_NUM_BLOCKS = 2;
  private static final short DEFAULT_NUM_REPL = 1;
  private static String absTestRootDir = null;

  /** Hidden constructor */
  private FileSystemTestHelper() {}
  
  public static void addFileSystemForTesting(URI uri, Configuration conf,
      FileSystem fs) throws IOException {
    FileSystem.addFileSystemForTesting(uri, conf, fs);
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
  
  
  /*
   * get testRootPath qualified for fSys
   */
  public static Path getTestRootPath(FileSystem fSys) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR));
  }

  /*
   * get testRootPath + pathString qualified for fSys
   */
  public static Path getTestRootPath(FileSystem fSys, String pathString) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }
  
  
  // the getAbsolutexxx method is needed because the root test dir
  // can be messed up by changing the working dir since the TEST_ROOT_PATH
  // is often relative to the working directory of process
  // running the unit tests.

  static String getAbsoluteTestRootDir(FileSystem fSys)
      throws IOException {
    // NOTE: can't cache because of different filesystems!
    //if (absTestRootDir == null) 
      if (new Path(TEST_ROOT_DIR).isAbsolute()) {
        absTestRootDir = TEST_ROOT_DIR;
      } else {
        absTestRootDir = fSys.getWorkingDirectory().toString() + "/"
            + TEST_ROOT_DIR;
      }
    //}
    return absTestRootDir;
  }
  
  public static Path getAbsoluteTestRootPath(FileSystem fSys) throws IOException {
    return fSys.makeQualified(new Path(getAbsoluteTestRootDir(fSys)));
  }

  public static Path getDefaultWorkingDirectory(FileSystem fSys)
      throws IOException {
    return getTestRootPath(fSys, "/user/" + System.getProperty("user.name"))
        .makeQualified(fSys.getUri(),
            fSys.getWorkingDirectory());
  }

  /*
   * Create files with numBlocks blocks each with block size blockSize.
   */
  public static long createFile(FileSystem fSys, Path path, int numBlocks,
      int blockSize, short numRepl, boolean createParent) throws IOException {
    FSDataOutputStream out = 
      fSys.create(path, false, 4096, numRepl, blockSize );

    byte[] data = getFileData(numBlocks, blockSize);
    out.write(data, 0, data.length);
    out.close();
    return data.length;
  }


  public static long createFile(FileSystem fSys, Path path, int numBlocks,
      int blockSize, boolean createParent) throws IOException {
      return createFile(fSys, path, numBlocks, blockSize, fSys.getDefaultReplication(path), true);
  }

  public static long createFile(FileSystem fSys, Path path, int numBlocks,
      int blockSize) throws IOException {
      return createFile(fSys, path, numBlocks, blockSize, true);
  }

  public static long createFile(FileSystem fSys, Path path) throws IOException {
    return createFile(fSys, path, DEFAULT_NUM_BLOCKS, DEFAULT_BLOCK_SIZE, DEFAULT_NUM_REPL, true);
  }

  public static long createFile(FileSystem fSys, String name) throws IOException {
    Path path = getTestRootPath(fSys, name);
    return createFile(fSys, path);
  }

  public static boolean exists(FileSystem fSys, Path p) throws IOException {
    return fSys.exists(p);
  }
  
  public static boolean isFile(FileSystem fSys, Path p) throws IOException {
    try {
      return fSys.getFileStatus(p).isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  public static boolean isDir(FileSystem fSys, Path p) throws IOException {
    try {
      return fSys.getFileStatus(p).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  static String writeFile(FileSystem fileSys, Path name, int fileSize)
    throws IOException {
    final long seed = 0xDEADBEEFL;
    // Create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
    return new String(buffer);
  }
  
  static String readFile(FileSystem fs, Path name, int buflen) 
    throws IOException {
    byte[] b = new byte[buflen];
    int offset = 0;
    FSDataInputStream in = fs.open(name);
    for (int remaining, n;
        (remaining = b.length - offset) > 0 && (n = in.read(b, offset, remaining)) != -1;
        offset += n); 
    assertEquals(offset, Math.min(b.length, in.getPos()));
    in.close();
    String s = new String(b, 0, offset);
    return s;
  }

  public static FileStatus containsPath(FileSystem fSys, Path path,
      FileStatus[] dirList)
    throws IOException {
    for(int i = 0; i < dirList.length; i ++) { 
      if (getTestRootPath(fSys, path.toString()).equals(
          dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }
  
  public static FileStatus containsPath(Path path,
      FileStatus[] dirList)
    throws IOException {
    for(int i = 0; i < dirList.length; i ++) { 
      if (path.equals(dirList[i].getPath()))
        return dirList[i];
      }
    return null;
  }
  
  
  public static FileStatus containsPath(FileSystem fSys, String path, FileStatus[] dirList)
     throws IOException {
    return containsPath(fSys, new Path(path), dirList);
  }
  
  public static enum fileType {isDir, isFile, isSymlink};
  public static void checkFileStatus(FileSystem aFs, String path,
      fileType expectedType) throws IOException {
    FileStatus s = aFs.getFileStatus(new Path(path));
    Assert.assertNotNull(s);
    if (expectedType == fileType.isDir) {
      Assert.assertTrue(s.isDirectory());
    } else if (expectedType == fileType.isFile) {
      Assert.assertTrue(s.isFile());
    } else if (expectedType == fileType.isSymlink) {
      Assert.assertTrue(s.isSymlink());
    }
    Assert.assertEquals(aFs.makeQualified(new Path(path)), s.getPath());
  }
  
  /**
   * Class to enable easier mocking of a FileSystem
   * Use getRawFileSystem to retrieve the mock
   */
  public static class MockFileSystem extends FilterFileSystem {
    public MockFileSystem() {
      // it's a bit ackward to mock ourselves, but it allows the visibility
      // of methods to be increased
      super(mock(MockFileSystem.class));
    }
    @Override
    public MockFileSystem getRawFileSystem() {
      return (MockFileSystem) super.getRawFileSystem();
      
    }
    // these basic methods need to directly propagate to the mock to be
    // more transparent
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      fs.initialize(uri, conf);
    }
    @Override
    public String getCanonicalServiceName() {
      return fs.getCanonicalServiceName();
    }
    @Override
    public FileSystem[] getChildFileSystems() {
      return fs.getChildFileSystems();
    }
    @Override // publicly expose for mocking
    public Token<?> getDelegationToken(String renewer) throws IOException {
      return fs.getDelegationToken(renewer);
    }    
  }
}
