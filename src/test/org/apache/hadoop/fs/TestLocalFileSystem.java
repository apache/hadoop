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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import java.io.*;
import java.util.Arrays;
import java.util.Random;

import junit.framework.*;

/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystem extends TestCase {
  private static String TEST_ROOT_DIR
    = System.getProperty("test.build.data","build/test/data/work-dir/localfs");
  private final Path TEST_PATH = new Path(TEST_ROOT_DIR, "test-file");


  static void writeFile(FileSystem fs, Path name) throws IOException {
    FSDataOutputStream stm = fs.create(name);
    stm.writeBytes("42\n");
    stm.close();
  }
  
  static String readFile(FileSystem fs, Path name) throws IOException {
    byte[] b = new byte[1024];
    int offset = 0;
    FSDataInputStream in = fs.open(name);
    for(int remaining, n;
        (remaining = b.length - offset) > 0 && (n = in.read(b, offset, remaining)) != -1;
        offset += n); 
    in.close();

    String s = new String(b, 0, offset);
    System.out.println("s=" + s);
    return s;
  }

  private void cleanupFile(FileSystem fs, Path name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name, true);
    assertTrue(!fs.exists(name));
  }
  
  /**
   * Test the capability of setting the working directory.
   */
  public void testWorkingDirectory() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getLocal(conf);
    Path origDir = fileSys.getWorkingDirectory();
    Path subdir = new Path(TEST_ROOT_DIR, "new");
    try {
      // make sure it doesn't already exist
      assertTrue(!fileSys.exists(subdir));
      // make it and check for it
      assertTrue(fileSys.mkdirs(subdir));
      assertTrue(fileSys.isDirectory(subdir));
      
      fileSys.setWorkingDirectory(subdir);
      
      // create a directory and check for it
      Path dir1 = new Path("dir1");
      assertTrue(fileSys.mkdirs(dir1));
      assertTrue(fileSys.isDirectory(dir1));
      
      // delete the directory and make sure it went away
      fileSys.delete(dir1, true);
      assertTrue(!fileSys.exists(dir1));
      
      // create files and manipulate them.
      Path file1 = new Path("file1");
      Path file2 = new Path("sub/file2");
      writeFile(fileSys, file1);
      fileSys.copyFromLocalFile(file1, file2);
      assertTrue(fileSys.exists(file1));
      assertTrue(fileSys.isFile(file1));
      cleanupFile(fileSys, file2);
      fileSys.copyToLocalFile(file1, file2);
      cleanupFile(fileSys, file2);
      
      // try a rename
      fileSys.rename(file1, file2);
      assertTrue(!fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      fileSys.rename(file2, file1);
      
      // try reading a file
      InputStream stm = fileSys.open(file1);
      byte[] buffer = new byte[3];
      int bytesRead = stm.read(buffer, 0, 3);
      assertEquals("42\n", new String(buffer, 0, bytesRead));
      stm.close();
    } finally {
      fileSys.setWorkingDirectory(origDir);
      fileSys.delete(subdir, true);
    }
  }

  public void testSyncable() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf).getRawFileSystem();
    Path file = new Path(TEST_ROOT_DIR, "syncable");
    FSDataOutputStream out = fs.create(file);;
    final int bytesWritten = 1;
    byte[] expectedBuf = new byte[] {'0', '1', '2', '3'};
    try {
      out.write(expectedBuf, 0, 1); 
      out.sync();
      verifyFile(fs, file, bytesWritten, expectedBuf);
      out.write(expectedBuf, bytesWritten, expectedBuf.length-bytesWritten);
      out.sync();
      verifyFile(fs, file, expectedBuf.length, expectedBuf);
    } finally {
      out.close();
    }   
  }
  
  private void verifyFile(FileSystem fs, Path file, int bytesToVerify, 
      byte[] expectedBytes) throws IOException {
    FSDataInputStream in = fs.open(file);
    try {
      byte[] readBuf = new byte[bytesToVerify];
      in.readFully(readBuf, 0, bytesToVerify);
      for (int i=0; i<bytesToVerify; i++) {
        assertEquals(expectedBytes[i], readBuf[i]);
      }   
    } finally {
      in.close();
    }   
  }

  public void testCopy() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path src = new Path(TEST_ROOT_DIR, "dingo");
    Path dst = new Path(TEST_ROOT_DIR, "yak");
    writeFile(fs, src);
    assertTrue(FileUtil.copy(fs, src, fs, dst, true, false, conf));
    assertTrue(!fs.exists(src) && fs.exists(dst));
    assertTrue(FileUtil.copy(fs, dst, fs, src, false, false, conf));
    assertTrue(fs.exists(src) && fs.exists(dst));
    assertTrue(FileUtil.copy(fs, src, fs, dst, true, true, conf));
    assertTrue(!fs.exists(src) && fs.exists(dst));
    fs.mkdirs(src);
    assertTrue(FileUtil.copy(fs, dst, fs, src, false, false, conf));
    Path tmp = new Path(src, dst.getName());
    assertTrue(fs.exists(tmp) && fs.exists(dst));
    assertTrue(FileUtil.copy(fs, dst, fs, src, false, true, conf));
    assertTrue(fs.delete(tmp, true));
    fs.mkdirs(tmp);
    try {
      FileUtil.copy(fs, dst, fs, src, true, true, conf);
      fail("Failed to detect existing dir");
    } catch (IOException e) { }
  }

  public void testHomeDirectory() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getLocal(conf);
    Path home = new Path(System.getProperty("user.home"))
      .makeQualified(fileSys);
    Path fsHome = fileSys.getHomeDirectory();
    assertEquals(home, fsHome);
  }

  public void testPathEscapes() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path path = new Path(TEST_ROOT_DIR, "foo%bar");
    writeFile(fs, path);
    FileStatus status = fs.getFileStatus(path);
    assertEquals(path.makeQualified(fs), status.getPath());
    cleanupFile(fs, path);
  }
  
  public void testGetCanonicalServiceName() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    assertNull(fs.getCanonicalServiceName());
  }

  public void testHasFileDescriptor() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path path = new Path(TEST_ROOT_DIR, "test-file");
    writeFile(fs, path);
    BufferedFSInputStream bis = new BufferedFSInputStream(
        new RawLocalFileSystem().new LocalFSFileInputStream(path), 1024);
    assertNotNull(bis.getFileDescriptor());
  }

  /**
   * Regression test for HADOOP-9307: BufferedFSInputStream returning
   * wrong results after certain sequences of seeks and reads.
   */
  public void testBufferedFSInputStream() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
    conf.set("fs.file.impl.disable.cache", "true");
    conf.setInt("io.file.buffer.size", 4096);
    FileSystem fs = FileSystem.get(conf);
    
    byte[] buf = new byte[10*1024];
    new Random().nextBytes(buf);
    
    // Write random bytes to file
    FSDataOutputStream stream = fs.create(TEST_PATH);
    try {
      stream.write(buf);
    } finally {
      stream.close();
    }
    
    Random r = new Random();

    FSDataInputStream stm = fs.open(TEST_PATH);
    // Record the sequence of seeks and reads which trigger a failure.
    int seeks[] = new int[10];
    int reads[] = new int[10];
    try {
      for (int i = 0; i < 1000; i++) {
        int seekOff = r.nextInt(buf.length); 
        int toRead = r.nextInt(Math.min(buf.length - seekOff, 32000));
        
        seeks[i % seeks.length] = seekOff;
        reads[i % reads.length] = toRead;
        verifyRead(stm, buf, seekOff, toRead);
        
      }
    } catch (AssertionError afe) {
      StringBuilder sb = new StringBuilder();
      sb.append("Sequence of actions:\n");
      for (int j = 0; j < seeks.length; j++) {
        sb.append("seek @ ").append(seeks[j]).append("  ")
          .append("read ").append(reads[j]).append("\n");
      }
      System.err.println(sb.toString());
      throw afe;
    } finally {
      stm.close();
    }
  }

  /**
   * Tests a simple rename of a directory.
   */
  public void testRenameDirectory() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path src = new Path(TEST_ROOT_DIR, "dir1");
    Path dst = new Path(TEST_ROOT_DIR, "dir2");
    try {
      fs.delete(src, true);
      fs.delete(dst, true);
      assertTrue(fs.mkdirs(src));
      assertTrue(fs.rename(src, dst));
      assertTrue(fs.exists(dst));
      assertFalse(fs.exists(src));
    } finally {
      cleanupFileSystem(fs, src, dst);
    }
  }

  /**
   * Tests that renaming a directory replaces the destination if the destination
   * is an existing empty directory.
   * 
   * Before:
   *   /dir1
   *     /file1
   *     /file2
   *   /dir2
   * 
   * After rename("/dir1", "/dir2"):
   *   /dir2
   *     /file1
   *     /file2
   */
  public void testRenameReplaceExistingEmptyDirectory() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path src = new Path(TEST_ROOT_DIR, "dir1");
    Path dst = new Path(TEST_ROOT_DIR, "dir2");
    try {
      fs.delete(src, true);
      fs.delete(dst, true);
      assertTrue(fs.mkdirs(src));
      writeFile(fs, new Path(src, "file1"));
      writeFile(fs, new Path(src, "file2"));
      assertTrue(fs.mkdirs(dst));
      assertTrue(fs.rename(src, dst));
      assertTrue(fs.exists(dst));
      assertTrue(fs.exists(new Path(dst, "file1")));
      assertTrue(fs.exists(new Path(dst, "file2")));
      assertFalse(fs.exists(src));
    } finally {
      cleanupFileSystem(fs, src, dst);
    }
  }

  /**
   * Tests that renaming a directory to an existing directory that is not empty
   * results in a full copy of source to destination.
   * 
   * Before:
   *   /dir1
   *     /dir2
   *       /dir3
   *         /file1
   *         /file2
   * 
   * After rename("/dir1/dir2/dir3", "/dir1"):
   *   /dir1
   *     /dir3
   *       /file1
   *       /file2
   */
  public void testRenameMoveToExistingNonEmptyDirectory() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path src = new Path(TEST_ROOT_DIR, "dir1/dir2/dir3");
    Path dst = new Path(TEST_ROOT_DIR, "dir1");
    try {
      fs.delete(src, true);
      fs.delete(dst, true);
      assertTrue(fs.mkdirs(src));
      writeFile(fs, new Path(src, "file1"));
      writeFile(fs, new Path(src, "file2"));
      assertTrue(fs.exists(dst));
      assertTrue(fs.rename(src, dst));
      assertTrue(fs.exists(dst));
      assertTrue(fs.exists(new Path(dst, "dir3")));
      assertTrue(fs.exists(new Path(dst, "dir3/file1")));
      assertTrue(fs.exists(new Path(dst, "dir3/file2")));
      assertFalse(fs.exists(src));
    } finally {
      cleanupFileSystem(fs, src, dst);
    }
  }
  
  private void verifyRead(FSDataInputStream stm, byte[] fileContents,
       int seekOff, int toRead) throws IOException {
    byte[] out = new byte[toRead];
    stm.seek(seekOff);
    stm.readFully(out);
    byte[] expected = Arrays.copyOfRange(fileContents, seekOff, seekOff+toRead);
    if (!Arrays.equals(out, expected)) {
      String s ="\nExpected: " +
          StringUtils.byteToHexString(expected) +
          "\ngot:      " +
          StringUtils.byteToHexString(out) + 
          "\noff=" + seekOff + " len=" + toRead;
      fail(s);
    }
  }

  /**
   * Cleans up the file system by deleting the given paths and closing the file
   * system.
   * 
   * @param fs FileSystem to clean up
   * @param paths Path... any number of paths to delete
   */
  private static void cleanupFileSystem(FileSystem fs, Path... paths) {
    for (Path path: paths) {
      deleteQuietly(fs, path);
    }
    IOUtils.cleanup(null, fs);
  }

  /**
   * Deletes the given path and silences any exceptions.
   * 
   * @param fs FileSystem to perform the delete
   * @param path Path to delete
   */
  private static void deleteQuietly(FileSystem fs, Path path) {
    try {
      fs.delete(path, true);
    } catch (IOException e) {
    }
  }
}
