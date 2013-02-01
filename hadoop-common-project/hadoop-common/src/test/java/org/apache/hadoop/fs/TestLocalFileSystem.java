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
import org.apache.hadoop.fs.FileSystem.Statistics;

import static org.apache.hadoop.fs.FileSystemTestHelper.*;

import java.io.*;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystem {
  private static String TEST_ROOT_DIR
    = System.getProperty("test.build.data","build/test/data/work-dir/localfs");

  private final File base = new File(TEST_ROOT_DIR);
  private Configuration conf;
  private LocalFileSystem fileSys;

  private void cleanupFile(FileSystem fs, Path name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name, true);
    assertTrue(!fs.exists(name));
  }
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    fileSys = FileSystem.getLocal(conf);
    fileSys.delete(new Path(TEST_ROOT_DIR), true);
  }
  
  @After
  public void after() throws IOException {
    base.setWritable(true);
    FileUtil.fullyDelete(base);
    assertTrue(!base.exists());
  }

  /**
   * Test the capability of setting the working directory.
   */
  @Test
  public void testWorkingDirectory() throws IOException {
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
      String contents = writeFile(fileSys, file1, 1);
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
      assertEquals(contents, new String(buffer, 0, bytesRead));
      stm.close();
    } finally {
      fileSys.setWorkingDirectory(origDir);
    }
  }

  /**
   * test Syncable interface on raw local file system
   * @throws IOException
   */
  @Test
  public void testSyncable() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf).getRawFileSystem();
    Path file = new Path(TEST_ROOT_DIR, "syncable");
    FSDataOutputStream out = fs.create(file);;
    final int bytesWritten = 1;
    byte[] expectedBuf = new byte[] {'0', '1', '2', '3'};
    try {
      out.write(expectedBuf, 0, 1);
      out.hflush();
      verifyFile(fs, file, bytesWritten, expectedBuf);
      out.write(expectedBuf, bytesWritten, expectedBuf.length-bytesWritten);
      out.hsync();
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
  
  @Test
  public void testCopy() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path src = new Path(TEST_ROOT_DIR, "dingo");
    Path dst = new Path(TEST_ROOT_DIR, "yak");
    writeFile(fs, src, 1);
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
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testHomeDirectory() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getLocal(conf);
    Path home = new Path(System.getProperty("user.home"))
      .makeQualified(fileSys);
    Path fsHome = fileSys.getHomeDirectory();
    assertEquals(home, fsHome);
  }

  @Test
  public void testPathEscapes() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path path = new Path(TEST_ROOT_DIR, "foo%bar");
    writeFile(fs, path, 1);
    FileStatus status = fs.getFileStatus(path);
    assertEquals(path.makeQualified(fs), status.getPath());
    cleanupFile(fs, path);
  }
  
  @Test
  public void testMkdirs() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path test_dir = new Path(TEST_ROOT_DIR, "test_dir");
    Path test_file = new Path(TEST_ROOT_DIR, "file1");
    assertTrue(fs.mkdirs(test_dir));
   
    writeFile(fs, test_file, 1);
    // creating dir over a file
    Path bad_dir = new Path(test_file, "another_dir");
    
    try {
      fs.mkdirs(bad_dir);
      fail("Failed to detect existing file in path");
    } catch (FileAlreadyExistsException e) { 
      // Expected
    }
    
    try {
      fs.mkdirs(null);
      fail("Failed to detect null in mkdir arg");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /** Test deleting a file, directory, and non-existent path */
  @Test
  public void testBasicDelete() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path dir1 = new Path(TEST_ROOT_DIR, "dir1");
    Path file1 = new Path(TEST_ROOT_DIR, "file1");
    Path file2 = new Path(TEST_ROOT_DIR+"/dir1", "file2");
    Path file3 = new Path(TEST_ROOT_DIR, "does-not-exist");
    assertTrue(fs.mkdirs(dir1));
    writeFile(fs, file1, 1);
    writeFile(fs, file2, 1);
    assertFalse("Returned true deleting non-existant path", 
        fs.delete(file3));
    assertTrue("Did not delete file", fs.delete(file1));
    assertTrue("Did not delete non-empty dir", fs.delete(dir1));
  }
  
  @Test
  public void testStatistics() throws Exception {
    FileSystem.getLocal(new Configuration());
    int fileSchemeCount = 0;
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals("file")) {
        fileSchemeCount++;
      }
    }
    assertEquals(1, fileSchemeCount);
  }

  @Test
  public void testListStatusWithColons() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    File colonFile = new File(TEST_ROOT_DIR, "foo:bar");
    colonFile.mkdirs();
    FileStatus[] stats = fs.listStatus(new Path(TEST_ROOT_DIR));
    assertEquals("Unexpected number of stats", 1, stats.length);
    assertEquals("Bad path from stat", colonFile.getAbsolutePath(),
        stats[0].getPath().toUri().getPath());
  }
  
  @Test
  public void testReportChecksumFailure() throws IOException {
    base.mkdirs();
    assertTrue(base.exists() && base.isDirectory());
    
    final File dir1 = new File(base, "dir1");
    final File dir2 = new File(dir1, "dir2");
    dir2.mkdirs();
    assertTrue(dir2.exists() && dir2.canWrite());
    
    final String dataFileName = "corruptedData";
    final Path dataPath = new Path(new File(dir2, dataFileName).toURI());
    final Path checksumPath = fileSys.getChecksumFile(dataPath);
    final FSDataOutputStream fsdos = fileSys.create(dataPath);
    try {
      fsdos.writeUTF("foo");
    } finally {
      fsdos.close();
    }
    assertTrue(fileSys.pathToFile(dataPath).exists());
    final long dataFileLength = fileSys.getFileStatus(dataPath).getLen();
    assertTrue(dataFileLength > 0);
    
    // check the the checksum file is created and not empty:
    assertTrue(fileSys.pathToFile(checksumPath).exists());
    final long checksumFileLength = fileSys.getFileStatus(checksumPath).getLen();
    assertTrue(checksumFileLength > 0);
    
    // this is a hack to force the #reportChecksumFailure() method to stop
    // climbing up at the 'base' directory and use 'dir1/bad_files' as the 
    // corrupted files storage:
    base.setWritable(false);
    
    FSDataInputStream dataFsdis = fileSys.open(dataPath);
    FSDataInputStream checksumFsdis = fileSys.open(checksumPath);
    
    boolean retryIsNecessary = fileSys.reportChecksumFailure(dataPath, dataFsdis, 0, checksumFsdis, 0);
    assertTrue(!retryIsNecessary);
    
    // the data file should be moved:
    assertTrue(!fileSys.pathToFile(dataPath).exists());
    // the checksum file should be moved:
    assertTrue(!fileSys.pathToFile(checksumPath).exists());
    
    // check that the files exist in the new location where they were moved:
    File[] dir1files = dir1.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname != null && !pathname.getName().equals("dir2");
      }
    });
    assertTrue(dir1files != null);
    assertTrue(dir1files.length == 1);
    File badFilesDir = dir1files[0];
    
    File[] badFiles = badFilesDir.listFiles();
    assertTrue(badFiles != null);
    assertTrue(badFiles.length == 2);
    boolean dataFileFound = false;
    boolean checksumFileFound = false;
    for (File badFile: badFiles) {
      if (badFile.getName().startsWith(dataFileName)) {
        assertTrue(dataFileLength == badFile.length());
        dataFileFound = true;
      } else if (badFile.getName().contains(dataFileName + ".crc")) {
        assertTrue(checksumFileLength == badFile.length());
        checksumFileFound = true;
      }
    }
    assertTrue(dataFileFound);
    assertTrue(checksumFileFound);
  }
  
}
