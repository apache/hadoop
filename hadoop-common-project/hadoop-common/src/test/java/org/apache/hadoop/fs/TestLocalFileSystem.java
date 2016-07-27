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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.FileSystemTestHelper.*;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;


/**
 * This class tests the local file system via the FileSystem abstraction.
 */
public class TestLocalFileSystem {
  private static final File base =
      GenericTestUtils.getTestDir("work-dir/localfs");

  private static final String TEST_ROOT_DIR = base.getAbsolutePath();
  private final Path TEST_PATH = new Path(TEST_ROOT_DIR, "test-file");
  private Configuration conf;
  private LocalFileSystem fileSys;

  private void cleanupFile(FileSystem fs, Path name) throws IOException {
    assertTrue(fs.exists(name));
    fs.delete(name, true);
    assertTrue(!fs.exists(name));
  }
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration(false);
    conf.set("fs.file.impl", LocalFileSystem.class.getName());
    fileSys = FileSystem.getLocal(conf);
    fileSys.delete(new Path(TEST_ROOT_DIR), true);
  }
  
  @After
  public void after() throws IOException {
    FileUtil.setWritable(base, true);
    FileUtil.fullyDelete(base);
    assertTrue(!base.exists());
    RawLocalFileSystem.useStatIfAvailable();
  }

  /**
   * Test the capability of setting the working directory.
   */
  @Test(timeout = 10000)
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
  @Test(timeout = 1000)
  public void testSyncable() throws IOException {
    FileSystem fs = fileSys.getRawFileSystem();
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
  
  @Test(timeout = 10000)
  public void testCopy() throws IOException {
    Path src = new Path(TEST_ROOT_DIR, "dingo");
    Path dst = new Path(TEST_ROOT_DIR, "yak");
    writeFile(fileSys, src, 1);
    assertTrue(FileUtil.copy(fileSys, src, fileSys, dst, true, false, conf));
    assertTrue(!fileSys.exists(src) && fileSys.exists(dst));
    assertTrue(FileUtil.copy(fileSys, dst, fileSys, src, false, false, conf));
    assertTrue(fileSys.exists(src) && fileSys.exists(dst));
    assertTrue(FileUtil.copy(fileSys, src, fileSys, dst, true, true, conf));
    assertTrue(!fileSys.exists(src) && fileSys.exists(dst));
    fileSys.mkdirs(src);
    assertTrue(FileUtil.copy(fileSys, dst, fileSys, src, false, false, conf));
    Path tmp = new Path(src, dst.getName());
    assertTrue(fileSys.exists(tmp) && fileSys.exists(dst));
    assertTrue(FileUtil.copy(fileSys, dst, fileSys, src, false, true, conf));
    assertTrue(fileSys.delete(tmp, true));
    fileSys.mkdirs(tmp);
    try {
      FileUtil.copy(fileSys, dst, fileSys, src, true, true, conf);
      fail("Failed to detect existing dir");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test(timeout = 1000)
  public void testHomeDirectory() throws IOException {
    Path home = new Path(System.getProperty("user.home"))
      .makeQualified(fileSys);
    Path fsHome = fileSys.getHomeDirectory();
    assertEquals(home, fsHome);
  }

  @Test(timeout = 1000)
  public void testPathEscapes() throws IOException {
    Path path = new Path(TEST_ROOT_DIR, "foo%bar");
    writeFile(fileSys, path, 1);
    FileStatus status = fileSys.getFileStatus(path);
    assertEquals(path.makeQualified(fileSys), status.getPath());
    cleanupFile(fileSys, path);
  }
  
  @Test(timeout = 1000)
  public void testCreateFileAndMkdirs() throws IOException {
    Path test_dir = new Path(TEST_ROOT_DIR, "test_dir");
    Path test_file = new Path(test_dir, "file1");
    assertTrue(fileSys.mkdirs(test_dir));
   
    final int fileSize = new Random().nextInt(1 << 20) + 1;
    writeFile(fileSys, test_file, fileSize);

    {
      //check FileStatus and ContentSummary 
      final FileStatus status = fileSys.getFileStatus(test_file);
      Assert.assertEquals(fileSize, status.getLen());
      final ContentSummary summary = fileSys.getContentSummary(test_dir);
      Assert.assertEquals(fileSize, summary.getLength());
    }
    
    // creating dir over a file
    Path bad_dir = new Path(test_file, "another_dir");
    
    try {
      fileSys.mkdirs(bad_dir);
      fail("Failed to detect existing file in path");
    } catch (ParentNotDirectoryException e) {
      // Expected
    }
    
    try {
        fileSys.mkdirs(null);
      fail("Failed to detect null in mkdir arg");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  /** Test deleting a file, directory, and non-existent path */
  @Test(timeout = 1000)
  public void testBasicDelete() throws IOException {
    Path dir1 = new Path(TEST_ROOT_DIR, "dir1");
    Path file1 = new Path(TEST_ROOT_DIR, "file1");
    Path file2 = new Path(TEST_ROOT_DIR+"/dir1", "file2");
    Path file3 = new Path(TEST_ROOT_DIR, "does-not-exist");
    assertTrue(fileSys.mkdirs(dir1));
    writeFile(fileSys, file1, 1);
    writeFile(fileSys, file2, 1);
    assertFalse("Returned true deleting non-existant path", 
            fileSys.delete(file3));
    assertTrue("Did not delete file", fileSys.delete(file1));
    assertTrue("Did not delete non-empty dir", fileSys.delete(dir1));
  }
  
  @Test(timeout = 1000)
  public void testStatistics() throws Exception {
    int fileSchemeCount = 0;
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals("file")) {
        fileSchemeCount++;
      }
    }
    assertEquals(1, fileSchemeCount);
  }

  @Test(timeout = 1000)
  public void testHasFileDescriptor() throws IOException {
    Path path = new Path(TEST_ROOT_DIR, "test-file");
    writeFile(fileSys, path, 1);
    BufferedFSInputStream bis = null;
    try {
      bis = new BufferedFSInputStream(new RawLocalFileSystem()
        .new LocalFSFileInputStream(path), 1024);
      assertNotNull(bis.getFileDescriptor());
    } finally {
      IOUtils.cleanup(null, bis);
    }
  }

  @Test(timeout = 1000)
  public void testListStatusWithColons() throws IOException {
    assumeNotWindows();
    File colonFile = new File(TEST_ROOT_DIR, "foo:bar");
    colonFile.mkdirs();
    FileStatus[] stats = fileSys.listStatus(new Path(TEST_ROOT_DIR));
    assertEquals("Unexpected number of stats", 1, stats.length);
    assertEquals("Bad path from stat", colonFile.getAbsolutePath(),
        stats[0].getPath().toUri().getPath());
  }
  
  @Test
  public void testListStatusReturnConsistentPathOnWindows() throws IOException {
    assumeWindows();
    String dirNoDriveSpec = TEST_ROOT_DIR;
    if (dirNoDriveSpec.charAt(1) == ':')
    	dirNoDriveSpec = dirNoDriveSpec.substring(2);
    
    File file = new File(dirNoDriveSpec, "foo");
    file.mkdirs();
    FileStatus[] stats = fileSys.listStatus(new Path(dirNoDriveSpec));
    assertEquals("Unexpected number of stats", 1, stats.length);
    assertEquals("Bad path from stat", new Path(file.getPath()).toUri().getPath(),
        stats[0].getPath().toUri().getPath());
  }
  
  @Test(timeout = 10000)
  public void testReportChecksumFailure() throws IOException {
    base.mkdirs();
    assertTrue(base.exists() && base.isDirectory());
    
    final File dir1 = new File(base, "dir1");
    final File dir2 = new File(dir1, "dir2");
    dir2.mkdirs();
    assertTrue(dir2.exists() && FileUtil.canWrite(dir2));
    
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
    FileUtil.setWritable(base, false);
    
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

  private void checkTimesStatus(Path path,
    long expectedModTime, long expectedAccTime) throws IOException {
    FileStatus status = fileSys.getFileStatus(path);
    assertEquals(expectedModTime, status.getModificationTime());
    assertEquals(expectedAccTime, status.getAccessTime());
  }

  @Test(timeout = 1000)
  public void testSetTimes() throws Exception {
    Path path = new Path(TEST_ROOT_DIR, "set-times");
    writeFile(fileSys, path, 1);

    // test only to the nearest second, as the raw FS may not
    // support millisecond timestamps
    long newModTime = 12345000;
    long newAccTime = 23456000;

    FileStatus status = fileSys.getFileStatus(path);
    assertTrue("check we're actually changing something", newModTime != status.getModificationTime());
    assertTrue("check we're actually changing something", newAccTime != status.getAccessTime());

    fileSys.setTimes(path, newModTime, newAccTime);
    checkTimesStatus(path, newModTime, newAccTime);

    newModTime = 34567000;

    fileSys.setTimes(path, newModTime, -1);
    checkTimesStatus(path, newModTime, newAccTime);

    newAccTime = 45678000;

    fileSys.setTimes(path, -1, newAccTime);
    checkTimesStatus(path, newModTime, newAccTime);
  }

  /**
   * Regression test for HADOOP-9307: BufferedFSInputStream returning
   * wrong results after certain sequences of seeks and reads.
   */
  @Test
  public void testBufferedFSInputStream() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
    conf.setInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, 4096);
    FileSystem fs = FileSystem.newInstance(conf);
    
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
  @Test
  public void testRenameDirectory() throws IOException {
    Path src = new Path(TEST_ROOT_DIR, "dir1");
    Path dst = new Path(TEST_ROOT_DIR, "dir2");
    fileSys.delete(src, true);
    fileSys.delete(dst, true);
    assertTrue(fileSys.mkdirs(src));
    assertTrue(fileSys.rename(src, dst));
    assertTrue(fileSys.exists(dst));
    assertFalse(fileSys.exists(src));
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
  @Test
  public void testRenameReplaceExistingEmptyDirectory() throws IOException {
    Path src = new Path(TEST_ROOT_DIR, "dir1");
    Path dst = new Path(TEST_ROOT_DIR, "dir2");
    fileSys.delete(src, true);
    fileSys.delete(dst, true);
    assertTrue(fileSys.mkdirs(src));
    writeFile(fileSys, new Path(src, "file1"), 1);
    writeFile(fileSys, new Path(src, "file2"), 1);
    assertTrue(fileSys.mkdirs(dst));
    assertTrue(fileSys.rename(src, dst));
    assertTrue(fileSys.exists(dst));
    assertTrue(fileSys.exists(new Path(dst, "file1")));
    assertTrue(fileSys.exists(new Path(dst, "file2")));
    assertFalse(fileSys.exists(src));
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
  @Test
  public void testRenameMoveToExistingNonEmptyDirectory() throws IOException {
    Path src = new Path(TEST_ROOT_DIR, "dir1/dir2/dir3");
    Path dst = new Path(TEST_ROOT_DIR, "dir1");
    fileSys.delete(src, true);
    fileSys.delete(dst, true);
    assertTrue(fileSys.mkdirs(src));
    writeFile(fileSys, new Path(src, "file1"), 1);
    writeFile(fileSys, new Path(src, "file2"), 1);
    assertTrue(fileSys.exists(dst));
    assertTrue(fileSys.rename(src, dst));
    assertTrue(fileSys.exists(dst));
    assertTrue(fileSys.exists(new Path(dst, "dir3")));
    assertTrue(fileSys.exists(new Path(dst, "dir3/file1")));
    assertTrue(fileSys.exists(new Path(dst, "dir3/file2")));
    assertFalse(fileSys.exists(src));
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

  @Test
  public void testStripFragmentFromPath() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path pathQualified = TEST_PATH.makeQualified(fs.getUri(),
        fs.getWorkingDirectory());
    Path pathWithFragment = new Path(
        new URI(pathQualified.toString() + "#glacier"));
    // Create test file with fragment
    FileSystemTestHelper.createFile(fs, pathWithFragment);
    Path resolved = fs.resolvePath(pathWithFragment);
    assertEquals("resolvePath did not strip fragment from Path", pathQualified,
        resolved);
  }

  @Test
  public void testAppendSetsPosCorrectly() throws Exception {
    FileSystem fs = fileSys.getRawFileSystem();
    Path file = new Path(TEST_ROOT_DIR, "test-append");

    fs.delete(file, true);
    FSDataOutputStream out = fs.create(file);

    try {
      out.write("text1".getBytes());
    } finally {
      out.close();
    }

    // Verify the position
    out = fs.append(file);
    try {
      assertEquals(5, out.getPos());
      out.write("text2".getBytes());
    } finally {
      out.close();
    }

    // Verify the content
    FSDataInputStream in = fs.open(file);
    try {
      byte[] buf = new byte[in.available()];
      in.readFully(buf);
      assertEquals("text1text2", new String(buf));
    } finally {
      in.close();
    }
  }

  @Test
  public void testFileStatusPipeFile() throws Exception {
    RawLocalFileSystem origFs = new RawLocalFileSystem();
    RawLocalFileSystem fs = spy(origFs);
    Configuration conf = mock(Configuration.class);
    fs.setConf(conf);
    Whitebox.setInternalState(fs, "useDeprecatedFileStatus", false);
    Path path = new Path("/foo");
    File pipe = mock(File.class);
    when(pipe.isFile()).thenReturn(false);
    when(pipe.isDirectory()).thenReturn(false);
    when(pipe.exists()).thenReturn(true);

    FileStatus stat = mock(FileStatus.class);
    doReturn(pipe).when(fs).pathToFile(path);
    doReturn(stat).when(fs).getFileStatus(path);
    FileStatus[] stats = fs.listStatus(path);
    assertTrue(stats != null && stats.length == 1 && stats[0] == stat);
  }
}
