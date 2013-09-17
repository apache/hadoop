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

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * <p>
 * A collection of tests for the contract of the {@link FileSystem}.
 * This test should be used for general-purpose implementations of
 * {@link FileSystem}, that is, implementations that provide implementations 
 * of all of the functionality of {@link FileSystem}.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fs</code> 
 * {@link FileSystem} instance variable.
 * </p>
 */
public abstract class FileSystemContractBaseTest extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(FileSystemContractBaseTest.class);

  protected final static String TEST_UMASK = "062";
  protected FileSystem fs;
  protected byte[] data = dataset(getBlockSize() * 2, 0, 255);

  @Override
  protected void tearDown() throws Exception {
    fs.delete(path("/test"), true);
  }
  
  protected int getBlockSize() {
    return 1024;
  }
  
  protected String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }

  protected boolean renameSupported() {
    return true;
  }

  public void testFsStatus() throws Exception {
    FsStatus fsStatus = fs.getStatus();
    assertNotNull(fsStatus);
    //used, free and capacity are non-negative longs
    assertTrue(fsStatus.getUsed() >= 0);
    assertTrue(fsStatus.getRemaining() >= 0);
    assertTrue(fsStatus.getCapacity() >= 0);
  }
  
  public void testWorkingDirectory() throws Exception {

    Path workDir = path(getDefaultWorkingDirectory());
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(path("."));
    assertEquals(workDir, fs.getWorkingDirectory());

    fs.setWorkingDirectory(path(".."));
    assertEquals(workDir.getParent(), fs.getWorkingDirectory());

    Path relativeDir = path("hadoop");
    fs.setWorkingDirectory(relativeDir);
    assertEquals(relativeDir, fs.getWorkingDirectory());
    
    Path absoluteDir = path("/test/hadoop");
    fs.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fs.getWorkingDirectory());

  }
  
  public void testMkdirs() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));

    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));
    
    assertTrue(fs.mkdirs(testDir));

    assertTrue(fs.exists(testDir));
    assertFalse(fs.isFile(testDir));

    Path parentDir = testDir.getParent();
    assertTrue(fs.exists(parentDir));
    assertFalse(fs.isFile(parentDir));

    Path grandparentDir = parentDir.getParent();
    assertTrue(fs.exists(grandparentDir));
    assertFalse(fs.isFile(grandparentDir));
    
  }
  
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));
    
    createFile(path("/test/hadoop/file"));
    
    Path testSubDir = path("/test/hadoop/file/subdir");
    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(fs.exists(testSubDir));
    
    Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertFalse(fs.exists(testDeepSubDir));
    
  }

  public void testMkdirsWithUmask() throws Exception {
    if (fs.getScheme().equals("s3") || fs.getScheme().equals("s3n")) {
      // skip permission tests for S3FileSystem until HDFS-1333 is fixed.
      return;
    }
    Configuration conf = fs.getConf();
    String oldUmask = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    try {
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, TEST_UMASK);
      final Path dir = new Path("/test/newDir");
      assertTrue(fs.mkdirs(dir, new FsPermission((short)0777)));
      FileStatus status = fs.getFileStatus(dir);
      assertTrue(status.isDirectory());
      assertEquals((short)0715, status.getPermission().toShort());
    } finally {
      conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, oldUmask);
    }
  }

  public void testGetFileStatusThrowsExceptionForNonExistentFile() 
    throws Exception {
    try {
      fs.getFileStatus(path("/test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // expected
    }
  }
  
  public void testListStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      fs.listStatus(path("/test/hadoop/file"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  public void testListStatus() throws Exception {
    Path[] testDirs = { path("/test/hadoop/a"),
                        path("/test/hadoop/b"),
                        path("/test/hadoop/c/1"), };
    assertFalse(fs.exists(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("/test"));
    assertEquals(1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());

    paths = fs.listStatus(path("/test/hadoop"));
    assertEquals(3, paths.length);
    assertEquals(path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(path("/test/hadoop/c"), paths[2].getPath());

    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(0, paths.length);
  }
  
  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(getBlockSize() / 2);
  }

  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(getBlockSize());
  }
  
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() + (getBlockSize() / 2));
  }
  
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(getBlockSize() * 2);
  }

  /**
   * Write a dataset, read it back in and verify that they match.
   * Afterwards, the file is deleted.
   * @param len length of data
   * @throws IOException on IO failures
   */
  protected void writeReadAndDelete(int len) throws IOException {
    Path path = path("/test/hadoop/file");
    writeAndRead(path, data, len, false, true);
  }
  
  public void testOverwrite() throws IOException {
    Path path = path("/test/hadoop/file");
    
    fs.mkdirs(path.getParent());

    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
    try {
      fs.create(path, false).close();
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = fs.create(path, true);
    out.write(data, 0, data.length);
    out.close();
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    
  }
  
  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = path("/test/hadoop/file");
    assertFalse("Parent doesn't exist", fs.exists(path.getParent()));
    createFile(path);
    
    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", data.length, fs.getFileStatus(path).getLen());
    assertTrue("Parent exists", fs.exists(path.getParent()));
  }

  public void testDeleteNonExistentFile() throws IOException {
    Path path = path("/test/hadoop/file");    
    assertFalse("Doesn't exist", fs.exists(path));
    assertFalse("No deletion", fs.delete(path, true));
  }
  
  public void testDeleteRecursively() throws IOException {
    Path dir = path("/test/hadoop");
    Path file = path("/test/hadoop/file");
    Path subdir = path("/test/hadoop/subdir");
    
    createFile(file);
    assertTrue("Created subdir", fs.mkdirs(subdir));
    
    assertTrue("File exists", fs.exists(file));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Subdir exists", fs.exists(subdir));
    
    try {
      fs.delete(dir, false);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    assertTrue("File still exists", fs.exists(file));
    assertTrue("Dir still exists", fs.exists(dir));
    assertTrue("Subdir still exists", fs.exists(subdir));
    
    assertTrue("Deleted", fs.delete(dir, true));
    assertFalse("File doesn't exist", fs.exists(file));
    assertFalse("Dir doesn't exist", fs.exists(dir));
    assertFalse("Subdir doesn't exist", fs.exists(subdir));
  }
  
  public void testDeleteEmptyDirectory() throws IOException {
    Path dir = path("/test/hadoop");
    assertTrue(fs.mkdirs(dir));
    assertTrue("Dir exists", fs.exists(dir));
    assertTrue("Deleted", fs.delete(dir, false));
    assertFalse("Dir doesn't exist", fs.exists(dir));
  }
  
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/path");
    Path dst = path("/test/new/newpath");
    rename(src, dst, false, false, false);
  }

  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    rename(src, dst, false, true, false);
  }

  public void testRenameFileMoveToExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
  }

  public void testRenameFileAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }

  public void testRenameFileAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed",
        fs.exists(path("/test/new/newdir/file")));
  }
  
  public void testRenameDirectoryMoveToNonExistentDirectory() 
    throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    Path dst = path("/test/new/newdir");
    rename(src, dst, false, true, false);
  }
  
  public void testRenameDirectoryMoveToExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
    
    assertFalse("Nested file1 exists",
        fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path("/test/new/newdir/subdir/file2")));
  }
  
  public void testRenameDirectoryAsExistingFile() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    Path dst = path("/test/new/newfile");
    createFile(dst);
    rename(src, dst, false, true, true);
  }
  
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;
    
    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed",
        fs.exists(path("/test/new/newdir/dir")));    
    assertFalse("Nested file1 exists",
        fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path("/test/new/newdir/dir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path("/test/new/newdir/dir/subdir/file2")));
  }

  public void testInputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("/test/hadoop/file");
    createFile(src);
    FSDataInputStream in = fs.open(src);
    in.close();
    in.close();
  }
  
  public void testOutputStreamClosedTwice() throws IOException {
    //HADOOP-4760 according to Closeable#close() closing already-closed 
    //streams should have no effect. 
    Path src = path("/test/hadoop/file");
    FSDataOutputStream out = fs.create(src);
    out.writeChar('H'); //write some data
    out.close();
    out.close();
  }
  
  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs);
  }
  
  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(data, 0, data.length);
    out.close();
  }
  
  private void rename(Path src, Path dst, boolean renameSucceeded,
      boolean srcExists, boolean dstExists) throws IOException {
    assertEquals("Rename result", renameSucceeded, fs.rename(src, dst));
    assertEquals("Source exists", srcExists, fs.exists(src));
    assertEquals("Destination exists", dstExists, fs.exists(dst));
  }

  /**
   * Verify that if you take an existing file and overwrite it, the new values
   * get picked up.
   * This is a test for the behavior of eventually consistent
   * filesystems.
   *
   * @throws Exception on any failure
   */

  public void testOverWriteAndRead() throws Exception {
    int blockSize = getBlockSize();

    byte[] filedata1 = dataset(blockSize * 2, 'A', 26);
    byte[] filedata2 = dataset(blockSize * 2, 'a', 26);
    Path path = path("/test/hadoop/file-overwrite");
    writeAndRead(path, filedata1, blockSize, true, false);
    writeAndRead(path, filedata2, blockSize, true, false);
    writeAndRead(path, filedata1, blockSize * 2, true, false);
    writeAndRead(path, filedata2, blockSize * 2, true, false);
    writeAndRead(path, filedata1, blockSize, true, false);
    writeAndRead(path, filedata2, blockSize * 2, true, false);
  }

  /**
   *
   * Write a file and read it in, validating the result. Optional flags control
   * whether file overwrite operations should be enabled, and whether the
   * file should be deleted afterwards.
   *
   * If there is a mismatch between what was written and what was expected,
   * a small range of bytes either side of the first error are logged to aid
   * diagnosing what problem occurred -whether it was a previous file
   * or a corrupting of the current file. This assumes that two
   * sequential runs to the same path use datasets with different character
   * moduli.
   *
   * @param path path to write to
   * @param len length of data
   * @param overwrite should the create option allow overwrites?
   * @param delete should the file be deleted afterwards? -with a verification
   * that it worked. Deletion is not attempted if an assertion has failed
   * earlier -it is not in a <code>finally{}</code> block.
   * @throws IOException IO problems
   */
  protected void writeAndRead(Path path, byte[] src, int len,
                              boolean overwrite,
                              boolean delete) throws IOException {
    assertTrue("Not enough data in source array to write " + len + " bytes",
               src.length >= len);
    fs.mkdirs(path.getParent());

    FSDataOutputStream out = fs.create(path, overwrite,
                                       fs.getConf().getInt("io.file.buffer.size",
                                                           4096),
                                       (short) 1, getBlockSize());
    out.write(src, 0, len);
    out.close();

    assertTrue("Exists", fs.exists(path));
    assertEquals("Length", len, fs.getFileStatus(path).getLen());

    FSDataInputStream in = fs.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    assertEquals(len, buf.length);
    int errors = 0;
    int first_error_byte = -1;
    for (int i = 0; i < len; i++) {
      if (src[i] != buf[i]) {
        if (errors == 0) {
          first_error_byte = i;
        }
        errors++;
      }
    }

    if (errors > 0) {
      String message = String.format(" %d errors in file of length %d",
                                     errors, len);
      LOG.warn(message);
      // the range either side of the first error to print
      // this is a purely arbitrary number, to aid user debugging
      final int overlap = 10;
      for (int i = Math.max(0, first_error_byte - overlap);
           i < Math.min(first_error_byte + overlap, len);
           i++) {
        byte actual = buf[i];
        byte expected = src[i];
        String letter = toChar(actual);
        String line = String.format("[%04d] %2x %s\n", i, actual, letter);
        if (expected != actual) {
          line = String.format("[%04d] %2x %s -expected %2x %s\n",
                               i,
                               actual,
                               letter,
                               expected,
                               toChar(expected));
        }
        LOG.warn(line);
      }
      fail(message);
    }

    if (delete) {
      boolean deleted = fs.delete(path, false);
      assertTrue("Deleted", deleted);
      assertFalse("No longer exists", fs.exists(path));
    }
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is < 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  protected String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    } else {
      return String.format("%02x", b);
    }
  }

  /**
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   * @return the newly generated dataset
   */
  protected byte[] dataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }
}
