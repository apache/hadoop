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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFileUtil {
  private static final Log LOG = LogFactory.getLog(TestFileUtil.class);

  private static final String TEST_ROOT_DIR = System.getProperty(
      "test.build.data", "/tmp") + "/fu";
  private static final File TEST_DIR = new File(TEST_ROOT_DIR);
  private static String FILE = "x";
  private static String LINK = "y";
  private static String DIR = "dir";
  private File del = new File(TEST_DIR, "del");
  private File tmp = new File(TEST_DIR, "tmp");
  private File dir1 = new File(del, DIR + "1");
  private File dir2 = new File(del, DIR + "2");
  private File partitioned = new File(TEST_DIR, "partitioned");

  /**
   * Creates multiple directories for testing.
   * 
   * Contents of them are
   * dir:tmp: 
   *   file: x
   * dir:del:
   *   file: x
   *   dir: dir1 : file:x
   *   dir: dir2 : file:x
   *   link: y to tmp/x
   *   link: tmpDir to tmp
   * dir:partitioned:
   *   file: part-r-00000, contents: "foo"
   *   file: part-r-00001, contents: "bar"
   */
  private void setupDirs() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
    Assert.assertFalse(partitioned.exists());
    del.mkdirs();
    tmp.mkdirs();
    partitioned.mkdirs();
    new File(del, FILE).createNewFile();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();

    // create directories 
    dir1.mkdirs();
    dir2.mkdirs();
    new File(dir1, FILE).createNewFile();
    new File(dir2, FILE).createNewFile();

    // create a symlink to file
    File link = new File(del, LINK);
    FileUtil.symLink(tmpFile.toString(), link.toString());

    // create a symlink to dir
    File linkDir = new File(del, "tmpDir");
    FileUtil.symLink(tmp.toString(), linkDir.toString());
    Assert.assertEquals(5, del.listFiles().length);

    // create files in partitioned directories
    createFile(partitioned, "part-r-00000", "foo");
    createFile(partitioned, "part-r-00001", "bar");
  }

  /**
   * Creates a new file in the specified directory, with the specified name and
   * the specified file contents.  This method will add a newline terminator to
   * the end of the contents string in the destination file.
   * @param directory File non-null destination directory.
   * @param name String non-null file name.
   * @param contents String non-null file contents.
   * @throws IOException if an I/O error occurs.
   */
  private void createFile(File directory, String name, String contents)
      throws IOException {
    File newFile = new File(directory, name);
    PrintWriter pw = new PrintWriter(newFile);

    try {
      pw.println(contents);
    }
    finally {
      pw.close();
    }
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(del);
    FileUtil.fullyDelete(tmp);
    FileUtil.fullyDelete(partitioned);
  }

  @Test
  public void testFullyDelete() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDelete(del);
    Assert.assertTrue(ret);
    Assert.assertFalse(del.exists());
    validateTmpDir();
  }

  /**
   * Tests if fullyDelete deletes
   * (a) symlink to file only and not the file pointed to by symlink.
   * (b) symlink to dir only and not the dir pointed to by symlink.
   * @throws IOException
   */
  @Test
  public void testFullyDeleteSymlinks() throws IOException {
    setupDirs();
    
    File link = new File(del, LINK);
    Assert.assertEquals(5, del.list().length);
    // Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
    // delete contents of tmp. See setupDirs for details.
    boolean ret = FileUtil.fullyDelete(link);
    Assert.assertTrue(ret);
    Assert.assertFalse(link.exists());
    Assert.assertEquals(4, del.list().length);
    validateTmpDir();

    File linkDir = new File(del, "tmpDir");
    // Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
    // delete contents of tmp. See setupDirs for details.
    ret = FileUtil.fullyDelete(linkDir);
    Assert.assertTrue(ret);
    Assert.assertFalse(linkDir.exists());
    Assert.assertEquals(3, del.list().length);
    validateTmpDir();
  }

  /**
   * Tests if fullyDelete deletes
   * (a) dangling symlink to file properly
   * (b) dangling symlink to directory properly
   * @throws IOException
   */
  @Test
  public void testFullyDeleteDanglingSymlinks() throws IOException {
    setupDirs();
    // delete the directory tmp to make tmpDir a dangling link to dir tmp and
    // to make y as a dangling link to file tmp/x
    boolean ret = FileUtil.fullyDelete(tmp);
    Assert.assertTrue(ret);
    Assert.assertFalse(tmp.exists());

    // dangling symlink to file
    File link = new File(del, LINK);
    Assert.assertEquals(5, del.list().length);
    // Even though 'y' is dangling symlink to file tmp/x, fullyDelete(y)
    // should delete 'y' properly.
    ret = FileUtil.fullyDelete(link);
    Assert.assertTrue(ret);
    Assert.assertEquals(4, del.list().length);

    // dangling symlink to directory
    File linkDir = new File(del, "tmpDir");
    // Even though tmpDir is dangling symlink to tmp, fullyDelete(tmpDir) should
    // delete tmpDir properly.
    ret = FileUtil.fullyDelete(linkDir);
    Assert.assertTrue(ret);
    Assert.assertEquals(3, del.list().length);
  }

  @Test
  public void testFullyDeleteContents() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDeleteContents(del);
    Assert.assertTrue(ret);
    Assert.assertTrue(del.exists());
    Assert.assertEquals(0, del.listFiles().length);
    validateTmpDir();
  }

  private void validateTmpDir() {
    Assert.assertTrue(tmp.exists());
    Assert.assertEquals(1, tmp.listFiles().length);
    Assert.assertTrue(new File(tmp, FILE).exists());
  }

  private File xSubDir = new File(del, "xsubdir");
  private File ySubDir = new File(del, "ysubdir");
  static String file1Name = "file1";
  private File file2 = new File(xSubDir, "file2");
  private File file3 = new File(ySubDir, "file3");
  private File zlink = new File(del, "zlink");
  
  /**
   * Creates a directory which can not be deleted completely.
   * 
   * Directory structure. The naming is important in that {@link MyFile}
   * is used to return them in alphabetical order when listed.
   * 
   *                     del(+w)
   *                       |
   *    .---------------------------------------,
   *    |            |              |           |
   *  file1(!w)   xsubdir(-w)   ysubdir(+w)   zlink
   *                 |              |
   *               file2          file3
   *
   * @throws IOException
   */
  private void setupDirsAndNonWritablePermissions() throws IOException {
    Assert.assertFalse("The directory del should not have existed!",
        del.exists());
    del.mkdirs();
    new MyFile(del, file1Name).createNewFile();

    // "file1" is non-deletable by default, see MyFile.delete().

    xSubDir.mkdirs();
    file2.createNewFile();
    xSubDir.setWritable(false);
    ySubDir.mkdirs();
    file3.createNewFile();

    Assert.assertFalse("The directory tmp should not have existed!",
        tmp.exists());
    tmp.mkdirs();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();
    FileUtil.symLink(tmpFile.toString(), zlink.toString());
  }
  
  // Validates the return value.
  // Validates the existence of directory "xsubdir" and the file "file1"
  // Sets writable permissions for the non-deleted dir "xsubdir" so that it can
  // be deleted in tearDown().
  private void validateAndSetWritablePermissions(boolean ret) {
    xSubDir.setWritable(true);
    Assert.assertFalse("The return value should have been false!", ret);
    Assert.assertTrue("The file file1 should not have been deleted!",
        new File(del, file1Name).exists());
    Assert.assertTrue(
        "The directory xsubdir should not have been deleted!",
        xSubDir.exists());
    Assert.assertTrue("The file file2 should not have been deleted!",
        file2.exists());
    Assert.assertFalse("The directory ysubdir should have been deleted!",
        ySubDir.exists());
    Assert.assertFalse("The link zlink should have been deleted!",
        zlink.exists());
  }

  @Test
  public void testFailFullyDelete() throws IOException {
    LOG.info("Running test to verify failure of fullyDelete()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDelete(new MyFile(del));
    validateAndSetWritablePermissions(ret);
  }

  /**
   * Extend {@link File}. Same as {@link File} except for two things: (1) This
   * treats file1Name as a very special file which is not delete-able
   * irrespective of it's parent-dir's permissions, a peculiar file instance for
   * testing. (2) It returns the files in alphabetically sorted order when
   * listed.
   * 
   */
  public static class MyFile extends File {

    private static final long serialVersionUID = 1L;

    public MyFile(File f) {
      super(f.getAbsolutePath());
    }

    public MyFile(File parent, String child) {
      super(parent, child);
    }

    /**
     * Same as {@link File#delete()} except for file1Name which will never be
     * deleted (hard-coded)
     */
    @Override
    public boolean delete() {
      LOG.info("Trying to delete myFile " + getAbsolutePath());
      boolean bool = false;
      if (getName().equals(file1Name)) {
        bool = false;
      } else {
        bool = super.delete();
      }
      if (bool) {
        LOG.info("Deleted " + getAbsolutePath() + " successfully");
      } else {
        LOG.info("Cannot delete " + getAbsolutePath());
      }
      return bool;
    }

    /**
     * Return the list of files in an alphabetically sorted order
     */
    @Override
    public File[] listFiles() {
      File[] files = super.listFiles();
      List<File> filesList = Arrays.asList(files);
      Collections.sort(filesList);
      File[] myFiles = new MyFile[files.length];
      int i=0;
      for(File f : filesList) {
        myFiles[i++] = new MyFile(f);
      }
      return myFiles;
    }
  }

  @Test
  public void testFailFullyDeleteContents() throws IOException {
    LOG.info("Running test to verify failure of fullyDeleteContents()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(new MyFile(del));
    validateAndSetWritablePermissions(ret);
  }

  @Test
  public void testCopyMergeSingleDirectory() throws IOException {
    setupDirs();
    boolean copyMergeResult = copyMerge("partitioned", "tmp/merged");
    Assert.assertTrue("Expected successful copyMerge result.", copyMergeResult);
    File merged = new File(TEST_DIR, "tmp/merged");
    Assert.assertTrue("File tmp/merged must exist after copyMerge.",
        merged.exists());
    BufferedReader rdr = new BufferedReader(new FileReader(merged));

    try {
      Assert.assertEquals("Line 1 of merged file must contain \"foo\".",
          "foo", rdr.readLine());
      Assert.assertEquals("Line 2 of merged file must contain \"bar\".",
          "bar", rdr.readLine());
      Assert.assertNull("Expected end of file reading merged file.",
          rdr.readLine());
    }
    finally {
      rdr.close();
    }
  }

  /**
   * Calls FileUtil.copyMerge using the specified source and destination paths.
   * Both source and destination are assumed to be on the local file system.
   * The call will not delete source on completion and will not add an
   * additional string between files.
   * @param src String non-null source path.
   * @param dst String non-null destination path.
   * @return boolean true if the call to FileUtil.copyMerge was successful.
   * @throws IOException if an I/O error occurs.
   */
  private boolean copyMerge(String src, String dst)
      throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    final boolean result;

    try {
      Path srcPath = new Path(TEST_ROOT_DIR, src);
      Path dstPath = new Path(TEST_ROOT_DIR, dst);
      boolean deleteSource = false;
      String addString = null;
      result = FileUtil.copyMerge(fs, srcPath, fs, dstPath, deleteSource, conf,
          addString);
    }
    finally {
      fs.close();
    }

    return result;
  }
}
