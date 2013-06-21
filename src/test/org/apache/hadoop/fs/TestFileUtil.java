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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFileUtil {
  private static final Log LOG = LogFactory.getLog(TestFileUtil.class);

  final static private File TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "fu");
  private static String FILE = "x";
  private static String LINK = "y";
  private static String DIR = "dir";
  private File del = new File(TEST_DIR, "del");
  private File tmp = new File(TEST_DIR, "tmp");

  /**
   * Creates directories del and tmp for testing.
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
   */
  private void setupDirs() throws IOException {
    Assert.assertFalse(del.exists());
    Assert.assertFalse(tmp.exists());
    del.mkdirs();
    tmp.mkdirs();
    new File(del, FILE).createNewFile();
    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();

    // create directories 
    File one = new File(del, DIR + "1");
    one.mkdirs();
    File two = new File(del, DIR + "2");
    two.mkdirs();
    new File(one, FILE).createNewFile();
    new File(two, FILE).createNewFile();

    // create a symlink to file
    File link = new File(del, LINK);
    FileUtil.symLink(tmpFile.toString(), link.toString());

    // create a symlink to dir
    File linkDir = new File(del, "tmpDir");
    FileUtil.symLink(tmp.toString(), linkDir.toString());
    Assert.assertEquals(5, del.listFiles().length);

    // create a cycle using symlinks. Cycles should be handled
    FileUtil.symLink(del.toString(), del.toString() + "/" + DIR + "1/cycle");
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(del);
    FileUtil.fullyDelete(tmp);
  }

  @Test
  public void testFullyDelete() throws IOException {
    setupDirs();
    boolean ret = FileUtil.fullyDelete(del);
    Assert.assertTrue(ret);
    Assert.assertFalse(del.exists());
    validateTmpDir();
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
    if(Shell.WINDOWS) {
      // windows Dir.setWritable(false) does not work for directories
      return;
    }
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
    if(Shell.WINDOWS) {
      // windows Dir.setWritable(false) does not work for directories
      return;
    }
    LOG.info("Running test to verify failure of fullyDeleteContents()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(new MyFile(del));
    validateAndSetWritablePermissions(ret);
  }
  
  @Test
  public void testListFiles() throws IOException {
    setupDirs();
    //Test existing files case 
    File[] files = FileUtil.listFiles(tmp);
    Assert.assertEquals(1, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    newDir.mkdir();
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.listFiles(newDir);
    Assert.assertEquals(0, files.length);
    newDir.delete();
    Assert.assertFalse("Failed to delete test dir", newDir.exists());
    
    //Test non-existing directory case, this throws 
    //IOException
    try {
      files = FileUtil.listFiles(newDir);
      Assert.fail("IOException expected on listFiles() for non-existent dir "
          + newDir.toString());
    } catch(IOException ioe) {
      //Expected an IOException
    }
  }

  @Test
  public void testListAPI() throws IOException {
    setupDirs();
    //Test existing files case 
    String[] files = FileUtil.list(tmp);
    Assert.assertEquals(1, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    newDir.mkdir();
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.list(newDir);
    Assert.assertEquals(0, files.length);
    newDir.delete();
    Assert.assertFalse("Failed to delete test dir", newDir.exists());
    
    //Test non-existing directory case, this throws 
    //IOException
    try {
      files = FileUtil.list(newDir);
      Assert.fail("IOException expected on list() for non-existent dir "
          + newDir.toString());
    } catch(IOException ioe) {
      //Expected an IOException
    }
  }

  /**
   * Test that getDU is able to handle cycles caused due to symbolic links
   * and that directory sizes are not added to the final calculated size
   * @throws IOException
   */
  @Test
  public void testGetDU() throws IOException {
    setupDirs();

    long du = FileUtil.getDU(TEST_DIR);
    Assert.assertEquals(du, 0);
  }

  @Test
  public void testSymlink() throws Exception {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    byte[] data = "testSymLink".getBytes();

    File file = new File(del, FILE);
    File link = new File(del, "_link");

    //write some data to the file
    FileOutputStream os = new FileOutputStream(file);
    os.write(data);
    os.close();

    //create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    //ensure that symlink length is correctly reported by Java
    Assert.assertEquals(data.length, file.length());
    Assert.assertEquals(data.length, link.length());

    //ensure that we can read from link.
    FileInputStream in = new FileInputStream(link);
    long len = 0;
    while (in.read() > 0) {
      len++;
    }
    in.close();
    Assert.assertEquals(data.length, len);
  }

  /**
   * Test that rename on a symlink works as expected.
   */
  @Test
  public void testSymlinkRenameTo() throws Exception {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File file = new File(del, FILE);
    file.createNewFile();
    File link = new File(del, "_link");

    // create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertTrue(file.exists());
    Assert.assertTrue(link.exists());

    File link2 = new File(del, "_link2");

    // Rename the symlink
    Assert.assertTrue(link.renameTo(link2));

    // Make sure the file still exists
    // (NOTE: this would fail on Java6 on Windows if we didn't
    // copy the file in FileUtil#symlink)
    Assert.assertTrue(file.exists());

    Assert.assertTrue(link2.exists());
    Assert.assertFalse(link.exists());
  }

  /**
   * Test that deletion of a symlink works as expected.
   */
  @Test
  public void testSymlinkDelete() throws Exception {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    File file = new File(del, FILE);
    file.createNewFile();
    File link = new File(del, "_link");

    // create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertTrue(file.exists());
    Assert.assertTrue(link.exists());

    // make sure that deleting a symlink works properly
    Assert.assertTrue(link.delete());
    Assert.assertFalse(link.exists());
    Assert.assertTrue(file.exists());
  }

  /**
   * Test that length on a symlink works as expected.
   */
  @Test
  public void testSymlinkLength() throws Exception {
    Assert.assertFalse(del.exists());
    del.mkdirs();

    byte[] data = "testSymLinkData".getBytes();

    File file = new File(del, FILE);
    File link = new File(del, "_link");

    // write some data to the file
    FileOutputStream os = new FileOutputStream(file);
    os.write(data);
    os.close();

    Assert.assertEquals(0, link.length());

    // create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    // ensure that File#length returns the target file and link size
    Assert.assertEquals(data.length, file.length());
    Assert.assertEquals(data.length, link.length());

    file.delete();
    Assert.assertFalse(file.exists());

    if (Shell.WINDOWS && !Shell.isJava7OrAbove()) {
      // On Java6 on Windows, we copied the file
      Assert.assertEquals(data.length, link.length());
    } else {
      // Otherwise, the target file size is zero
      Assert.assertEquals(0, link.length());
    }

    link.delete();
    Assert.assertFalse(link.exists());
  }

  private void doUntarAndVerify(File tarFile, File untarDir) 
                                 throws IOException {
    if (untarDir.exists() && !FileUtil.fullyDelete(untarDir)) {
      throw new IOException("Could not delete directory '" + untarDir + "'");
    }
    FileUtil.unTar(tarFile, untarDir);

    String parentDir = untarDir.getCanonicalPath() + Path.SEPARATOR + "name";
    File testFile = new File(parentDir + Path.SEPARATOR + "version");
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(testFile.length() == 0);
    String imageDir = parentDir + Path.SEPARATOR + "image";
    testFile = new File(imageDir + Path.SEPARATOR + "fsimage");
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(testFile.length() == 157);
    String currentDir = parentDir + Path.SEPARATOR + "current";
    testFile = new File(currentDir + Path.SEPARATOR + "fsimage");
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(testFile.length() == 4331);
    testFile = new File(currentDir + Path.SEPARATOR + "edits");
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(testFile.length() == 1033);
    testFile = new File(currentDir + Path.SEPARATOR + "fstime");
    Assert.assertTrue(testFile.exists());
    Assert.assertTrue(testFile.length() == 8);
  }

  @Test
  public void testUntar() throws IOException {
    String tarGzFileName = System.getProperty("test.cache.data",
        "build/test/cache") + "/test-untar.tgz";
    String tarFileName = System.getProperty("test.cache.data",
        "build/test/cache") + "/test-untar.tar";
    String dataDir = System.getProperty("test.build.data", "build/test/data");
    File untarDir = new File(dataDir, "untarDir");

    doUntarAndVerify(new File(tarGzFileName), untarDir);
    doUntarAndVerify(new File(tarFileName), untarDir);
  }
  
  /**
   * Test that copies file from local filesystem to hdfs. It tests the overwrite
   * flag is respected when copying files over.
   */
  @Test
  public void testCopy() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
    // local file system
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    try {
      String fname = "testCopyFromLocal1.txt";
      File f = new File(TEST_DIR, fname);
      f.createNewFile();
      // copy the file to hdfs with overwrite = false
      boolean status = FileUtil.copy(lfs, new Path(f.getAbsolutePath()), dfs,
          new Path("."), false, true, conf);
      assertTrue(status);
      // make sure the file exists on hdfs
      assertTrue("File " + fname + " does not exist on hdfs.",
          dfs.exists(new Path(fname)));
      // copy the same file to hdfs with overwrite and it should pass
      status = FileUtil.copy(lfs, new Path(f.getAbsolutePath()), dfs, new Path(
          "."), false, true, conf);
      assertTrue(status);

      try {
        // copy the same file to hdfs with overwrite as false and it should
        // thrown an exception, catch it and make sure the file name is part of
        // the message
        status = FileUtil.copy(lfs, new Path(f.getAbsolutePath()), dfs,
            new Path("."), false, false, conf);
      } catch (IOException expected) {
        assertTrue("Exception message doesn't contain filename " + fname,
            expected.getMessage().indexOf(fname) >= 0);
      }
    } finally {
      try {
        dfs.close();
        lfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }
}
