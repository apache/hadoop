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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileUtil.class);

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private static final String FILE = "x";
  private static final String LINK = "y";
  private static final String DIR = "dir";

  private static final String FILE_1_NAME = "file1";

  private File del;
  private File tmp;
  private File dir1;
  private File dir2;
  private File partitioned;

  private File xSubDir;
  private File xSubSubDir;
  private File ySubDir;

  private File file2;
  private File file22;
  private File file3;
  private File zlink;

  private InetAddress inet1;
  private InetAddress inet2;
  private InetAddress inet3;
  private InetAddress inet4;
  private InetAddress inet5;
  private InetAddress inet6;
  private URI uri1;
  private URI uri2;
  private URI uri3;
  private URI uri4;
  private URI uri5;
  private URI uri6;
  private FileSystem fs1;
  private FileSystem fs2;
  private FileSystem fs3;
  private FileSystem fs4;
  private FileSystem fs5;
  private FileSystem fs6;

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
  @Before
  public void setup() throws IOException {
    del = testFolder.newFolder("del");
    tmp = testFolder.newFolder("tmp");
    partitioned = testFolder.newFolder("partitioned");

    zlink = new File(del, "zlink");

    xSubDir = new File(del, "xSubDir");
    xSubSubDir = new File(xSubDir, "xSubSubDir");
    ySubDir = new File(del, "ySubDir");


    file2 = new File(xSubDir, "file2");
    file22 = new File(xSubSubDir, "file22");
    file3 = new File(ySubDir, "file3");

    dir1 = new File(del, DIR + "1");
    dir2 = new File(del, DIR + "2");

    FileUtils.forceMkdir(dir1);
    FileUtils.forceMkdir(dir2);

    Verify.createNewFile(new File(del, FILE));
    File tmpFile = Verify.createNewFile(new File(tmp, FILE));

    // create files
    Verify.createNewFile(new File(dir1, FILE));
    Verify.createNewFile(new File(dir2, FILE));

    // create a symlink to file
    File link = new File(del, LINK);
    FileUtil.symLink(tmpFile.toString(), link.toString());

    // create a symlink to dir
    File linkDir = new File(del, "tmpDir");
    FileUtil.symLink(tmp.toString(), linkDir.toString());
    Assert.assertEquals(5, Objects.requireNonNull(del.listFiles()).length);

    // create files in partitioned directories
    createFile(partitioned, "part-r-00000", "foo");
    createFile(partitioned, "part-r-00001", "bar");

    // create a cycle using symlinks. Cycles should be handled
    FileUtil.symLink(del.toString(), dir1.toString() + "/cycle");
  }

  @After
  public void tearDown() throws IOException {
    testFolder.delete();
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
  private File createFile(File directory, String name, String contents)
      throws IOException {
    File newFile = new File(directory, name);
    try (PrintWriter pw = new PrintWriter(newFile)) {
      pw.println(contents);
    }
    return newFile;
  }

  @Test (timeout = 30000)
  public void testListFiles() throws IOException {
    //Test existing files case 
    File[] files = FileUtil.listFiles(partitioned);
    Assert.assertEquals(2, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    Verify.mkdir(newDir);
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.listFiles(newDir);
    Assert.assertEquals(0, files.length);
    assertTrue(newDir.delete());
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

  @Test (timeout = 30000)
  public void testListAPI() throws IOException {
    //Test existing files case 
    String[] files = FileUtil.list(partitioned);
    Assert.assertEquals("Unexpected number of pre-existing files", 2, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    Verify.mkdir(newDir);
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.list(newDir);
    Assert.assertEquals("New directory unexpectedly contains files", 0, files.length);
    assertTrue(newDir.delete());
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

  @Test (timeout = 30000)
  public void testFullyDelete() throws IOException {
    boolean ret = FileUtil.fullyDelete(del);
    Assert.assertTrue(ret);
    Verify.notExists(del);
    validateTmpDir();
  }

  /**
   * Tests if fullyDelete deletes
   * (a) symlink to file only and not the file pointed to by symlink.
   * (b) symlink to dir only and not the dir pointed to by symlink.
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testFullyDeleteSymlinks() throws IOException {
    File link = new File(del, LINK);
    assertDelListLength(5);
    // Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
    // delete contents of tmp. See setupDirs for details.
    boolean ret = FileUtil.fullyDelete(link);
    Assert.assertTrue(ret);
    Verify.notExists(link);
    assertDelListLength(4);
    validateTmpDir();

    File linkDir = new File(del, "tmpDir");
    // Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
    // delete contents of tmp. See setupDirs for details.
    ret = FileUtil.fullyDelete(linkDir);
    Assert.assertTrue(ret);
    Verify.notExists(linkDir);
    assertDelListLength(3);
    validateTmpDir();
  }

  /**
   * Tests if fullyDelete deletes
   * (a) dangling symlink to file properly
   * (b) dangling symlink to directory properly
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testFullyDeleteDanglingSymlinks() throws IOException {
    // delete the directory tmp to make tmpDir a dangling link to dir tmp and
    // to make y as a dangling link to file tmp/x
    boolean ret = FileUtil.fullyDelete(tmp);
    Assert.assertTrue(ret);
    Verify.notExists(tmp);

    // dangling symlink to file
    File link = new File(del, LINK);
    assertDelListLength(5);
    // Even though 'y' is dangling symlink to file tmp/x, fullyDelete(y)
    // should delete 'y' properly.
    ret = FileUtil.fullyDelete(link);
    Assert.assertTrue(ret);
    assertDelListLength(4);

    // dangling symlink to directory
    File linkDir = new File(del, "tmpDir");
    // Even though tmpDir is dangling symlink to tmp, fullyDelete(tmpDir) should
    // delete tmpDir properly.
    ret = FileUtil.fullyDelete(linkDir);
    Assert.assertTrue(ret);
    assertDelListLength(3);
  }

  @Test (timeout = 30000)
  public void testFullyDeleteContents() throws IOException {
    boolean ret = FileUtil.fullyDeleteContents(del);
    Assert.assertTrue(ret);
    Verify.exists(del);
    Assert.assertEquals(0, Objects.requireNonNull(del.listFiles()).length);
    validateTmpDir();
  }

  private void validateTmpDir() {
    Verify.exists(tmp);
    Assert.assertEquals(1, Objects.requireNonNull(tmp.listFiles()).length);
    Verify.exists(new File(tmp, FILE));
  }

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
   *  file1(!w)   xSubDir(-rwx)   ySubDir(+w)   zlink
   *              |  |              |
   *              | file2(-rwx)   file3
   *              |
   *            xSubSubDir(-rwx) 
   *              |
   *             file22(-rwx)
   *             
   * @throws IOException
   */
  private void setupDirsAndNonWritablePermissions() throws IOException {
    Verify.createNewFile(new MyFile(del, FILE_1_NAME));

    // "file1" is non-deletable by default, see MyFile.delete().

    Verify.mkdirs(xSubDir);
    Verify.createNewFile(file2);

    Verify.mkdirs(xSubSubDir);
    Verify.createNewFile(file22);

    revokePermissions(file22);
    revokePermissions(xSubSubDir);

    revokePermissions(file2);
    revokePermissions(xSubDir);

    Verify.mkdirs(ySubDir);
    Verify.createNewFile(file3);

    File tmpFile = new File(tmp, FILE);
    tmpFile.createNewFile();
    FileUtil.symLink(tmpFile.toString(), zlink.toString());
  }

  private static void grantPermissions(final File f) {
    FileUtil.setReadable(f, true);
    FileUtil.setWritable(f, true);
    FileUtil.setExecutable(f, true);
  }
  
  private static void revokePermissions(final File f) {
     FileUtil.setWritable(f, false);
     FileUtil.setExecutable(f, false);
     FileUtil.setReadable(f, false);
  }
  
  // Validates the return value.
  // Validates the existence of the file "file1"
  private void validateAndSetWritablePermissions(
      final boolean expectedRevokedPermissionDirsExist, final boolean ret) {
    grantPermissions(xSubDir);
    grantPermissions(xSubSubDir);
    
    Assert.assertFalse("The return value should have been false.", ret);
    Assert.assertTrue("The file file1 should not have been deleted.",
        new File(del, FILE_1_NAME).exists());
    
    Assert.assertEquals(
        "The directory xSubDir *should* not have been deleted.",
        expectedRevokedPermissionDirsExist, xSubDir.exists());
    Assert.assertEquals("The file file2 *should* not have been deleted.",
        expectedRevokedPermissionDirsExist, file2.exists());
    Assert.assertEquals(
        "The directory xSubSubDir *should* not have been deleted.",
        expectedRevokedPermissionDirsExist, xSubSubDir.exists());
    Assert.assertEquals("The file file22 *should* not have been deleted.",
        expectedRevokedPermissionDirsExist, file22.exists());
    
    Assert.assertFalse("The directory ySubDir should have been deleted.",
        ySubDir.exists());
    Assert.assertFalse("The link zlink should have been deleted.",
        zlink.exists());
  }

  @Test (timeout = 30000)
  public void testFailFullyDelete() throws IOException {
    // Windows Dir.setWritable(false) does not work for directories
    assumeNotWindows();
    LOG.info("Running test to verify failure of fullyDelete()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDelete(new MyFile(del));
    validateAndSetWritablePermissions(true, ret);
  }

  @Test (timeout = 30000)
  public void testFailFullyDeleteGrantPermissions() throws IOException {
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDelete(new MyFile(del), true);
    // this time the directories with revoked permissions *should* be deleted:
    validateAndSetWritablePermissions(false, ret);
  }


  /**
   * Tests if fullyDelete deletes symlink's content when deleting unremovable dir symlink.
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testFailFullyDeleteDirSymlinks() throws IOException {
    File linkDir = new File(del, "tmpDir");
    FileUtil.setWritable(del, false);
    // Since tmpDir is symlink to tmp, fullyDelete(tmpDir) should not
    // delete contents of tmp. See setupDirs for details.
    boolean ret = FileUtil.fullyDelete(linkDir);
    // fail symlink deletion
    Assert.assertFalse(ret);
    Verify.exists(linkDir);
    assertDelListLength(5);
    // tmp dir should exist
    validateTmpDir();
    // simulate disk recovers and turns good
    FileUtil.setWritable(del, true);
    ret = FileUtil.fullyDelete(linkDir);
    // success symlink deletion
    Assert.assertTrue(ret);
    Verify.notExists(linkDir);
    assertDelListLength(4);
    // tmp dir should exist
    validateTmpDir();
  }

  /**
   * Asserts if the {@link TestFileUtil#del} meets the given expected length.
   *
   * @param expectedLength The expected length of the {@link TestFileUtil#del}.
   */
  private void assertDelListLength(int expectedLength) {
    Assertions.assertThat(del.list()).describedAs("del list").isNotNull().hasSize(expectedLength);
  }

  /**
   * Helper class to perform {@link File} operation and also verify them.
   */
  public static class Verify {
    /**
     * Invokes {@link File#createNewFile()} on the given {@link File} instance.
     *
     * @param file The file to call {@link File#createNewFile()} on.
     * @return The result of {@link File#createNewFile()}.
     * @throws IOException As per {@link File#createNewFile()}.
     */
    public static File createNewFile(File file) throws IOException {
      assertTrue("Unable to create new file " + file, file.createNewFile());
      return file;
    }

    /**
     * Invokes {@link File#mkdir()} on the given {@link File} instance.
     *
     * @param file The file to call {@link File#mkdir()} on.
     * @return The result of {@link File#mkdir()}.
     */
    public static File mkdir(File file) {
      assertTrue("Unable to mkdir for " + file, file.mkdir());
      return file;
    }

    /**
     * Invokes {@link File#mkdirs()} on the given {@link File} instance.
     *
     * @param file The file to call {@link File#mkdirs()} on.
     * @return The result of {@link File#mkdirs()}.
     */
    public static File mkdirs(File file) {
      assertTrue("Unable to mkdirs for " + file, file.mkdirs());
      return file;
    }

    /**
     * Invokes {@link File#delete()} on the given {@link File} instance.
     *
     * @param file The file to call {@link File#delete()} on.
     * @return The result of {@link File#delete()}.
     */
    public static File delete(File file) {
      assertTrue("Unable to delete " + file, file.delete());
      return file;
    }

    /**
     * Invokes {@link File#exists()} on the given {@link File} instance.
     *
     * @param file The file to call {@link File#exists()} on.
     * @return The result of {@link File#exists()}.
     */
    public static File exists(File file) {
      assertTrue("Expected file " + file + " doesn't exist", file.exists());
      return file;
    }

    /**
     * Invokes {@link File#exists()} on the given {@link File} instance to check if the
     * {@link File} doesn't exists.
     *
     * @param file The file to call {@link File#exists()} on.
     * @return The negation of the result of {@link File#exists()}.
     */
    public static File notExists(File file) {
      assertFalse("Expected file " + file + " must not exist", file.exists());
      return file;
    }
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
      if (getName().equals(FILE_1_NAME)) {
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
      final File[] files = super.listFiles();
      if (files == null) {
         return null;
      }
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

  @Test (timeout = 30000)
  public void testFailFullyDeleteContents() throws IOException {
    // Windows Dir.setWritable(false) does not work for directories
    assumeNotWindows();
    LOG.info("Running test to verify failure of fullyDeleteContents()");
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(new MyFile(del));
    validateAndSetWritablePermissions(true, ret);
  }

  @Test (timeout = 30000)
  public void testFailFullyDeleteContentsGrantPermissions() throws IOException {
    setupDirsAndNonWritablePermissions();
    boolean ret = FileUtil.fullyDeleteContents(new MyFile(del), true);
    // this time the directories with revoked permissions *should* be deleted:
    validateAndSetWritablePermissions(false, ret);
  }

  /**
   * Test that getDU is able to handle cycles caused due to symbolic links
   * and that directory sizes are not added to the final calculated size
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testGetDU() throws Exception {
    long du = FileUtil.getDU(testFolder.getRoot());
    // Only two files (in partitioned).  Each has 3 characters + system-specific
    // line separator.
    final long expected = 2 * (3 + System.getProperty("line.separator").length());
    Assert.assertEquals(expected, du);
    
    // target file does not exist:
    final File doesNotExist = new File(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
    long duDoesNotExist = FileUtil.getDU(doesNotExist);
    assertEquals(0, duDoesNotExist);
    
    // target file is not a directory:
    File notADirectory = new File(partitioned, "part-r-00000");
    long duNotADirectoryActual = FileUtil.getDU(notADirectory);
    long duNotADirectoryExpected = 3 + System.getProperty("line.separator").length();
    assertEquals(duNotADirectoryExpected, duNotADirectoryActual);
    
    try {
      // one of target files is not accessible, but the containing directory
      // is accessible:
      try {
        FileUtil.chmod(notADirectory.getAbsolutePath(), "0000");
      } catch (InterruptedException ie) {
        // should never happen since that method never throws InterruptedException.      
        assertNull(ie);  
      }
      assertFalse(FileUtil.canRead(notADirectory));
      final long du3 = FileUtil.getDU(partitioned);
      assertEquals(expected, du3);

      // some target files and containing directory are not accessible:
      try {
        FileUtil.chmod(partitioned.getAbsolutePath(), "0000");
      } catch (InterruptedException ie) {
        // should never happen since that method never throws InterruptedException.      
        assertNull(ie);  
      }
      assertFalse(FileUtil.canRead(partitioned));
      final long du4 = FileUtil.getDU(partitioned);
      assertEquals(0, du4);
    } finally {
      // Restore the permissions so that we can delete the folder 
      // in @After method:
      FileUtil.chmod(partitioned.getAbsolutePath(), "0777", true/*recursive*/);
    }
  }

  @Test (timeout = 30000)
  public void testUnTar() throws Exception {
    // make a simple tar:
    final File simpleTar = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleTar);
    try (TarOutputStream tos = new TarOutputStream(os)) {
      TarEntry te = new TarEntry("/bar/foo");
      byte[] data = "some-content".getBytes("UTF-8");
      te.setSize(data.length);
      tos.putNextEntry(te);
      tos.write(data);
      tos.closeEntry();
      tos.flush();
      tos.finish();
    }

    // successfully untar it into an existing dir:
    FileUtil.unTar(simpleTar, tmp);
    // check result:
    Verify.exists(new File(tmp, "/bar/foo"));
    assertEquals(12, new File(tmp, "/bar/foo").length());

    final File regularFile =
        Verify.createNewFile(new File(tmp, "QuickBrownFoxJumpsOverTheLazyDog"));
    LambdaTestUtils.intercept(IOException.class, () -> FileUtil.unTar(simpleTar, regularFile));
  }
  
  @Test (timeout = 30000)
  public void testReplaceFile() throws IOException {
    // src exists, and target does not exist:
    final File srcFile = Verify.createNewFile(new File(tmp, "src"));
    final File targetFile = new File(tmp, "target");
    Verify.notExists(targetFile);
    FileUtil.replaceFile(srcFile, targetFile);
    Verify.notExists(srcFile);
    Verify.exists(targetFile);

    // src exists and target is a regular file: 
    Verify.createNewFile(srcFile);
    Verify.exists(srcFile);
    FileUtil.replaceFile(srcFile, targetFile);
    Verify.notExists(srcFile);
    Verify.exists(targetFile);
    
    // src exists, and target is a non-empty directory: 
    Verify.createNewFile(srcFile);
    Verify.exists(srcFile);
    Verify.delete(targetFile);
    Verify.mkdirs(targetFile);
    File obstacle = Verify.createNewFile(new File(targetFile, "obstacle"));
    assertTrue(targetFile.exists() && targetFile.isDirectory());
    try {
      FileUtil.replaceFile(srcFile, targetFile);
      assertTrue(false);
    } catch (IOException ioe) {
      // okay
    }
    // check up the post-condition: nothing is deleted:
    Verify.exists(srcFile);
    assertTrue(targetFile.exists() && targetFile.isDirectory());
    Verify.exists(obstacle);
  }
  
  @Test (timeout = 30000)
  public void testCreateLocalTempFile() throws IOException {
    final File baseFile = new File(tmp, "base");
    File tmp1 = FileUtil.createLocalTempFile(baseFile, "foo", false);
    File tmp2 = FileUtil.createLocalTempFile(baseFile, "foo", true);
    assertFalse(tmp1.getAbsolutePath().equals(baseFile.getAbsolutePath()));
    assertFalse(tmp2.getAbsolutePath().equals(baseFile.getAbsolutePath()));
    assertTrue(tmp1.exists() && tmp2.exists());
    assertTrue(tmp1.canWrite() && tmp2.canWrite());
    assertTrue(tmp1.canRead() && tmp2.canRead());
    Verify.delete(tmp1);
    Verify.delete(tmp2);
    assertTrue(!tmp1.exists() && !tmp2.exists());
  }
  
  @Test (timeout = 30000)
  public void testUnZip() throws Exception {
    // make sa simple zip
    final File simpleZip = new File(del, FILE);
    try (OutputStream os = new FileOutputStream(simpleZip);
         ZipArchiveOutputStream tos = new ZipArchiveOutputStream(os)) {
      List<ZipArchiveEntry> ZipArchiveList = new ArrayList<>(7);
      int count = 0;
      // create 7 files to verify permissions
      for (int i = 0; i < 7; i++) {
        ZipArchiveList.add(new ZipArchiveEntry("foo_" + i));
        ZipArchiveEntry archiveEntry = ZipArchiveList.get(i);
        archiveEntry.setUnixMode(count += 0100);
        byte[] data = "some-content".getBytes("UTF-8");
        archiveEntry.setSize(data.length);
        tos.putArchiveEntry(archiveEntry);
        tos.write(data);
      }
      tos.closeArchiveEntry();
      tos.flush();
      tos.finish();
    }

    // successfully unzip it into an existing dir:
    FileUtil.unZip(simpleZip, tmp);
    File foo0 = new File(tmp, "foo_0");
    File foo1 = new File(tmp, "foo_1");
    File foo2 = new File(tmp, "foo_2");
    File foo3 = new File(tmp, "foo_3");
    File foo4 = new File(tmp, "foo_4");
    File foo5 = new File(tmp, "foo_5");
    File foo6 = new File(tmp, "foo_6");
    // check result:
    assertTrue(foo0.exists());
    assertTrue(foo1.exists());
    assertTrue(foo2.exists());
    assertTrue(foo3.exists());
    assertTrue(foo4.exists());
    assertTrue(foo5.exists());
    assertTrue(foo6.exists());
    assertEquals(12, foo0.length());
    // tests whether file foo_0 has executable permissions
    assertTrue("file lacks execute permissions", foo0.canExecute());
    assertFalse("file has write permissions", foo0.canWrite());
    assertFalse("file has read permissions", foo0.canRead());
    // tests whether file foo_1 has writable permissions
    assertFalse("file has execute permissions", foo1.canExecute());
    assertTrue("file lacks write permissions", foo1.canWrite());
    assertFalse("file has read permissions", foo1.canRead());
    // tests whether file foo_2 has executable and writable permissions
    assertTrue("file lacks execute permissions", foo2.canExecute());
    assertTrue("file lacks write permissions", foo2.canWrite());
    assertFalse("file has read permissions", foo2.canRead());
    // tests whether file foo_3 has readable permissions
    assertFalse("file has execute permissions", foo3.canExecute());
    assertFalse("file has write permissions", foo3.canWrite());
    assertTrue("file lacks read permissions", foo3.canRead());
    // tests whether file foo_4 has readable and executable permissions
    assertTrue("file lacks execute permissions", foo4.canExecute());
    assertFalse("file has write permissions", foo4.canWrite());
    assertTrue("file lacks read permissions", foo4.canRead());
    // tests whether file foo_5 has readable and writable permissions
    assertFalse("file has execute permissions", foo5.canExecute());
    assertTrue("file lacks write permissions", foo5.canWrite());
    assertTrue("file lacks read permissions", foo5.canRead());
    // tests whether file foo_6 has readable, writable and executable permissions
    assertTrue("file lacks execute permissions", foo6.canExecute());
    assertTrue("file lacks write permissions", foo6.canWrite());
    assertTrue("file lacks read permissions", foo6.canRead());

    final File regularFile =
        Verify.createNewFile(new File(tmp, "QuickBrownFoxJumpsOverTheLazyDog"));
    LambdaTestUtils.intercept(IOException.class, () -> FileUtil.unZip(simpleZip, regularFile));
  }

  @Test (timeout = 30000)
  public void testUnZip2() throws IOException {
    // make a simple zip
    final File simpleZip = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleZip);
    try (ZipArchiveOutputStream tos = new ZipArchiveOutputStream(os)) {
      // Add an entry that contains invalid filename
      ZipArchiveEntry ze = new ZipArchiveEntry("../foo");
      byte[] data = "some-content".getBytes(StandardCharsets.UTF_8);
      ze.setSize(data.length);
      tos.putArchiveEntry(ze);
      tos.write(data);
      tos.closeArchiveEntry();
      tos.flush();
      tos.finish();
    }

    // Unzip it into an existing dir
    try {
      FileUtil.unZip(simpleZip, tmp);
      fail("unZip should throw IOException.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "would create file outside of", e);
    }
  }

  @Test (timeout = 30000)
  /*
   * Test method copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)
   */
  public void testCopy5() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.newInstance(uri, conf);
    final String content = "some-content";
    File srcFile = createFile(tmp, "src", content);
    Path srcPath = new Path(srcFile.toURI());
    
    // copy regular file:
    final File dest = new File(del, "dest");
    boolean result = FileUtil.copy(fs, srcPath, dest, false, conf);
    assertTrue(result);
    Verify.exists(dest);
    assertEquals(content.getBytes().length 
        + System.getProperty("line.separator").getBytes().length, dest.length());
    Verify.exists(srcFile); // should not be deleted
    
    // copy regular file, delete src:
    Verify.delete(dest);
    Verify.notExists(dest);
    result = FileUtil.copy(fs, srcPath, dest, true, conf);
    assertTrue(result);
    Verify.exists(dest);
    assertEquals(content.getBytes().length 
        + System.getProperty("line.separator").getBytes().length, dest.length());
    Verify.notExists(srcFile); // should be deleted
    
    // copy a dir:
    Verify.delete(dest);
    Verify.notExists(dest);
    srcPath = new Path(partitioned.toURI());
    result = FileUtil.copy(fs, srcPath, dest, true, conf);
    assertTrue(result);
    assertTrue(dest.exists() && dest.isDirectory());
    File[] files = dest.listFiles();
    assertTrue(files != null);
    assertEquals(2, files.length);
    for (File f: files) {
      assertEquals(3 
          + System.getProperty("line.separator").getBytes().length, f.length());
    }
    Verify.notExists(partitioned); // should be deleted
  }  

  @Test (timeout = 30000)
  public void testStat2Paths1() {
    assertNull(FileUtil.stat2Paths(null));
    
    FileStatus[] fileStatuses = new FileStatus[0]; 
    Path[] paths = FileUtil.stat2Paths(fileStatuses);
    assertEquals(0, paths.length);
    
    Path path1 = new Path("file://foo");
    Path path2 = new Path("file://moo");
    fileStatuses = new FileStatus[] { 
        new FileStatus(3, false, 0, 0, 0, path1), 
        new FileStatus(3, false, 0, 0, 0, path2) 
        };
    paths = FileUtil.stat2Paths(fileStatuses);
    assertEquals(2, paths.length);
    assertEquals(paths[0], path1);
    assertEquals(paths[1], path2);
  }
  
  @Test (timeout = 30000)
  public void testStat2Paths2()  {
    Path defaultPath = new Path("file://default");
    Path[] paths = FileUtil.stat2Paths(null, defaultPath);
    assertEquals(1, paths.length);
    assertEquals(defaultPath, paths[0]);

    paths = FileUtil.stat2Paths(null, null);
    assertTrue(paths != null);
    assertEquals(1, paths.length);
    assertEquals(null, paths[0]);
    
    Path path1 = new Path("file://foo");
    Path path2 = new Path("file://moo");
    FileStatus[] fileStatuses = new FileStatus[] { 
        new FileStatus(3, false, 0, 0, 0, path1), 
        new FileStatus(3, false, 0, 0, 0, path2) 
        };
    paths = FileUtil.stat2Paths(fileStatuses, defaultPath);
    assertEquals(2, paths.length);
    assertEquals(paths[0], path1);
    assertEquals(paths[1], path2);
  }

  @Test (timeout = 30000)
  public void testSymlink() throws Exception {
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
  @Test (timeout = 30000)
  public void testSymlinkRenameTo() throws Exception {
    File file = new File(del, FILE);
    file.createNewFile();
    File link = new File(del, "_link");

    // create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Verify.exists(file);
    Verify.exists(link);

    File link2 = new File(del, "_link2");

    // Rename the symlink
    Assert.assertTrue(link.renameTo(link2));

    // Make sure the file still exists
    // (NOTE: this would fail on Java6 on Windows if we didn't
    // copy the file in FileUtil#symlink)
    Verify.exists(file);

    Verify.exists(link2);
    Verify.notExists(link);
  }

  /**
   * Test that deletion of a symlink works as expected.
   */
  @Test (timeout = 30000)
  public void testSymlinkDelete() throws Exception {
    File file = new File(del, FILE);
    file.createNewFile();
    File link = new File(del, "_link");

    // create the symlink
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Verify.exists(file);
    Verify.exists(link);

    // make sure that deleting a symlink works properly
    Verify.delete(link);
    Verify.notExists(link);
    Verify.exists(file);
  }

  /**
   * Test that length on a symlink works as expected.
   */
  @Test (timeout = 30000)
  public void testSymlinkLength() throws Exception {
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

    Verify.delete(file);
    Verify.notExists(file);

    Assert.assertEquals(0, link.length());

    Verify.delete(link);
    Verify.notExists(link);
  }

  /**
   * This test validates the correctness of
   * {@link FileUtil#symLink(String, String)} in case of null pointer inputs.
   *
   * @throws IOException
   */
  @Test
  public void testSymlinkWithNullInput() throws IOException {
    File file = new File(del, FILE);
    File link = new File(del, "_link");

    // Create the same symbolic link
    // The operation should fail and returns 1
    int result = FileUtil.symLink(null, null);
    Assert.assertEquals(1, result);

    // Create the same symbolic link
    // The operation should fail and returns 1
    result = FileUtil.symLink(file.getAbsolutePath(), null);
    Assert.assertEquals(1, result);

    // Create the same symbolic link
    // The operation should fail and returns 1
    result = FileUtil.symLink(null, link.getAbsolutePath());
    Assert.assertEquals(1, result);
  }

  /**
   * This test validates the correctness of
   * {@link FileUtil#symLink(String, String)} in case the file already exists.
   *
   * @throws IOException
   */
  @Test
  public void testSymlinkFileAlreadyExists() throws IOException {
    File file = new File(del, FILE);
    File link = new File(del, "_link");

    // Create a symbolic link
    // The operation should succeed
    int result1 =
        FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertEquals(0, result1);

    // Create the same symbolic link
    // The operation should fail and returns 1
    result1 = FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertEquals(1, result1);
  }

  /**
   * This test validates the correctness of
   * {@link FileUtil#symLink(String, String)} in case the file and the link are
   * the same file.
   *
   * @throws IOException
   */
  @Test
  public void testSymlinkSameFile() throws IOException {
    File file = new File(del, FILE);

    Verify.delete(file);

    // Create a symbolic link
    // The operation should succeed
    int result =
        FileUtil.symLink(file.getAbsolutePath(), file.getAbsolutePath());

    Assert.assertEquals(0, result);
  }

  /**
   * This test validates the correctness of
   * {@link FileUtil#symLink(String, String)} in case we want to use a link for
   * 2 different files.
   *
   * @throws IOException
   */
  @Test
  public void testSymlink2DifferentFile() throws IOException {
    File file = new File(del, FILE);
    File fileSecond = new File(del, FILE + "_1");
    File link = new File(del, "_link");

    // Create a symbolic link
    // The operation should succeed
    int result =
        FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertEquals(0, result);

    // The operation should fail and returns 1
    result =
        FileUtil.symLink(fileSecond.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertEquals(1, result);
  }

  /**
   * This test validates the correctness of
   * {@link FileUtil#symLink(String, String)} in case we want to use a 2
   * different links for the same file.
   *
   * @throws IOException
   */
  @Test
  public void testSymlink2DifferentLinks() throws IOException {
    File file = new File(del, FILE);
    File link = new File(del, "_link");
    File linkSecond = new File(del, "_link_1");

    // Create a symbolic link
    // The operation should succeed
    int result =
        FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    Assert.assertEquals(0, result);

    // The operation should succeed
    result =
        FileUtil.symLink(file.getAbsolutePath(), linkSecond.getAbsolutePath());

    Assert.assertEquals(0, result);
  }

  private void doUntarAndVerify(File tarFile, File untarDir) 
                                 throws IOException {
    if (untarDir.exists() && !FileUtil.fullyDelete(untarDir)) {
      throw new IOException("Could not delete directory '" + untarDir + "'");
    }
    FileUtil.unTar(tarFile, untarDir);

    String parentDir = untarDir.getCanonicalPath() + Path.SEPARATOR + "name";
    File testFile = new File(parentDir + Path.SEPARATOR + "version");
    Verify.exists(testFile);
    Assert.assertTrue(testFile.length() == 0);
    String imageDir = parentDir + Path.SEPARATOR + "image";
    testFile = new File(imageDir + Path.SEPARATOR + "fsimage");
    Verify.exists(testFile);
    Assert.assertTrue(testFile.length() == 157);
    String currentDir = parentDir + Path.SEPARATOR + "current";
    testFile = new File(currentDir + Path.SEPARATOR + "fsimage");
    Verify.exists(testFile);
    Assert.assertTrue(testFile.length() == 4331);
    testFile = new File(currentDir + Path.SEPARATOR + "edits");
    Verify.exists(testFile);
    Assert.assertTrue(testFile.length() == 1033);
    testFile = new File(currentDir + Path.SEPARATOR + "fstime");
    Verify.exists(testFile);
    Assert.assertTrue(testFile.length() == 8);
  }

  @Test (timeout = 30000)
  public void testUntar() throws IOException {
    String tarGzFileName = System.getProperty("test.cache.data",
        "target/test/cache") + "/test-untar.tgz";
    String tarFileName = System.getProperty("test.cache.data",
        "build/test/cache") + "/test-untar.tar";
    File dataDir = GenericTestUtils.getTestDir();
    File untarDir = new File(dataDir, "untarDir");

    doUntarAndVerify(new File(tarGzFileName), untarDir);
    doUntarAndVerify(new File(tarFileName), untarDir);
  }

  /**
   * Verify we can't unTar a file which isn't there.
   * This will test different codepaths on Windows from unix,
   * but both MUST throw an IOE of some kind.
   */
  @Test(timeout = 30000)
  public void testUntarMissingFile() throws Throwable {
    File dataDir = GenericTestUtils.getTestDir();
    File tarFile = new File(dataDir, "missing; true");
    File untarDir = new File(dataDir, "untarDir");
    intercept(IOException.class, () ->
        FileUtil.unTar(tarFile, untarDir));
  }

  /**
   * Verify we can't unTar a file which isn't there
   * through the java untar code.
   * This is how {@code FileUtil.unTar(File, File}
   * will behave on Windows,
   */
  @Test(timeout = 30000)
  public void testUntarMissingFileThroughJava() throws Throwable {
    File dataDir = GenericTestUtils.getTestDir();
    File tarFile = new File(dataDir, "missing; true");
    File untarDir = new File(dataDir, "untarDir");
    // java8 on unix throws java.nio.file.NoSuchFileException here;
    // leaving as an IOE intercept in case windows throws something
    // else.
    intercept(IOException.class, () ->
        FileUtil.unTarUsingJava(tarFile, untarDir, false));
  }

  @Test (timeout = 30000)
  public void testCreateJarWithClassPath() throws Exception {
    // create files expected to match a wildcard
    List<File> wildcardMatches = Arrays.asList(new File(tmp, "wildcard1.jar"),
      new File(tmp, "wildcard2.jar"), new File(tmp, "wildcard3.JAR"),
      new File(tmp, "wildcard4.JAR"));
    for (File wildcardMatch: wildcardMatches) {
      Assert.assertTrue("failure creating file: " + wildcardMatch,
        wildcardMatch.createNewFile());
    }

    // create non-jar files, which we expect to not be included in the classpath
    Verify.createNewFile(new File(tmp, "text.txt"));
    Verify.createNewFile(new File(tmp, "executable.exe"));
    Verify.createNewFile(new File(tmp, "README"));

    // create classpath jar
    String wildcardPath = tmp.getCanonicalPath() + File.separator + "*";
    String nonExistentSubdir = tmp.getCanonicalPath() + Path.SEPARATOR + "subdir"
      + Path.SEPARATOR;
    List<String> classPaths = Arrays.asList("", "cp1.jar", "cp2.jar", wildcardPath,
      "cp3.jar", nonExistentSubdir);
    String inputClassPath = StringUtils.join(File.pathSeparator, classPaths);
    String[] jarCp = FileUtil.createJarWithClassPath(inputClassPath + File.pathSeparator + "unexpandedwildcard/*",
      new Path(tmp.getCanonicalPath()), System.getenv());
    String classPathJar = jarCp[0];
    assertNotEquals("Unexpanded wildcard was not placed in extra classpath", jarCp[1].indexOf("unexpanded"), -1);

    // verify classpath by reading manifest from jar file
    JarFile jarFile = null;
    try {
      jarFile = new JarFile(classPathJar);
      Manifest jarManifest = jarFile.getManifest();
      Assert.assertNotNull(jarManifest);
      Attributes mainAttributes = jarManifest.getMainAttributes();
      Assert.assertNotNull(mainAttributes);
      Assert.assertTrue(mainAttributes.containsKey(Attributes.Name.CLASS_PATH));
      String classPathAttr = mainAttributes.getValue(Attributes.Name.CLASS_PATH);
      Assert.assertNotNull(classPathAttr);
      List<String> expectedClassPaths = new ArrayList<String>();
      for (String classPath: classPaths) {
        if (classPath.length() == 0) {
          continue;
        }
        if (wildcardPath.equals(classPath)) {
          // add wildcard matches
          for (File wildcardMatch: wildcardMatches) {
            expectedClassPaths.add(wildcardMatch.getCanonicalFile().toURI().toURL()
              .toExternalForm());
          }
        } else {
          File fileCp = null;
          if(!new Path(classPath).isAbsolute()) {
            fileCp = new File(tmp, classPath).getCanonicalFile();
          }
          else {
            fileCp = new File(classPath).getCanonicalFile();
          }
          if (nonExistentSubdir.equals(classPath)) {
            // expect to maintain trailing path separator if present in input, even
            // if directory doesn't exist yet
            expectedClassPaths.add(fileCp.toURI().toURL()
              .toExternalForm() + Path.SEPARATOR);
          } else {
            expectedClassPaths.add(fileCp.toURI().toURL()
              .toExternalForm());
          }
        }
      }
      List<String> actualClassPaths = Arrays.asList(classPathAttr.split(" "));
      Collections.sort(expectedClassPaths);
      Collections.sort(actualClassPaths);
      Assert.assertEquals(expectedClassPaths, actualClassPaths);
    } finally {
      if (jarFile != null) {
        try {
          jarFile.close();
        } catch (IOException e) {
          LOG.warn("exception closing jarFile: " + classPathJar, e);
        }
      }
    }
  }

  @Test
  public void testGetJarsInDirectory() throws Exception {
    List<Path> jars = FileUtil.getJarsInDirectory("/foo/bar/bogus/");
    assertTrue("no jars should be returned for a bogus path",
        jars.isEmpty());


    // create jar files to be returned
    File jar1 = new File(tmp, "wildcard1.jar");
    File jar2 = new File(tmp, "wildcard2.JAR");
    List<File> matches = Arrays.asList(jar1, jar2);
    for (File match: matches) {
      assertTrue("failure creating file: " + match, match.createNewFile());
    }

    // create non-jar files, which we expect to not be included in the result
    Verify.createNewFile(new File(tmp, "text.txt"));
    Verify.createNewFile(new File(tmp, "executable.exe"));
    Verify.createNewFile(new File(tmp, "README"));

    // pass in the directory
    String directory = tmp.getCanonicalPath();
    jars = FileUtil.getJarsInDirectory(directory);
    assertEquals("there should be 2 jars", 2, jars.size());
    for (Path jar: jars) {
      URL url = jar.toUri().toURL();
      assertTrue("the jar should match either of the jars",
          url.equals(jar1.getCanonicalFile().toURI().toURL()) ||
          url.equals(jar2.getCanonicalFile().toURI().toURL()));
    }
  }

  @Ignore
  public void setupCompareFs() {
    // Set up Strings
    String host1 = "1.2.3.4";
    String host2 = "2.3.4.5";
    int port1 = 7000;
    int port2 = 7001;
    String uris1 = "hdfs://" + host1 + ":" + Integer.toString(port1) + "/tmp/foo";
    String uris2 = "hdfs://" + host1 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris3 = "hdfs://" + host2 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris4 = "hdfs://" + host2 + ":" + Integer.toString(port2) + "/tmp/foo";
    String uris5 = "file:///" + host1 + ":" + Integer.toString(port1) + "/tmp/foo";
    String uris6 = "hdfs:///" + host1 + "/tmp/foo";
    // Set up URI objects
    try {
      uri1 = new URI(uris1);
      uri2 = new URI(uris2);
      uri3 = new URI(uris3);
      uri4 = new URI(uris4);
      uri5 = new URI(uris5);
      uri6 = new URI(uris6);
    } catch (URISyntaxException ignored) {
    }
    // Set up InetAddress
    inet1 = mock(InetAddress.class);
    when(inet1.getCanonicalHostName()).thenReturn(host1);
    inet2 = mock(InetAddress.class);
    when(inet2.getCanonicalHostName()).thenReturn(host1);
    inet3 = mock(InetAddress.class);
    when(inet3.getCanonicalHostName()).thenReturn(host2);
    inet4 = mock(InetAddress.class);
    when(inet4.getCanonicalHostName()).thenReturn(host2);
    inet5 = mock(InetAddress.class);
    when(inet5.getCanonicalHostName()).thenReturn(host1);
    inet6 = mock(InetAddress.class);
    when(inet6.getCanonicalHostName()).thenReturn(host1);

    // Link of InetAddress to corresponding URI
    try {
      when(InetAddress.getByName(uris1)).thenReturn(inet1);
      when(InetAddress.getByName(uris2)).thenReturn(inet2);
      when(InetAddress.getByName(uris3)).thenReturn(inet3);
      when(InetAddress.getByName(uris4)).thenReturn(inet4);
      when(InetAddress.getByName(uris5)).thenReturn(inet5);
    } catch (UnknownHostException ignored) {
    }

    fs1 = mock(FileSystem.class);
    when(fs1.getUri()).thenReturn(uri1);
    fs2 = mock(FileSystem.class);
    when(fs2.getUri()).thenReturn(uri2);
    fs3 = mock(FileSystem.class);
    when(fs3.getUri()).thenReturn(uri3);
    fs4 = mock(FileSystem.class);
    when(fs4.getUri()).thenReturn(uri4);
    fs5 = mock(FileSystem.class);
    when(fs5.getUri()).thenReturn(uri5);
    fs6 = mock(FileSystem.class);
    when(fs6.getUri()).thenReturn(uri6);
  }

  @Test
  public void testCompareFsNull() throws Exception {
    setupCompareFs();
    assertFalse(FileUtil.compareFs(null, fs1));
    assertFalse(FileUtil.compareFs(fs1, null));
  }

  @Test
  public void testCompareFsDirectories() throws Exception {
    setupCompareFs();
    assertTrue(FileUtil.compareFs(fs1, fs1));
    assertFalse(FileUtil.compareFs(fs1, fs2));
    assertFalse(FileUtil.compareFs(fs1, fs5));
    assertTrue(FileUtil.compareFs(fs3, fs4));
    assertFalse(FileUtil.compareFs(fs1, fs6));
  }

  @Test(timeout = 8000)
  public void testCreateSymbolicLinkUsingJava() throws IOException {
    final File simpleTar = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleTar);
    try (TarArchiveOutputStream tos = new TarArchiveOutputStream(os)) {
      // Files to tar
      final String tmpDir = "tmp/test";
      File tmpDir1 = new File(tmpDir, "dir1/");
      File tmpDir2 = new File(tmpDir, "dir2/");
      Verify.mkdirs(tmpDir1);
      Verify.mkdirs(tmpDir2);

      java.nio.file.Path symLink = Paths.get(tmpDir1.getPath(), "sl");

      // Create Symbolic Link
      Files.createSymbolicLink(symLink, Paths.get(tmpDir2.getPath()));
      assertTrue(Files.isSymbolicLink(symLink.toAbsolutePath()));
      // Put entries in tar file
      putEntriesInTar(tos, tmpDir1.getParentFile());
      tos.close();

      File untarFile = new File(tmpDir, "2");
      // Untar using Java
      FileUtil.unTarUsingJava(simpleTar, untarFile, false);

      // Check symbolic link and other directories are there in untar file
      assertTrue(Files.exists(untarFile.toPath()));
      assertTrue(Files.exists(Paths.get(untarFile.getPath(), tmpDir)));
      assertTrue(Files.isSymbolicLink(Paths.get(untarFile.getPath(), symLink.toString())));
    } finally {
      FileUtils.deleteDirectory(new File("tmp"));
    }
  }

  @Test(expected = IOException.class)
  public void testCreateArbitrarySymlinkUsingJava() throws IOException {
    final File simpleTar = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleTar);

    File rootDir = new File("tmp");
    try (TarArchiveOutputStream tos = new TarArchiveOutputStream(os)) {
      tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

      // Create arbitrary dir
      File arbitraryDir = new File(rootDir, "arbitrary-dir/");
      Verify.mkdirs(arbitraryDir);

      // We will tar from the tar-root lineage
      File tarRoot = new File(rootDir, "tar-root/");
      File symlinkRoot = new File(tarRoot, "dir1/");
      Verify.mkdirs(symlinkRoot);

      // Create Symbolic Link to an arbitrary dir
      java.nio.file.Path symLink = Paths.get(symlinkRoot.getPath(), "sl");
      Files.createSymbolicLink(symLink, arbitraryDir.toPath().toAbsolutePath());

      // Put entries in tar file
      putEntriesInTar(tos, tarRoot);
      putEntriesInTar(tos, new File(symLink.toFile(), "dir-outside-tar-root/"));
      tos.close();

      // Untar using Java
      File untarFile = new File(rootDir, "extracted");
      FileUtil.unTarUsingJava(simpleTar, untarFile, false);
    } finally {
      FileUtils.deleteDirectory(rootDir);
    }
  }

  private void putEntriesInTar(TarArchiveOutputStream tos, File f)
      throws IOException {
    if (Files.isSymbolicLink(f.toPath())) {
      TarArchiveEntry tarEntry = new TarArchiveEntry(f.getPath(),
          TarArchiveEntry.LF_SYMLINK);
      tarEntry.setLinkName(Files.readSymbolicLink(f.toPath()).toString());
      tos.putArchiveEntry(tarEntry);
      tos.closeArchiveEntry();
      return;
    }

    if (f.isDirectory()) {
      tos.putArchiveEntry(new TarArchiveEntry(f));
      tos.closeArchiveEntry();
      for (File child : f.listFiles()) {
        putEntriesInTar(tos, child);
      }
    }

    if (f.isFile()) {
      tos.putArchiveEntry(new TarArchiveEntry(f));
      BufferedInputStream origin = new BufferedInputStream(
          new FileInputStream(f));
      int count;
      byte[] data = new byte[2048];
      while ((count = origin.read(data)) != -1) {
        tos.write(data, 0, count);
      }
      tos.flush();
      tos.closeArchiveEntry();
      origin.close();
    }
  }

  /**
   * This test validates the correctness of {@link FileUtil#readLink(File)} in
   * case of null pointer inputs.
   */
  @Test
  public void testReadSymlinkWithNullInput() {
    String result = FileUtil.readLink(null);
    Assert.assertEquals("", result);
  }

  /**
   * This test validates the correctness of {@link FileUtil#readLink(File)}.
   *
   * @throws IOException
   */
  @Test
  public void testReadSymlink() throws IOException {
    File file = new File(del, FILE);
    File link = new File(del, "_link");

    // Create a symbolic link
    FileUtil.symLink(file.getAbsolutePath(), link.getAbsolutePath());

    String result = FileUtil.readLink(link);
    Assert.assertEquals(file.getAbsolutePath(), result);
  }

  @Test
  public void testRegularFile() throws IOException {
    byte[] data = "testRegularData".getBytes();
    File tmpFile = new File(del, "reg1");

    // write some data to the file
    FileOutputStream os = new FileOutputStream(tmpFile);
    os.write(data);
    os.close();
    assertTrue(FileUtil.isRegularFile(tmpFile));

    // create a symlink to file
    File link = new File(del, "reg2");
    FileUtil.symLink(tmpFile.toString(), link.toString());
    assertFalse(FileUtil.isRegularFile(link, false));
  }

  /**
   * This test validates the correctness of {@link FileUtil#readLink(File)} when
   * it gets a file in input.
   *
   * @throws IOException
   */
  @Test
  public void testReadSymlinkWithAFileAsInput() throws IOException {
    File file = new File(del, FILE);

    String result = FileUtil.readLink(file);
    Assert.assertEquals("", result);

    Verify.delete(file);
  }

  /**
   * Test that bytes are written out correctly to the local file system.
   */
  @Test
  public void testWriteBytesFileSystem() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path testPath = new Path(new Path(uri), "writebytes.out");

    byte[] write = new byte[] {0x00, 0x01, 0x02, 0x03};

    FileUtil.write(fs, testPath, write);

    byte[] read = FileUtils.readFileToByteArray(new File(testPath.toUri()));

    assertArrayEquals(write, read);
  }

  /**
   * Test that a Collection of Strings are written out correctly to the local
   * file system.
   */
  @Test
  public void testWriteStringsFileSystem() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path testPath = new Path(new Path(uri), "writestrings.out");

    Collection<String> write = Arrays.asList("over", "the", "lazy", "dog");

    FileUtil.write(fs, testPath, write, StandardCharsets.UTF_8);

    List<String> read =
        FileUtils.readLines(new File(testPath.toUri()), StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * Test that a String is written out correctly to the local file system.
   */
  @Test
  public void testWriteStringFileSystem() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path testPath = new Path(new Path(uri), "writestring.out");

    String write = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";

    FileUtil.write(fs, testPath, write, StandardCharsets.UTF_8);

    String read = FileUtils.readFileToString(new File(testPath.toUri()),
        StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * Test that a String is written out correctly to the local file system
   * without specifying a character set.
   */
  @Test
  public void testWriteStringNoCharSetFileSystem() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(uri, conf);
    Path testPath = new Path(new Path(uri), "writestring.out");

    String write = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
    FileUtil.write(fs, testPath, write);

    String read = FileUtils.readFileToString(new File(testPath.toUri()),
        StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * Test that bytes are written out correctly to the local file system.
   */
  @Test
  public void testWriteBytesFileContext() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileContext fc = FileContext.getFileContext(uri, conf);
    Path testPath = new Path(new Path(uri), "writebytes.out");

    byte[] write = new byte[] {0x00, 0x01, 0x02, 0x03};

    FileUtil.write(fc, testPath, write);

    byte[] read = FileUtils.readFileToByteArray(new File(testPath.toUri()));

    assertArrayEquals(write, read);
  }

  /**
   * Test that a Collection of Strings are written out correctly to the local
   * file system.
   */
  @Test
  public void testWriteStringsFileContext() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileContext fc = FileContext.getFileContext(uri, conf);
    Path testPath = new Path(new Path(uri), "writestrings.out");

    Collection<String> write = Arrays.asList("over", "the", "lazy", "dog");

    FileUtil.write(fc, testPath, write, StandardCharsets.UTF_8);

    List<String> read =
        FileUtils.readLines(new File(testPath.toUri()), StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * Test that a String is written out correctly to the local file system.
   */
  @Test
  public void testWriteStringFileContext() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileContext fc = FileContext.getFileContext(uri, conf);
    Path testPath = new Path(new Path(uri), "writestring.out");

    String write = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";

    FileUtil.write(fc, testPath, write, StandardCharsets.UTF_8);

    String read = FileUtils.readFileToString(new File(testPath.toUri()),
        StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * Test that a String is written out correctly to the local file system
   * without specifying a character set.
   */
  @Test
  public void testWriteStringNoCharSetFileContext() throws IOException {
    URI uri = tmp.toURI();
    Configuration conf = new Configuration();
    FileContext fc = FileContext.getFileContext(uri, conf);
    Path testPath = new Path(new Path(uri), "writestring.out");

    String write = "A" + "\u00ea" + "\u00f1" + "\u00fc" + "C";
    FileUtil.write(fc, testPath, write);

    String read = FileUtils.readFileToString(new File(testPath.toUri()),
        StandardCharsets.UTF_8);

    assertEquals(write, read);
  }

  /**
   * The size of FileSystem cache.
   */
  public static int getCacheSize() {
    return FileSystem.cacheSize();
  }
}
