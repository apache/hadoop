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

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileUtil.class);

  private static final File TEST_DIR = GenericTestUtils.getTestDir("fu");
  private static final String FILE = "x";
  private static final String LINK = "y";
  private static final String DIR = "dir";
  private final File del = new File(TEST_DIR, "del");
  private final File tmp = new File(TEST_DIR, "tmp");
  private final File dir1 = new File(del, DIR + "1");
  private final File dir2 = new File(del, DIR + "2");
  private final File partitioned = new File(TEST_DIR, "partitioned");

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
  @Ignore
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

    // create a cycle using symlinks. Cycles should be handled
    FileUtil.symLink(del.toString(), dir1.toString() + "/cycle");
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
    PrintWriter pw = new PrintWriter(newFile);
    try {
      pw.println(contents);
    }
    finally {
      pw.close();
    }
    return newFile;
  }

  @Test (timeout = 30000)
  public void testListFiles() throws IOException {
    setupDirs();
    //Test existing files case 
    File[] files = FileUtil.listFiles(partitioned);
    Assert.assertEquals(2, files.length);

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

  @Test (timeout = 30000)
  public void testListAPI() throws IOException {
    setupDirs();
    //Test existing files case 
    String[] files = FileUtil.list(partitioned);
    Assert.assertEquals("Unexpected number of pre-existing files", 2, files.length);

    //Test existing directory with no files case 
    File newDir = new File(tmp.getPath(),"test");
    newDir.mkdir();
    Assert.assertTrue("Failed to create test dir", newDir.exists());
    files = FileUtil.list(newDir);
    Assert.assertEquals("New directory unexpectedly contains files", 0, files.length);
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

  @Before
  public void before() throws IOException {
    cleanupImpl();
  }
  
  @After
  public void tearDown() throws IOException {
    cleanupImpl();
  }
  
  private void cleanupImpl() throws IOException  {
    FileUtil.fullyDelete(del, true);
    Assert.assertTrue(!del.exists());
    
    FileUtil.fullyDelete(tmp, true);
    Assert.assertTrue(!tmp.exists());
    
    FileUtil.fullyDelete(partitioned, true);
    Assert.assertTrue(!partitioned.exists());
  }

  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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

  @Test (timeout = 30000)
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

  private final File xSubDir = new File(del, "xSubDir");
  private final File xSubSubDir = new File(xSubDir, "xSubSubDir");
  private final File ySubDir = new File(del, "ySubDir");
  private static final String file1Name = "file1";
  private final File file2 = new File(xSubDir, "file2");
  private final File file22 = new File(xSubSubDir, "file22");
  private final File file3 = new File(ySubDir, "file3");
  private final File zlink = new File(del, "zlink");
  
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
    Assert.assertFalse("The directory del should not have existed!",
        del.exists());
    del.mkdirs();
    new MyFile(del, file1Name).createNewFile();

    // "file1" is non-deletable by default, see MyFile.delete().

    xSubDir.mkdirs();
    file2.createNewFile();
    
    xSubSubDir.mkdirs();
    file22.createNewFile();
    
    revokePermissions(file22);
    revokePermissions(xSubSubDir);
    
    revokePermissions(file2);
    revokePermissions(xSubDir);
    
    ySubDir.mkdirs();
    file3.createNewFile();

    Assert.assertFalse("The directory tmp should not have existed!",
        tmp.exists());
    tmp.mkdirs();
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
        new File(del, file1Name).exists());
    
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
    setupDirs();

    long du = FileUtil.getDU(TEST_DIR);
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
  public void testUnTar() throws IOException {
    setupDirs();
    
    // make a simple tar:
    final File simpleTar = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleTar); 
    TarOutputStream tos = new TarOutputStream(os);
    try {
      TarEntry te = new TarEntry("/bar/foo");
      byte[] data = "some-content".getBytes("UTF-8");
      te.setSize(data.length);
      tos.putNextEntry(te);
      tos.write(data);
      tos.closeEntry();
      tos.flush();
      tos.finish();
    } finally {
      tos.close();
    }

    // successfully untar it into an existing dir:
    FileUtil.unTar(simpleTar, tmp);
    // check result:
    assertTrue(new File(tmp, "/bar/foo").exists());
    assertEquals(12, new File(tmp, "/bar/foo").length());
    
    final File regularFile = new File(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
    regularFile.createNewFile();
    assertTrue(regularFile.exists());
    try {
      FileUtil.unTar(simpleTar, regularFile);
      assertTrue("An IOException expected.", false);
    } catch (IOException ioe) {
      // okay
    }
  }
  
  @Test (timeout = 30000)
  public void testReplaceFile() throws IOException {
    setupDirs();
    final File srcFile = new File(tmp, "src");
    
    // src exists, and target does not exist:
    srcFile.createNewFile();
    assertTrue(srcFile.exists());
    final File targetFile = new File(tmp, "target");
    assertTrue(!targetFile.exists());
    FileUtil.replaceFile(srcFile, targetFile);
    assertTrue(!srcFile.exists());
    assertTrue(targetFile.exists());

    // src exists and target is a regular file: 
    srcFile.createNewFile();
    assertTrue(srcFile.exists());
    FileUtil.replaceFile(srcFile, targetFile);
    assertTrue(!srcFile.exists());
    assertTrue(targetFile.exists());
    
    // src exists, and target is a non-empty directory: 
    srcFile.createNewFile();
    assertTrue(srcFile.exists());
    targetFile.delete();
    targetFile.mkdirs();
    File obstacle = new File(targetFile, "obstacle");
    obstacle.createNewFile();
    assertTrue(obstacle.exists());
    assertTrue(targetFile.exists() && targetFile.isDirectory());
    try {
      FileUtil.replaceFile(srcFile, targetFile);
      assertTrue(false);
    } catch (IOException ioe) {
      // okay
    }
    // check up the post-condition: nothing is deleted:
    assertTrue(srcFile.exists());
    assertTrue(targetFile.exists() && targetFile.isDirectory());
    assertTrue(obstacle.exists());
  }
  
  @Test (timeout = 30000)
  public void testCreateLocalTempFile() throws IOException {
    setupDirs();
    final File baseFile = new File(tmp, "base");
    File tmp1 = FileUtil.createLocalTempFile(baseFile, "foo", false);
    File tmp2 = FileUtil.createLocalTempFile(baseFile, "foo", true);
    assertFalse(tmp1.getAbsolutePath().equals(baseFile.getAbsolutePath()));
    assertFalse(tmp2.getAbsolutePath().equals(baseFile.getAbsolutePath()));
    assertTrue(tmp1.exists() && tmp2.exists());
    assertTrue(tmp1.canWrite() && tmp2.canWrite());
    assertTrue(tmp1.canRead() && tmp2.canRead());
    tmp1.delete();
    tmp2.delete();
    assertTrue(!tmp1.exists() && !tmp2.exists());
  }
  
  @Test (timeout = 30000)
  public void testUnZip() throws IOException {
    // make sa simple zip
    setupDirs();
    
    // make a simple tar:
    final File simpleZip = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleZip); 
    ZipOutputStream tos = new ZipOutputStream(os);
    try {
      ZipEntry ze = new ZipEntry("foo");
      byte[] data = "some-content".getBytes("UTF-8");
      ze.setSize(data.length);
      tos.putNextEntry(ze);
      tos.write(data);
      tos.closeEntry();
      tos.flush();
      tos.finish();
    } finally {
      tos.close();
    }
    
    // successfully untar it into an existing dir:
    FileUtil.unZip(simpleZip, tmp);
    // check result:
    assertTrue(new File(tmp, "foo").exists());
    assertEquals(12, new File(tmp, "foo").length());
    
    final File regularFile = new File(tmp, "QuickBrownFoxJumpsOverTheLazyDog");
    regularFile.createNewFile();
    assertTrue(regularFile.exists());
    try {
      FileUtil.unZip(simpleZip, regularFile);
      assertTrue("An IOException expected.", false);
    } catch (IOException ioe) {
      // okay
    }
  }  
  
  @Test (timeout = 30000)
  /*
   * Test method copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)
   */
  public void testCopy5() throws IOException {
    setupDirs();
    
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
    assertTrue(dest.exists());
    assertEquals(content.getBytes().length 
        + System.getProperty("line.separator").getBytes().length, dest.length());
    assertTrue(srcFile.exists()); // should not be deleted
    
    // copy regular file, delete src:
    dest.delete();
    assertTrue(!dest.exists());
    result = FileUtil.copy(fs, srcPath, dest, true, conf);
    assertTrue(result);
    assertTrue(dest.exists());
    assertEquals(content.getBytes().length 
        + System.getProperty("line.separator").getBytes().length, dest.length());
    assertTrue(!srcFile.exists()); // should be deleted
    
    // copy a dir:
    dest.delete();
    assertTrue(!dest.exists());
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
    assertTrue(!partitioned.exists()); // should be deleted
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
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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
  @Test (timeout = 30000)
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

    Assert.assertEquals(0, link.length());

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

  @Test (timeout = 30000)
  public void testCreateJarWithClassPath() throws Exception {
    // setup test directory for files
    Assert.assertFalse(tmp.exists());
    Assert.assertTrue(tmp.mkdirs());

    // create files expected to match a wildcard
    List<File> wildcardMatches = Arrays.asList(new File(tmp, "wildcard1.jar"),
      new File(tmp, "wildcard2.jar"), new File(tmp, "wildcard3.JAR"),
      new File(tmp, "wildcard4.JAR"));
    for (File wildcardMatch: wildcardMatches) {
      Assert.assertTrue("failure creating file: " + wildcardMatch,
        wildcardMatch.createNewFile());
    }

    // create non-jar files, which we expect to not be included in the classpath
    Assert.assertTrue(new File(tmp, "text.txt").createNewFile());
    Assert.assertTrue(new File(tmp, "executable.exe").createNewFile());
    Assert.assertTrue(new File(tmp, "README").createNewFile());

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
            expectedClassPaths.add(wildcardMatch.toURI().toURL()
              .toExternalForm());
          }
        } else {
          File fileCp = null;
          if(!new Path(classPath).isAbsolute()) {
            fileCp = new File(tmp, classPath);
          }
          else {
            fileCp = new File(classPath);
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

    // setup test directory for files
    assertFalse(tmp.exists());
    assertTrue(tmp.mkdirs());

    // create jar files to be returned
    File jar1 = new File(tmp, "wildcard1.jar");
    File jar2 = new File(tmp, "wildcard2.JAR");
    List<File> matches = Arrays.asList(jar1, jar2);
    for (File match: matches) {
      assertTrue("failure creating file: " + match, match.createNewFile());
    }

    // create non-jar files, which we expect to not be included in the result
    assertTrue(new File(tmp, "text.txt").createNewFile());
    assertTrue(new File(tmp, "executable.exe").createNewFile());
    assertTrue(new File(tmp, "README").createNewFile());

    // pass in the directory
    String directory = tmp.getCanonicalPath();
    jars = FileUtil.getJarsInDirectory(directory);
    assertEquals("there should be 2 jars", 2, jars.size());
    for (Path jar: jars) {
      URL url = jar.toUri().toURL();
      assertTrue("the jar should match either of the jars",
          url.equals(jar1.toURI().toURL()) || url.equals(jar2.toURI().toURL()));
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
    } catch (URISyntaxException use) {
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
    } catch (UnknownHostException ue) {
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
    assertEquals(FileUtil.compareFs(null,fs1),false);
    assertEquals(FileUtil.compareFs(fs1,null),false);
  }

  @Test
  public void testCompareFsDirectories() throws Exception {
    setupCompareFs();
    assertEquals(FileUtil.compareFs(fs1,fs1),true);
    assertEquals(FileUtil.compareFs(fs1,fs2),false);
    assertEquals(FileUtil.compareFs(fs1,fs5),false);
    assertEquals(FileUtil.compareFs(fs3,fs4),true);
    assertEquals(FileUtil.compareFs(fs1,fs6),false);
  }

  @Test(timeout = 8000)
  public void testCreateSymbolicLinkUsingJava() throws IOException {
    setupDirs();
    final File simpleTar = new File(del, FILE);
    OutputStream os = new FileOutputStream(simpleTar);
    TarArchiveOutputStream tos = new TarArchiveOutputStream(os);
    File untarFile = null;
    try {
      // Files to tar
      final String tmpDir = "tmp/test";
      File tmpDir1 = new File(tmpDir, "dir1/");
      File tmpDir2 = new File(tmpDir, "dir2/");
      // Delete the directories if they already exist
      tmpDir1.mkdirs();
      tmpDir2.mkdirs();

      java.nio.file.Path symLink = FileSystems
          .getDefault().getPath(tmpDir1.getPath() + "/sl");

      // Create Symbolic Link
      Files.createSymbolicLink(symLink,
          FileSystems.getDefault().getPath(tmpDir2.getPath())).toString();
      assertTrue(Files.isSymbolicLink(symLink.toAbsolutePath()));
      // put entries in tar file
      putEntriesInTar(tos, tmpDir1.getParentFile());
      tos.close();

      untarFile = new File(tmpDir, "2");
      // Untar using java
      FileUtil.unTarUsingJava(simpleTar, untarFile, false);

      // Check symbolic link and other directories are there in untar file
      assertTrue(Files.exists(untarFile.toPath()));
      assertTrue(Files.exists(FileSystems.getDefault().getPath(untarFile
          .getPath(), tmpDir)));
      assertTrue(Files.isSymbolicLink(FileSystems.getDefault().getPath(untarFile
          .getPath().toString(), symLink.toString())));

    } finally {
      FileUtils.deleteDirectory(new File("tmp"));
      tos.close();
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

}
