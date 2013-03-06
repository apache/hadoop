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

import org.junit.Before;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
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
    f.setReadable(true);
    f.setWritable(true);
    f.setExecutable(true);
  }
  
  private static void revokePermissions(final File f) {
     f.setWritable(false);
     f.setExecutable(false);
     f.setReadable(false);
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
    if(Shell.WINDOWS) {
      // windows Dir.setWritable(false) does not work for directories
      return;
    }
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
    if(Shell.WINDOWS) {
      // windows Dir.setWritable(false) does not work for directories
      return;
    }
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
  
  @Test (timeout = 30000)
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

  /**
   * Test that getDU is able to handle cycles caused due to symbolic links
   * and that directory sizes are not added to the final calculated size
   * @throws IOException
   */
  @Test (timeout = 30000)
  public void testGetDU() throws IOException {
    setupDirs();

    long du = FileUtil.getDU(TEST_DIR);
    // Only two files (in partitioned).  Each has 3 characters + system-specific
    // line separator.
    long expected = 2 * (3 + System.getProperty("line.separator").length());
    Assert.assertEquals(expected, du);
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

  @Test (timeout = 30000)
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
    List<String> classPaths = Arrays.asList("cp1.jar", "cp2.jar", wildcardPath,
      "cp3.jar");
    String inputClassPath = StringUtils.join(File.pathSeparator, classPaths);
    String classPathJar = FileUtil.createJarWithClassPath(inputClassPath,
      new Path(tmp.getCanonicalPath()));

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
        if (!wildcardPath.equals(classPath)) {
          expectedClassPaths.add(new File(classPath).toURI().toURL()
            .toExternalForm());
        } else {
          // add wildcard matches
          for (File wildcardMatch: wildcardMatches) {
            expectedClassPaths.add(wildcardMatch.toURI().toURL()
              .toExternalForm());
          }
        }
      }
      List<String> actualClassPaths = Arrays.asList(classPathAttr.split(" "));
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
}
