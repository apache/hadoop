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

import java.io.*;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * Base test for symbolic links
 */
public abstract class SymlinkBaseTest {
  // Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
  static {
    FileSystem.enableSymlinks();
  }
  static final long seed = 0xDEADBEEFL;
  static final int  blockSize =  8192;
  static final int  fileSize  = 16384;
  static final int numBlocks = fileSize / blockSize;

  protected static FSTestWrapper wrapper;

  abstract protected String getScheme();
  abstract protected String testBaseDir1() throws IOException;
  abstract protected String testBaseDir2() throws IOException;
  abstract protected URI testURI();

  // Returns true if the filesystem is emulating symlink support. Certain
  // checks will be bypassed if that is the case.
  //
  protected boolean emulatingSymlinksOnWindows() {
    return false;
  }

  protected IOException unwrapException(IOException e) {
    return e;
  }

  protected static void createAndWriteFile(Path p) throws IOException {
    createAndWriteFile(wrapper, p);
  }

  protected static void createAndWriteFile(FSTestWrapper wrapper, Path p)
      throws IOException {
    wrapper.createFile(p, numBlocks, CreateOpts.createParent(),
        CreateOpts.repFac((short) 1), CreateOpts.blockSize(blockSize));
  }

  protected static void readFile(Path p) throws IOException {
    wrapper.readFile(p, fileSize);
  }

  protected static void appendToFile(Path p) throws IOException {
    wrapper.appendToFile(p, numBlocks,
        CreateOpts.blockSize(blockSize));
  }

  @Before
  public void setUp() throws Exception {
    wrapper.mkdir(new Path(testBaseDir1()), FileContext.DEFAULT_PERM, true);
    wrapper.mkdir(new Path(testBaseDir2()), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    wrapper.delete(new Path(testBaseDir1()), true);
    wrapper.delete(new Path(testBaseDir2()), true);
  }

  @Test(timeout=10000)
  /** The root is not a symlink */
  public void testStatRoot() throws IOException {
    assertFalse(wrapper.getFileLinkStatus(new Path("/")).isSymlink());
  }

  @Test(timeout=10000)
  /** Test setWorkingDirectory not resolves symlinks */
  public void testSetWDNotResolvesLinks() throws IOException {
    Path dir       = new Path(testBaseDir1());
    Path linkToDir = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(dir, linkToDir, false);
    wrapper.setWorkingDirectory(linkToDir);
    assertEquals(linkToDir.getName(), wrapper.getWorkingDirectory().getName());
  }

  @Test(timeout=10000)
  /** Test create a dangling link */
  public void testCreateDanglingLink() throws IOException {
    Path file = new Path("/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.getFileStatus(link);
      fail("Got file status of non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    wrapper.delete(link, false);
  }

  @Test(timeout=10000)
  /** Test create a link to null and empty path */
  public void testCreateLinkToNullEmpty() throws IOException {
    Path link = new Path(testBaseDir1()+"/link");
    try {
      wrapper.createSymlink(null, link, false);
      fail("Can't create symlink to null");
    } catch (java.lang.NullPointerException e) {
      // Expected, create* with null yields NPEs
    }
    try {
      wrapper.createSymlink(new Path(""), link, false);
      fail("Can't create symlink to empty string");
    } catch (java.lang.IllegalArgumentException e) {
      // Expected, Path("") is invalid
    }
  }

  @Test(timeout=10000)
  /** Create a link with createParent set */
  public void testCreateLinkCanCreateParent() throws IOException {
    Path file = new Path(testBaseDir1()+"/file");
    Path link = new Path(testBaseDir2()+"/linkToFile");
    createAndWriteFile(file);
    wrapper.delete(new Path(testBaseDir2()), true);
    try {
      wrapper.createSymlink(file, link, false);
      fail("Created link without first creating parent dir");
    } catch (IOException x) {
      // Expected. Need to create testBaseDir2() first.
    }
    assertFalse(wrapper.exists(new Path(testBaseDir2())));
    wrapper.createSymlink(file, link, true);
    readFile(link);
  }

  @Test(timeout=10000)
  /** Try to create a directory given a path that refers to a symlink */
  public void testMkdirExistingLink() throws IOException {
    Path file = new Path(testBaseDir1() + "/targetFile");
    createAndWriteFile(file);

    Path dir  = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(file, dir, false);
    try {
      wrapper.mkdir(dir, FileContext.DEFAULT_PERM, false);
      fail("Created a dir where a symlink exists");
    } catch (FileAlreadyExistsException e) {
      // Expected. The symlink already exists.
    } catch (IOException e) {
      // LocalFs just throws an IOException
      assertEquals("file", getScheme());
    }
  }

  @Test(timeout=10000)
  /** Try to create a file with parent that is a dangling link */
  public void testCreateFileViaDanglingLinkParent() throws IOException {
    Path dir  = new Path(testBaseDir1()+"/dangling");
    Path file = new Path(testBaseDir1()+"/dangling/file");
    wrapper.createSymlink(new Path("/doesNotExist"), dir, false);
    FSDataOutputStream out;
    try {
      out = wrapper.create(file, EnumSet.of(CreateFlag.CREATE),
                      CreateOpts.repFac((short) 1),
                      CreateOpts.blockSize(blockSize));
      out.close();
      fail("Created a link with dangling link parent");
    } catch (FileNotFoundException e) {
      // Expected. The parent is dangling.
    }
  }

  @Test(timeout=10000)
  /** Delete a link */
  public void testDeleteLink() throws IOException {
    Path file = new Path(testBaseDir1()+"/file");
    Path link = new Path(testBaseDir1()+"/linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    readFile(link);
    wrapper.delete(link, false);
    try {
      readFile(link);
      fail("Symlink should have been deleted");
    } catch (IOException x) {
      // Expected
    }
    // If we deleted the link we can put it back
    wrapper.createSymlink(file, link, false);
  }

  @Test(timeout=10000)
  /** Ensure open resolves symlinks */
  public void testOpenResolvesLinks() throws IOException {
    Path file = new Path(testBaseDir1()+"/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.open(link);
      fail("link target does not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
    wrapper.delete(link, false);
  }

  @Test(timeout=10000)
  /** Stat a link to a file */
  public void testStatLinkToFile() throws IOException {
    assumeTrue(!emulatingSymlinksOnWindows());
    Path file = new Path(testBaseDir1()+"/file");
    Path linkToFile = new Path(testBaseDir1()+"/linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, linkToFile, false);
    assertFalse(wrapper.getFileLinkStatus(linkToFile).isDirectory());
    assertTrue(wrapper.isSymlink(linkToFile));
    assertTrue(wrapper.isFile(linkToFile));
    assertFalse(wrapper.isDir(linkToFile));
    assertEquals(file, wrapper.getLinkTarget(linkToFile));
    // The local file system does not fully resolve the link
    // when obtaining the file status
    if (!"file".equals(getScheme())) {
      assertEquals(wrapper.getFileStatus(file),
                   wrapper.getFileStatus(linkToFile));
      assertEquals(wrapper.makeQualified(file),
                   wrapper.getFileStatus(linkToFile).getPath());
      assertEquals(wrapper.makeQualified(linkToFile),
                   wrapper.getFileLinkStatus(linkToFile).getPath());
    }
  }

  @Test(timeout=10000)
  /** Stat a relative link to a file */
  public void testStatRelLinkToFile() throws IOException {
    assumeTrue(!"file".equals(getScheme()));
    Path file       = new Path(testBaseDir1(), "file");
    Path linkToFile = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(new Path("file"), linkToFile, false);
    assertEquals(wrapper.getFileStatus(file),
                 wrapper.getFileStatus(linkToFile));
    assertEquals(wrapper.makeQualified(file),
                 wrapper.getFileStatus(linkToFile).getPath());
    assertEquals(wrapper.makeQualified(linkToFile),
                 wrapper.getFileLinkStatus(linkToFile).getPath());
  }

  @Test(timeout=10000)
  /** Stat a link to a directory */
  public void testStatLinkToDir() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path linkToDir = new Path(testBaseDir1()+"/linkToDir");
    wrapper.createSymlink(dir, linkToDir, false);

    assertFalse(wrapper.getFileStatus(linkToDir).isSymlink());
    assertTrue(wrapper.isDir(linkToDir));
    assertFalse(wrapper.getFileLinkStatus(linkToDir).isDirectory());
    assertTrue(wrapper.getFileLinkStatus(linkToDir).isSymlink());

    assertFalse(wrapper.isFile(linkToDir));
    assertTrue(wrapper.isDir(linkToDir));

    assertEquals(dir, wrapper.getLinkTarget(linkToDir));
  }

  @Test(timeout=10000)
  /** Stat a dangling link */
  public void testStatDanglingLink() throws IOException {
    Path file = new Path("/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");
    wrapper.createSymlink(file, link, false);
    assertFalse(wrapper.getFileLinkStatus(link).isDirectory());
    assertTrue(wrapper.getFileLinkStatus(link).isSymlink());
  }

  @Test(timeout=10000)
  /** Stat a non-existant file */
  public void testStatNonExistentFiles() throws IOException {
    Path fileAbs = new Path("/doesNotExist");
    try {
      wrapper.getFileLinkStatus(fileAbs);
      fail("Got FileStatus for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    try {
      wrapper.getLinkTarget(fileAbs);
      fail("Got link target for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /** Test stat'ing a regular file and directory */
  public void testStatNonLinks() throws IOException {
    Path dir   = new Path(testBaseDir1());
    Path file  = new Path(testBaseDir1()+"/file");
    createAndWriteFile(file);
    try {
      wrapper.getLinkTarget(dir);
      fail("Lstat'd a non-symlink");
    } catch (IOException e) {
      // Expected.
    }
    try {
      wrapper.getLinkTarget(file);
      fail("Lstat'd a non-symlink");
    } catch (IOException e) {
      // Expected.
    }
  }

  @Test(timeout=10000)
  /** Test links that link to each other */
  public void testRecursiveLinks() throws IOException {
    Path link1 = new Path(testBaseDir1()+"/link1");
    Path link2 = new Path(testBaseDir1()+"/link2");
    wrapper.createSymlink(link1, link2, false);
    wrapper.createSymlink(link2, link1, false);
    try {
      readFile(link1);
      fail("Read recursive link");
    } catch (FileNotFoundException f) {
      // LocalFs throws sub class of IOException, since File.exists
      // returns false for a link to link.
    } catch (IOException x) {
      assertEquals("Possible cyclic loop while following symbolic link "+
                   link1.toString(), x.getMessage());
    }
  }

  /* Assert that the given link to a file behaves as expected. */
  private void checkLink(Path linkAbs, Path expectedTarget, Path targetQual)
      throws IOException {

    // If we are emulating symlinks then many of these checks will fail
    // so we skip them.
    //
    assumeTrue(!emulatingSymlinksOnWindows());

    Path dir = new Path(testBaseDir1());
    // isFile/Directory
    assertTrue(wrapper.isFile(linkAbs));
    assertFalse(wrapper.isDir(linkAbs));

    // Check getFileStatus
    assertFalse(wrapper.getFileStatus(linkAbs).isSymlink());
    assertFalse(wrapper.getFileStatus(linkAbs).isDirectory());
    assertEquals(fileSize, wrapper.getFileStatus(linkAbs).getLen());

    // Check getFileLinkStatus
    assertTrue(wrapper.isSymlink(linkAbs));
    assertFalse(wrapper.getFileLinkStatus(linkAbs).isDirectory());

    // Check getSymlink always returns a qualified target, except
    // when partially qualified paths are used (see tests below).
    assertEquals(targetQual.toString(),
        wrapper.getFileLinkStatus(linkAbs).getSymlink().toString());
    assertEquals(targetQual, wrapper.getFileLinkStatus(linkAbs).getSymlink());
    // Check that the target is qualified using the file system of the
    // path used to access the link (if the link target was not specified
    // fully qualified, in that case we use the link target verbatim).
    if (!"file".equals(getScheme())) {
      FileContext localFc = FileContext.getLocalFSFileContext();
      Path linkQual = new Path(testURI().toString(), linkAbs);
      assertEquals(targetQual,
                   localFc.getFileLinkStatus(linkQual).getSymlink());
    }

    // Check getLinkTarget
    assertEquals(expectedTarget, wrapper.getLinkTarget(linkAbs));

    // Now read using all path types..
    wrapper.setWorkingDirectory(dir);
    readFile(new Path("linkToFile"));
    readFile(linkAbs);
    // And fully qualified.. (NB: for local fs this is partially qualified)
    readFile(new Path(testURI().toString(), linkAbs));
    // And partially qualified..
    boolean failureExpected = true;
    // local files are special cased, no authority
    if ("file".equals(getScheme())) {
      failureExpected = false;
    }
    // FileSystem automatically adds missing authority if scheme matches default
    else if (wrapper instanceof FileSystemTestWrapper) {
      failureExpected = false;
    }
    try {
      readFile(new Path(getScheme()+":///"+testBaseDir1()+"/linkToFile"));
      assertFalse(failureExpected);
    } catch (Exception e) {
      if (!failureExpected) {
        throw new IOException(e);
      }
      //assertTrue(failureExpected);
    }

    // Now read using a different file context (for HDFS at least)
    if (wrapper instanceof FileContextTestWrapper
        && !"file".equals(getScheme())) {
      FSTestWrapper localWrapper = wrapper.getLocalFSWrapper();
      localWrapper.readFile(new Path(testURI().toString(), linkAbs), fileSize);
    }
  }

  @Test(timeout=10000)
  /** Test creating a symlink using relative paths */
  public void testCreateLinkUsingRelPaths() throws IOException {
    Path fileAbs = new Path(testBaseDir1(), "file");
    Path linkAbs = new Path(testBaseDir1(), "linkToFile");
    Path schemeAuth = new Path(testURI().toString());
    Path fileQual = new Path(schemeAuth, testBaseDir1()+"/file");
    createAndWriteFile(fileAbs);

    wrapper.setWorkingDirectory(new Path(testBaseDir1()));
    wrapper.createSymlink(new Path("file"), new Path("linkToFile"), false);
    checkLink(linkAbs, new Path("file"), fileQual);

    // Now rename the link's parent. Because the target was specified
    // with a relative path the link should still resolve.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    Path fileViaDir2 = new Path(schemeAuth, testBaseDir2()+"/file");
    wrapper.rename(dir1, dir2, Rename.OVERWRITE);
    FileStatus[] stats = wrapper.listStatus(dir2);
    assertEquals(fileViaDir2,
        wrapper.getFileLinkStatus(linkViaDir2).getSymlink());
    readFile(linkViaDir2);
  }

  @Test(timeout=10000)
  /** Test creating a symlink using absolute paths */
  public void testCreateLinkUsingAbsPaths() throws IOException {
    Path fileAbs = new Path(testBaseDir1()+"/file");
    Path linkAbs = new Path(testBaseDir1()+"/linkToFile");
    Path schemeAuth = new Path(testURI().toString());
    Path fileQual = new Path(schemeAuth, testBaseDir1()+"/file");
    createAndWriteFile(fileAbs);

    wrapper.createSymlink(fileAbs, linkAbs, false);
    checkLink(linkAbs, fileAbs, fileQual);

    // Now rename the link's parent. The target doesn't change and
    // now no longer exists so accessing the link should fail.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    wrapper.rename(dir1, dir2, Rename.OVERWRITE);
    assertEquals(fileQual, wrapper.getFileLinkStatus(linkViaDir2).getSymlink());
    try {
      readFile(linkViaDir2);
      fail("The target should not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /**
   * Test creating a symlink using fully and partially qualified paths.
   * NB: For local fs this actually tests partially qualified paths,
   * as they don't support fully qualified paths.
   */
  public void testCreateLinkUsingFullyQualPaths() throws IOException {
    Path fileAbs  = new Path(testBaseDir1(), "file");
    Path linkAbs  = new Path(testBaseDir1(), "linkToFile");
    Path fileQual = new Path(testURI().toString(), fileAbs);
    Path linkQual = new Path(testURI().toString(), linkAbs);
    createAndWriteFile(fileAbs);

    wrapper.createSymlink(fileQual, linkQual, false);
    checkLink(linkAbs,
              "file".equals(getScheme()) ? fileAbs : fileQual,
              fileQual);

    // Now rename the link's parent. The target doesn't change and
    // now no longer exists so accessing the link should fail.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    wrapper.rename(dir1, dir2, Rename.OVERWRITE);
    assertEquals(fileQual, wrapper.getFileLinkStatus(linkViaDir2).getSymlink());
    try {
      readFile(linkViaDir2);
      fail("The target should not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /**
   * Test creating a symlink using partially qualified paths, ie a scheme
   * but no authority and vice versa. We just test link targets here since
   * creating using a partially qualified path is file system specific.
   */
  public void testCreateLinkUsingPartQualPath1() throws IOException {
    // Partially qualified paths are covered for local file systems
    // in the previous test.
    assumeTrue(!"file".equals(getScheme()));
    Path schemeAuth   = new Path(testURI().toString());
    Path fileWoHost   = new Path(getScheme()+"://"+testBaseDir1()+"/file");
    Path link         = new Path(testBaseDir1()+"/linkToFile");
    Path linkQual     = new Path(schemeAuth, testBaseDir1()+"/linkToFile");
    FSTestWrapper localWrapper = wrapper.getLocalFSWrapper();

    wrapper.createSymlink(fileWoHost, link, false);
    // Partially qualified path is stored
    assertEquals(fileWoHost, wrapper.getLinkTarget(linkQual));
    // NB: We do not add an authority
    assertEquals(fileWoHost.toString(),
      wrapper.getFileLinkStatus(link).getSymlink().toString());
    assertEquals(fileWoHost.toString(),
      wrapper.getFileLinkStatus(linkQual).getSymlink().toString());
    // Ditto even from another file system
    if (wrapper instanceof FileContextTestWrapper) {
      assertEquals(fileWoHost.toString(),
        localWrapper.getFileLinkStatus(linkQual).getSymlink().toString());
    }
    // Same as if we accessed a partially qualified path directly
    try {
      readFile(link);
      fail("DFS requires URIs with schemes have an authority");
    } catch (java.lang.RuntimeException e) {
      assertTrue(wrapper instanceof FileContextTestWrapper);
      // Expected
    } catch (FileNotFoundException e) {
      assertTrue(wrapper instanceof FileSystemTestWrapper);
      GenericTestUtils.assertExceptionContains(
          "File does not exist: /test1/file", e);
    }
  }

  @Test(timeout=10000)
  /** Same as above but vice versa (authority but no scheme) */
  public void testCreateLinkUsingPartQualPath2() throws IOException {
    Path link         = new Path(testBaseDir1(), "linkToFile");
    Path fileWoScheme = new Path("//"+testURI().getAuthority()+
                                 testBaseDir1()+"/file");
    if ("file".equals(getScheme())) {
      return;
    }
    wrapper.createSymlink(fileWoScheme, link, false);
    assertEquals(fileWoScheme, wrapper.getLinkTarget(link));
    assertEquals(fileWoScheme.toString(),
      wrapper.getFileLinkStatus(link).getSymlink().toString());
    try {
      readFile(link);
      fail("Accessed a file with w/o scheme");
    } catch (IOException e) {
      // Expected
      if (wrapper instanceof FileContextTestWrapper) {
        GenericTestUtils.assertExceptionContains(
            AbstractFileSystem.NO_ABSTRACT_FS_ERROR, e);
      } else if (wrapper instanceof FileSystemTestWrapper) {
        assertEquals("No FileSystem for scheme: null", e.getMessage());
      }
    }
  }

  @Test(timeout=10000)
  /** Lstat and readlink on a normal file and directory */
  public void testLinkStatusAndTargetWithNonLink() throws IOException {
    Path schemeAuth = new Path(testURI().toString());
    Path dir        = new Path(testBaseDir1());
    Path dirQual    = new Path(schemeAuth, dir.toString());
    Path file       = new Path(testBaseDir1(), "file");
    Path fileQual   = new Path(schemeAuth, file.toString());
    createAndWriteFile(file);
    assertEquals(wrapper.getFileStatus(file), wrapper.getFileLinkStatus(file));
    assertEquals(wrapper.getFileStatus(dir), wrapper.getFileLinkStatus(dir));
    try {
      wrapper.getLinkTarget(file);
      fail("Get link target on non-link should throw an IOException");
    } catch (IOException x) {
      assertEquals("Path "+fileQual+" is not a symbolic link", x.getMessage());
    }
    try {
      wrapper.getLinkTarget(dir);
      fail("Get link target on non-link should throw an IOException");
    } catch (IOException x) {
      assertEquals("Path "+dirQual+" is not a symbolic link", x.getMessage());
    }
  }

  @Test(timeout=10000)
  /** Test create symlink to a directory */
  public void testCreateLinkToDirectory() throws IOException {
    Path dir1      = new Path(testBaseDir1());
    Path file      = new Path(testBaseDir1(), "file");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    createAndWriteFile(file);
    wrapper.createSymlink(dir1, linkToDir, false);
    assertFalse(wrapper.isFile(linkToDir));
    assertTrue(wrapper.isDir(linkToDir));
    assertTrue(wrapper.getFileStatus(linkToDir).isDirectory());
    assertTrue(wrapper.getFileLinkStatus(linkToDir).isSymlink());
  }

  @Test(timeout=10000)
  /** Test create and remove a file through a symlink */
  public void testCreateFileViaSymlink() throws IOException {
    Path dir         = new Path(testBaseDir1());
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink = new Path(linkToDir, "file");
    wrapper.createSymlink(dir, linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertTrue(wrapper.isFile(fileViaLink));
    assertFalse(wrapper.isDir(fileViaLink));
    assertFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(wrapper.getFileStatus(fileViaLink).isDirectory());
    readFile(fileViaLink);
    wrapper.delete(fileViaLink, true);
    assertFalse(wrapper.exists(fileViaLink));
  }

  @Test(timeout=10000)
  /** Test make and delete directory through a symlink */
  public void testCreateDirViaSymlink() throws IOException {
    Path dir1          = new Path(testBaseDir1());
    Path subDir        = new Path(testBaseDir1(), "subDir");
    Path linkToDir     = new Path(testBaseDir2(), "linkToDir");
    Path subDirViaLink = new Path(linkToDir, "subDir");
    wrapper.createSymlink(dir1, linkToDir, false);
    wrapper.mkdir(subDirViaLink, FileContext.DEFAULT_PERM, true);
    assertTrue(wrapper.isDir(subDirViaLink));
    wrapper.delete(subDirViaLink, false);
    assertFalse(wrapper.exists(subDirViaLink));
    assertFalse(wrapper.exists(subDir));
  }

  @Test(timeout=10000)
  /** Create symlink through a symlink */
  public void testCreateLinkViaLink() throws IOException {
    assumeTrue(!emulatingSymlinksOnWindows());
    Path dir1        = new Path(testBaseDir1());
    Path file        = new Path(testBaseDir1(), "file");
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink = new Path(linkToDir, "file");
    Path linkToFile  = new Path(linkToDir, "linkToFile");
    /*
     * /b2/linkToDir            -> /b1
     * /b2/linkToDir/linkToFile -> /b2/linkToDir/file
     */
    createAndWriteFile(file);
    wrapper.createSymlink(dir1, linkToDir, false);
    wrapper.createSymlink(fileViaLink, linkToFile, false);
    assertTrue(wrapper.isFile(linkToFile));
    assertTrue(wrapper.getFileLinkStatus(linkToFile).isSymlink());
    readFile(linkToFile);
    assertEquals(fileSize, wrapper.getFileStatus(linkToFile).getLen());
    assertEquals(fileViaLink, wrapper.getLinkTarget(linkToFile));
  }

  @Test(timeout=10000)
  /** Test create symlink to a directory */
  public void testListStatusUsingLink() throws IOException {
    Path file  = new Path(testBaseDir1(), "file");
    Path link  = new Path(testBaseDir1(), "link");
    createAndWriteFile(file);
    wrapper.createSymlink(new Path(testBaseDir1()), link, false);
    // The size of the result is file system dependent, Hdfs is 2 (file
    // and link) and LocalFs is 3 (file, link, file crc).
    FileStatus[] stats = wrapper.listStatus(link);
    assertTrue(stats.length == 2 || stats.length == 3);
    RemoteIterator<FileStatus> statsItor = wrapper.listStatusIterator(link);
    int dirLen = 0;
    while(statsItor.hasNext()) {
      statsItor.next();
      dirLen++;
    }
    assertTrue(dirLen == 2 || dirLen == 3);
  }

  @Test(timeout=10000)
  /** Test create symlink using the same path */
  public void testCreateLinkTwice() throws IOException {
    assumeTrue(!emulatingSymlinksOnWindows());
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.createSymlink(file, link, false);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /** Test access via a symlink to a symlink */
  public void testCreateLinkToLink() throws IOException {
    Path dir1        = new Path(testBaseDir1());
    Path file        = new Path(testBaseDir1(), "file");
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path linkToLink  = new Path(testBaseDir2(), "linkToLink");
    Path fileViaLink = new Path(testBaseDir2(), "linkToLink/file");
    createAndWriteFile(file);
    wrapper.createSymlink(dir1, linkToDir, false);
    wrapper.createSymlink(linkToDir, linkToLink, false);
    assertTrue(wrapper.isFile(fileViaLink));
    assertFalse(wrapper.isDir(fileViaLink));
    assertFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(wrapper.getFileStatus(fileViaLink).isDirectory());
    readFile(fileViaLink);
  }

  @Test(timeout=10000)
  /** Can not create a file with path that refers to a symlink */
  public void testCreateFileDirExistingLink() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    try {
      createAndWriteFile(link);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }
    try {
      wrapper.mkdir(link, FsPermission.getDefault(), false);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /** Test deleting and recreating a symlink */
  public void testUseLinkAferDeleteLink() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    wrapper.delete(link, false);
    try {
      readFile(link);
      fail("link was deleted");
    } catch (IOException x) {
      // Expected
    }
    readFile(file);
    wrapper.createSymlink(file, link, false);
    readFile(link);
  }

  @Test(timeout=10000)
  /** Test create symlink to . */
  public void testCreateLinkToDot() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToDot");
    createAndWriteFile(file);
    wrapper.setWorkingDirectory(dir);
    try {
      wrapper.createSymlink(new Path("."), link, false);
      fail("Created symlink to dot");
    } catch (IOException x) {
      // Expected. Path(".") resolves to "" because URI normalizes
      // the dot away and AbstractFileSystem considers "" invalid.
    }
  }

  @Test(timeout=10000)
  /** Test create symlink to .. */
  public void testCreateLinkToDotDot() throws IOException {
    Path file        = new Path(testBaseDir1(), "test/file");
    Path dotDot      = new Path(testBaseDir1(), "test/..");
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink = new Path(linkToDir,      "test/file");
    // Symlink to .. is not a problem since the .. is squashed early
    assertEquals(new Path(testBaseDir1()), dotDot);
    createAndWriteFile(file);
    wrapper.createSymlink(dotDot, linkToDir, false);
    readFile(fileViaLink);
    assertEquals(fileSize, wrapper.getFileStatus(fileViaLink).getLen());
  }

  @Test(timeout=10000)
  /** Test create symlink to ../file */
  public void testCreateLinkToDotDotPrefix() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path dir  = new Path(testBaseDir1(), "test");
    Path link = new Path(testBaseDir1(), "test/link");
    createAndWriteFile(file);
    wrapper.mkdir(dir, FsPermission.getDefault(), false);
    wrapper.setWorkingDirectory(dir);
    wrapper.createSymlink(new Path("../file"), link, false);
    readFile(link);
    assertEquals(new Path("../file"), wrapper.getLinkTarget(link));
  }

  @Test(timeout=10000)
  /** Test rename file using a path that contains a symlink. The rename should
   * work as if the path did not contain a symlink */
  public void testRenameFileViaSymlink() throws IOException {
    Path dir            = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink    = new Path(linkToDir, "file");
    Path fileNewViaLink = new Path(linkToDir, "fileNew");
    createAndWriteFile(file);
    wrapper.createSymlink(dir, linkToDir, false);
    wrapper.rename(fileViaLink, fileNewViaLink);
    assertFalse(wrapper.exists(fileViaLink));
    assertFalse(wrapper.exists(file));
    assertTrue(wrapper.exists(fileNewViaLink));
  }

  @Test(timeout=10000)
  /** Test rename a file through a symlink but this time only the
   * destination path has an intermediate symlink. The rename should work
   * as if the path did not contain a symlink */
  public void testRenameFileToDestViaSymlink() throws IOException {
    Path dir       = new Path(testBaseDir1());
    Path file      = new Path(testBaseDir1(), "file");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    Path subDir    = new Path(linkToDir, "subDir");
    createAndWriteFile(file);
    wrapper.createSymlink(dir, linkToDir, false);
    wrapper.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    try {
      wrapper.rename(file, subDir);
      fail("Renamed file to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(wrapper.exists(file));
  }

  @Test(timeout=10000)
  /** Similar tests as the previous ones but rename a directory */
  public void testRenameDirViaSymlink() throws IOException {
    Path baseDir       = new Path(testBaseDir1());
    Path dir           = new Path(baseDir, "dir");
    Path linkToDir     = new Path(testBaseDir2(), "linkToDir");
    Path dirViaLink    = new Path(linkToDir, "dir");
    Path dirNewViaLink = new Path(linkToDir, "dirNew");
    wrapper.mkdir(dir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(baseDir, linkToDir, false);
    assertTrue(wrapper.exists(dirViaLink));
    wrapper.rename(dirViaLink, dirNewViaLink);
    assertFalse(wrapper.exists(dirViaLink));
    assertFalse(wrapper.exists(dir));
    assertTrue(wrapper.exists(dirNewViaLink));
  }

  @Test(timeout=10000)
  /** Similar tests as the previous ones but rename a symlink */
  public void testRenameSymlinkViaSymlink() throws IOException {
    Path baseDir        = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path link           = new Path(testBaseDir1(), "link");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path linkViaLink    = new Path(linkToDir, "link");
    Path linkNewViaLink = new Path(linkToDir, "linkNew");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    wrapper.createSymlink(baseDir, linkToDir, false);
    wrapper.rename(linkViaLink, linkNewViaLink);
    assertFalse(wrapper.exists(linkViaLink));
    // Check that we didn't rename the link target
    assertTrue(wrapper.exists(file));
    assertTrue(wrapper.getFileLinkStatus(linkNewViaLink).isSymlink() ||
        emulatingSymlinksOnWindows());
    readFile(linkNewViaLink);
  }

  @Test(timeout=10000)
  /** Test rename a directory to a symlink to a directory */
  public void testRenameDirToSymlinkToDir() throws IOException {
    Path dir1      = new Path(testBaseDir1());
    Path subDir = new Path(testBaseDir2(), "subDir");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    wrapper.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(subDir, linkToDir, false);
    try {
      wrapper.rename(dir1, linkToDir, Rename.OVERWRITE);
      fail("Renamed directory to a symlink");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(wrapper.exists(dir1));
    assertTrue(wrapper.exists(linkToDir));
  }

  @Test(timeout=10000)
  /** Test rename a directory to a symlink to a file */
  public void testRenameDirToSymlinkToFile() throws IOException {
    Path dir1 = new Path(testBaseDir1());
    Path file = new Path(testBaseDir2(), "file");
    Path linkToFile = new Path(testBaseDir2(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, linkToFile, false);
    try {
      wrapper.rename(dir1, linkToFile, Rename.OVERWRITE);
      fail("Renamed directory to a symlink");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(wrapper.exists(dir1));
    assertTrue(wrapper.exists(linkToFile));
  }

  @Test(timeout=10000)
  /** Test rename a directory to a dangling symlink */
  public void testRenameDirToDanglingSymlink() throws IOException {
    Path dir = new Path(testBaseDir1());
    Path link = new Path(testBaseDir2(), "linkToFile");
    wrapper.createSymlink(new Path("/doesNotExist"), link, false);
    try {
      wrapper.rename(dir, link, Rename.OVERWRITE);
      fail("Renamed directory to a symlink");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(wrapper.exists(dir));
    assertTrue(wrapper.getFileLinkStatus(link) != null);
  }

  @Test(timeout=10000)
  /** Test rename a file to a symlink to a directory */
  public void testRenameFileToSymlinkToDir() throws IOException {
    Path file   = new Path(testBaseDir1(), "file");
    Path subDir = new Path(testBaseDir1(), "subDir");
    Path link   = new Path(testBaseDir1(), "link");
    wrapper.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(subDir, link, false);
    createAndWriteFile(file);
    try {
      wrapper.rename(file, link);
      fail("Renamed file to symlink w/o overwrite");
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    wrapper.rename(file, link, Rename.OVERWRITE);
    assertFalse(wrapper.exists(file));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isFile(link));
    assertFalse(wrapper.getFileLinkStatus(link).isSymlink());
  }

  @Test(timeout=10000)
  /** Test rename a file to a symlink to a file */
  public void testRenameFileToSymlinkToFile() throws IOException {
    Path file1 = new Path(testBaseDir1(), "file1");
    Path file2 = new Path(testBaseDir1(), "file2");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file1);
    createAndWriteFile(file2);
    wrapper.createSymlink(file2, link, false);
    try {
      wrapper.rename(file1, link);
      fail("Renamed file to symlink w/o overwrite");
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    wrapper.rename(file1, link, Rename.OVERWRITE);
    assertFalse(wrapper.exists(file1));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isFile(link));
    assertFalse(wrapper.getFileLinkStatus(link).isSymlink());
  }

  @Test(timeout=10000)
  /** Test rename a file to a dangling symlink */
  public void testRenameFileToDanglingSymlink() throws IOException {
    /* NB: Local file system doesn't handle dangling links correctly
     * since File.exists(danglinLink) returns false. */
    if ("file".equals(getScheme())) {
      return;
    }
    Path file1 = new Path(testBaseDir1(), "file1");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file1);
    wrapper.createSymlink(new Path("/doesNotExist"), link, false);
    try {
      wrapper.rename(file1, link);
    } catch (IOException e) {
      // Expected
    }
    wrapper.rename(file1, link, Rename.OVERWRITE);
    assertFalse(wrapper.exists(file1));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isFile(link));
    assertFalse(wrapper.getFileLinkStatus(link).isSymlink());
  }

  @Test(timeout=10000)
  /** Rename a symlink to a new non-existant name */
  public void testRenameSymlinkNonExistantDest() throws IOException {
    Path file  = new Path(testBaseDir1(), "file");
    Path link1 = new Path(testBaseDir1(), "linkToFile1");
    Path link2 = new Path(testBaseDir1(), "linkToFile2");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link1, false);
    wrapper.rename(link1, link2);
    assertTrue(wrapper.getFileLinkStatus(link2).isSymlink() ||
        emulatingSymlinksOnWindows());
    readFile(link2);
    readFile(file);
    assertFalse(wrapper.exists(link1));
  }

  @Test(timeout=10000)
  /** Rename a symlink to a file that exists */
  public void testRenameSymlinkToExistingFile() throws IOException {
    Path file1 = new Path(testBaseDir1(), "file");
    Path file2 = new Path(testBaseDir1(), "someFile");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file1);
    createAndWriteFile(file2);
    wrapper.createSymlink(file2, link, false);
    try {
      wrapper.rename(link, file1);
      fail("Renamed w/o passing overwrite");
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    wrapper.rename(link, file1, Rename.OVERWRITE);
    assertFalse(wrapper.exists(link));

    if (!emulatingSymlinksOnWindows()) {
      assertTrue(wrapper.getFileLinkStatus(file1).isSymlink());
      assertEquals(file2, wrapper.getLinkTarget(file1));
    }
  }

  @Test(timeout=10000)
  /** Rename a symlink to a directory that exists */
  public void testRenameSymlinkToExistingDir() throws IOException {
    Path dir1   = new Path(testBaseDir1());
    Path dir2   = new Path(testBaseDir2());
    Path subDir = new Path(testBaseDir2(), "subDir");
    Path link   = new Path(testBaseDir1(), "linkToDir");
    wrapper.createSymlink(dir1, link, false);
    try {
      wrapper.rename(link, dir2);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    try {
      wrapper.rename(link, dir2, Rename.OVERWRITE);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    // Also fails when dir2 has a sub-directory
    wrapper.mkdir(subDir, FsPermission.getDefault(), false);
    try {
      wrapper.rename(link, dir2, Rename.OVERWRITE);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
  }

  @Test(timeout=10000)
  /** Rename a symlink to itself */
  public void testRenameSymlinkToItself() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    createAndWriteFile(file);

    Path link = new Path(testBaseDir1(), "linkToFile1");
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.rename(link, link);
      fail("Failed to get expected IOException");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails with overwrite as well
    try {
      wrapper.rename(link, link, Rename.OVERWRITE);
      fail("Failed to get expected IOException");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
  }

  @Test(timeout=10000)
  /** Rename a symlink */
  public void testRenameSymlink() throws IOException {
    assumeTrue(!emulatingSymlinksOnWindows());
    Path file  = new Path(testBaseDir1(), "file");
    Path link1 = new Path(testBaseDir1(), "linkToFile1");
    Path link2 = new Path(testBaseDir1(), "linkToFile2");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link1, false);
    wrapper.rename(link1, link2);
    assertTrue(wrapper.getFileLinkStatus(link2).isSymlink());
    assertFalse(wrapper.getFileStatus(link2).isDirectory());
    readFile(link2);
    readFile(file);
    try {
      createAndWriteFile(link2);
      fail("link was not renamed");
    } catch (IOException x) {
      // Expected
    }
  }

  @Test(timeout=10000)
  /** Rename a symlink to the file it links to */
  public void testRenameSymlinkToFileItLinksTo() throws IOException {
    /* NB: The rename is not atomic, so file is deleted before renaming
     * linkToFile. In this interval linkToFile is dangling and local file
     * system does not handle dangling links because File.exists returns
     * false for dangling links. */
    if ("file".equals(getScheme())) {
      return;
    }
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    try {
      wrapper.rename(link, file);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(wrapper.isFile(file));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isSymlink(link));
    assertEquals(file, wrapper.getLinkTarget(link));
    try {
      wrapper.rename(link, file, Rename.OVERWRITE);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(wrapper.isFile(file));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isSymlink(link));
    assertEquals(file, wrapper.getLinkTarget(link));
  }

  @Test(timeout=10000)
  /** Rename a symlink to the directory it links to */
  public void testRenameSymlinkToDirItLinksTo() throws IOException {
    /* NB: The rename is not atomic, so dir is deleted before renaming
     * linkToFile. In this interval linkToFile is dangling and local file
     * system does not handle dangling links because File.exists returns
     * false for dangling links. */
    if ("file".equals(getScheme())) {
      return;
    }
    Path dir  = new Path(testBaseDir1(), "dir");
    Path link = new Path(testBaseDir1(), "linkToDir");
    wrapper.mkdir(dir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(dir, link, false);
    try {
      wrapper.rename(link, dir);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(wrapper.isDir(dir));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isSymlink(link));
    assertEquals(dir, wrapper.getLinkTarget(link));
    try {
      wrapper.rename(link, dir, Rename.OVERWRITE);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(wrapper.isDir(dir));
    assertTrue(wrapper.exists(link));
    assertTrue(wrapper.isSymlink(link));
    assertEquals(dir, wrapper.getLinkTarget(link));
  }

  @Test(timeout=10000)
  /** Test rename the symlink's target */
  public void testRenameLinkTarget() throws IOException {
    assumeTrue(!emulatingSymlinksOnWindows());
    Path file    = new Path(testBaseDir1(), "file");
    Path fileNew = new Path(testBaseDir1(), "fileNew");
    Path link    = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    wrapper.rename(file, fileNew, Rename.OVERWRITE);
    try {
      readFile(link);
      fail("Link should be dangling");
    } catch (IOException x) {
      // Expected
    }
    wrapper.rename(fileNew, file, Rename.OVERWRITE);
    readFile(link);
  }

  @Test(timeout=10000)
  /** Test rename a file to path with destination that has symlink parent */
  public void testRenameFileWithDestParentSymlink() throws IOException {
    Path link  = new Path(testBaseDir1(), "link");
    Path file1 = new Path(testBaseDir1(), "file1");
    Path file2 = new Path(testBaseDir1(), "file2");
    Path file3 = new Path(link, "file3");
    Path dir2  = new Path(testBaseDir2());

    // Renaming /dir1/file1 to non-existant file /dir1/link/file3 is OK
    // if link points to a directory...
    wrapper.createSymlink(dir2, link, false);
    createAndWriteFile(file1);
    wrapper.rename(file1, file3);
    assertFalse(wrapper.exists(file1));
    assertTrue(wrapper.exists(file3));
    wrapper.rename(file3, file1);

    // But fails if link is dangling...
    wrapper.delete(link, false);
    wrapper.createSymlink(file2, link, false);
    try {
      wrapper.rename(file1, file3);
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    // And if link points to a file...
    createAndWriteFile(file2);
    try {
      wrapper.rename(file1, file3);
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof ParentNotDirectoryException);
    }
  }

  @Test(timeout=10000)
  /**
   * Create, write, read, append, rename, get the block locations,
   * checksums, and delete a file using a path with a symlink as an
   * intermediate path component where the link target was specified
   * using an absolute path. Rename is covered in more depth below.
   */
  public void testAccessFileViaInterSymlinkAbsTarget() throws IOException {
    Path baseDir        = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path fileNew        = new Path(baseDir, "fileNew");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink    = new Path(linkToDir, "file");
    Path fileNewViaLink = new Path(linkToDir, "fileNew");
    wrapper.createSymlink(baseDir, linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertTrue(wrapper.exists(fileViaLink));
    assertTrue(wrapper.isFile(fileViaLink));
    assertFalse(wrapper.isDir(fileViaLink));
    assertFalse(wrapper.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(wrapper.isDir(fileViaLink));
    assertEquals(wrapper.getFileStatus(file),
                 wrapper.getFileLinkStatus(file));
    assertEquals(wrapper.getFileStatus(fileViaLink),
                 wrapper.getFileLinkStatus(fileViaLink));
    readFile(fileViaLink);
    appendToFile(fileViaLink);
    wrapper.rename(fileViaLink, fileNewViaLink);
    assertFalse(wrapper.exists(fileViaLink));
    assertTrue(wrapper.exists(fileNewViaLink));
    readFile(fileNewViaLink);
    assertEquals(wrapper.getFileBlockLocations(fileNew, 0, 1).length,
                 wrapper.getFileBlockLocations(fileNewViaLink, 0, 1).length);
    assertEquals(wrapper.getFileChecksum(fileNew),
                 wrapper.getFileChecksum(fileNewViaLink));
    wrapper.delete(fileNewViaLink, true);
    assertFalse(wrapper.exists(fileNewViaLink));
  }

  @Test(timeout=10000)
  /**
   * Operate on a file using a path with an intermediate symlink where
   * the link target was specified as a fully qualified path.
   */
  public void testAccessFileViaInterSymlinkQualTarget() throws IOException {
    Path baseDir        = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink    = new Path(linkToDir, "file");
    wrapper.createSymlink(wrapper.makeQualified(baseDir), linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertEquals(wrapper.getFileStatus(file),
                 wrapper.getFileLinkStatus(file));
    assertEquals(wrapper.getFileStatus(fileViaLink),
                 wrapper.getFileLinkStatus(fileViaLink));
    readFile(fileViaLink);
  }

  @Test(timeout=10000)
  /**
   * Operate on a file using a path with an intermediate symlink where
   * the link target was specified as a relative path.
   */
  public void testAccessFileViaInterSymlinkRelTarget() throws IOException {
    assumeTrue(!"file".equals(getScheme()));
    Path dir         = new Path(testBaseDir1(), "dir");
    Path file        = new Path(dir, "file");
    Path linkToDir   = new Path(testBaseDir1(), "linkToDir");
    Path fileViaLink = new Path(linkToDir, "file");

    wrapper.mkdir(dir, FileContext.DEFAULT_PERM, false);
    wrapper.createSymlink(new Path("dir"), linkToDir, false);
    createAndWriteFile(fileViaLink);
    // Note that getFileStatus returns fully qualified paths even
    // when called on an absolute path.
    assertEquals(wrapper.makeQualified(file),
                 wrapper.getFileStatus(file).getPath());
    // In each case getFileLinkStatus returns the same FileStatus
    // as getFileStatus since we're not calling it on a link and
    // FileStatus objects are compared by Path.
    assertEquals(wrapper.getFileStatus(file),
                 wrapper.getFileLinkStatus(file));
    assertEquals(wrapper.getFileStatus(fileViaLink),
                 wrapper.getFileLinkStatus(fileViaLink));
    assertEquals(wrapper.getFileStatus(fileViaLink),
                 wrapper.getFileLinkStatus(file));
  }

  @Test(timeout=10000)
  /** Test create, list, and delete a directory through a symlink */
  public void testAccessDirViaSymlink() throws IOException {
    Path baseDir    = new Path(testBaseDir1());
    Path dir        = new Path(testBaseDir1(), "dir");
    Path linkToDir  = new Path(testBaseDir2(), "linkToDir");
    Path dirViaLink = new Path(linkToDir, "dir");
    wrapper.createSymlink(baseDir, linkToDir, false);
    wrapper.mkdir(dirViaLink, FileContext.DEFAULT_PERM, true);
    assertTrue(wrapper.getFileStatus(dirViaLink).isDirectory());
    FileStatus[] stats = wrapper.listStatus(dirViaLink);
    assertEquals(0, stats.length);
    RemoteIterator<FileStatus> statsItor = wrapper.listStatusIterator(dirViaLink);
    assertFalse(statsItor.hasNext());
    wrapper.delete(dirViaLink, false);
    assertFalse(wrapper.exists(dirViaLink));
    assertFalse(wrapper.exists(dir));
  }

  @Test(timeout=10000)
  /** setTimes affects the target not the link */
  public void testSetTimes() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    wrapper.createSymlink(file, link, false);
    long at = wrapper.getFileLinkStatus(link).getAccessTime();
    wrapper.setTimes(link, 2L, 3L);
    // NB: local file systems don't implement setTimes
    if (!"file".equals(getScheme())) {
      assertEquals(at, wrapper.getFileLinkStatus(link).getAccessTime());
      assertEquals(3, wrapper.getFileStatus(file).getAccessTime());
      assertEquals(2, wrapper.getFileStatus(file).getModificationTime());
    }
  }
}
