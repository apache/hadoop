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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import static org.apache.hadoop.fs.FileContextTestHelper.*;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

/**
 * Test symbolic links using FileContext.
 */
public abstract class FileContextSymlinkBaseTest {
  static final long seed = 0xDEADBEEFL;
  static final int  blockSize =  8192;
  static final int  fileSize  = 16384;
 
  protected final FileContextTestHelper fileContextTestHelper = new FileContextTestHelper();
  protected static FileContext fc;

  abstract protected String getScheme();
  abstract protected String testBaseDir1() throws IOException;
  abstract protected String testBaseDir2() throws IOException;
  abstract protected URI testURI();

  protected IOException unwrapException(IOException e) {
    return e;
  }

  protected static void createAndWriteFile(FileContext fc, Path p) 
      throws IOException {
    createFile(fc, p, fileSize / blockSize,
        CreateOpts.createParent(),
        CreateOpts.repFac((short) 1),
        CreateOpts.blockSize(blockSize));
  }

  protected static void createAndWriteFile(Path p) throws IOException {
    createAndWriteFile(fc, p);
  }

  protected static void readFile(Path p) throws IOException {
    FileContextTestHelper.readFile(fc, p, fileSize);
  }

  protected static void readFile(FileContext fc, Path p) throws IOException {
    FileContextTestHelper.readFile(fc, p, fileSize);
  }

  protected static void appendToFile(Path p) throws IOException {
    FileContextTestHelper.appendToFile(fc, p, fileSize / blockSize, 
        CreateOpts.blockSize(blockSize));
  }

  @Before
  public void setUp() throws Exception {
    fc.mkdir(new Path(testBaseDir1()), FileContext.DEFAULT_PERM, true);
    fc.mkdir(new Path(testBaseDir2()), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception { 
    fc.delete(new Path(testBaseDir1()), true);
    fc.delete(new Path(testBaseDir2()), true);
  } 

  @Test
  /** The root is not a symlink */
  public void testStatRoot() throws IOException {
    assertFalse(fc.getFileLinkStatus(new Path("/")).isSymlink());    
  }

  @Test
  /** Test setWorkingDirectory not resolves symlinks */
  public void testSetWDNotResolvesLinks() throws IOException {
    Path dir       = new Path(testBaseDir1());
    Path linkToDir = new Path(testBaseDir1()+"/link");
    fc.createSymlink(dir, linkToDir, false);
    fc.setWorkingDirectory(linkToDir);
    assertEquals(linkToDir.getName(), fc.getWorkingDirectory().getName());
  }

  @Test
  /** Test create a dangling link */
  public void testCreateDanglingLink() throws IOException {
    Path file = new Path("/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");    
    fc.createSymlink(file, link, false);
    try {
      fc.getFileStatus(link);
      fail("Got file status of non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    fc.delete(link, false);
  } 

  @Test
  /** Test create a link to null and empty path */
  public void testCreateLinkToNullEmpty() throws IOException {
    Path link = new Path(testBaseDir1()+"/link");
    try {
      fc.createSymlink(null, link, false);
      fail("Can't create symlink to null");
    } catch (java.lang.NullPointerException e) {
      // Expected, create* with null yields NPEs
    }
    try {
      fc.createSymlink(new Path(""), link, false);
      fail("Can't create symlink to empty string");
    } catch (java.lang.IllegalArgumentException e) {
      // Expected, Path("") is invalid
    }
  } 
    
  @Test
  /** Create a link with createParent set */
  public void testCreateLinkCanCreateParent() throws IOException {
    Path file = new Path(testBaseDir1()+"/file");
    Path link = new Path(testBaseDir2()+"/linkToFile");
    createAndWriteFile(file);
    fc.delete(new Path(testBaseDir2()), true);
    try {
      fc.createSymlink(file, link, false);
      fail("Created link without first creating parent dir");
    } catch (IOException x) {
      // Expected. Need to create testBaseDir2() first.
    }
    assertFalse(exists(fc, new Path(testBaseDir2())));
    fc.createSymlink(file, link, true);
    readFile(link);
  }

  @Test
  /** Try to create a directory given a path that refers to a symlink */
  public void testMkdirExistingLink() throws IOException {
    Path dir  = new Path(testBaseDir1()+"/link");
    fc.createSymlink(new Path("/doesNotExist"), dir, false);
    try {
      fc.mkdir(dir, FileContext.DEFAULT_PERM, false);
      fail("Created a dir where a symlink exists");
    } catch (FileAlreadyExistsException e) {
      // Expected. The symlink already exists.
    } catch (IOException e) {
      // LocalFs just throws an IOException
      assertEquals("file", getScheme());
    }
  }

  @Test
  /** Try to create a file with parent that is a dangling link */
  public void testCreateFileViaDanglingLinkParent() throws IOException {
    Path dir  = new Path(testBaseDir1()+"/dangling");
    Path file = new Path(testBaseDir1()+"/dangling/file");
    fc.createSymlink(new Path("/doesNotExist"), dir, false);
    FSDataOutputStream out;
    try {
      out = fc.create(file, EnumSet.of(CreateFlag.CREATE), 
                      CreateOpts.repFac((short) 1),
                      CreateOpts.blockSize(blockSize));
      out.close();
      fail("Created a link with dangling link parent");
    } catch (FileNotFoundException e) {
      // Expected. The parent is dangling.
    }
  }

  @Test
  /** Delete a link */
  public void testDeleteLink() throws IOException {
    Path file = new Path(testBaseDir1()+"/file");
    Path link = new Path(testBaseDir1()+"/linkToFile");    
    createAndWriteFile(file);  
    fc.createSymlink(file, link, false);
    readFile(link);
    fc.delete(link, false);
    try {
      readFile(link);
      fail("Symlink should have been deleted");
    } catch (IOException x) {
      // Expected
    }
    // If we deleted the link we can put it back
    fc.createSymlink(file, link, false);    
  }
  
  @Test
  /** Ensure open resolves symlinks */
  public void testOpenResolvesLinks() throws IOException {
    Path file = new Path(testBaseDir1()+"/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");
    fc.createSymlink(file, link, false);
    try {
      fc.open(link);
      fail("link target does not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
    fc.delete(link, false);
  }

  @Test
  /** Stat a link to a file */
  public void testStatLinkToFile() throws IOException {
    Path file = new Path(testBaseDir1()+"/file");
    Path linkToFile = new Path(testBaseDir1()+"/linkToFile");    
    createAndWriteFile(file);
    fc.createSymlink(file, linkToFile, false);
    assertFalse(fc.getFileLinkStatus(linkToFile).isDirectory());
    assertTrue(isSymlink(fc, linkToFile));
    assertTrue(isFile(fc, linkToFile));
    assertFalse(isDir(fc, linkToFile));
    assertEquals(file.toUri().getPath(), 
                 fc.getLinkTarget(linkToFile).toString());
    // The local file system does not fully resolve the link
    // when obtaining the file status
    if (!"file".equals(getScheme())) {
      assertEquals(fc.getFileStatus(file), fc.getFileStatus(linkToFile));
      assertEquals(fc.makeQualified(file),
                   fc.getFileStatus(linkToFile).getPath());
      assertEquals(fc.makeQualified(linkToFile),
                   fc.getFileLinkStatus(linkToFile).getPath());
    }
  }

  @Test
  /** Stat a relative link to a file */
  public void testStatRelLinkToFile() throws IOException {
    assumeTrue(!"file".equals(getScheme()));
    Path baseDir    = new Path(testBaseDir1());
    Path file       = new Path(testBaseDir1(), "file");
    Path linkToFile = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(new Path("file"), linkToFile, false);
    assertEquals(fc.getFileStatus(file), fc.getFileStatus(linkToFile));
    assertEquals(fc.makeQualified(file),
                 fc.getFileStatus(linkToFile).getPath());
    assertEquals(fc.makeQualified(linkToFile),
                 fc.getFileLinkStatus(linkToFile).getPath());
  }

  @Test
  /** Stat a link to a directory */
  public void testStatLinkToDir() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path linkToDir = new Path(testBaseDir1()+"/linkToDir");
    fc.createSymlink(dir, linkToDir, false);

    assertFalse(fc.getFileStatus(linkToDir).isSymlink());
    assertTrue(isDir(fc, linkToDir));
    assertFalse(fc.getFileLinkStatus(linkToDir).isDirectory());
    assertTrue(fc.getFileLinkStatus(linkToDir).isSymlink());

    assertFalse(isFile(fc, linkToDir));
    assertTrue(isDir(fc, linkToDir));

    assertEquals(dir.toUri().getPath(), 
                 fc.getLinkTarget(linkToDir).toString());
  }

  @Test
  /** Stat a dangling link */
  public void testStatDanglingLink() throws IOException {
    Path file = new Path("/noSuchFile");
    Path link = new Path(testBaseDir1()+"/link");    
    fc.createSymlink(file, link, false);
    assertFalse(fc.getFileLinkStatus(link).isDirectory());
    assertTrue(fc.getFileLinkStatus(link).isSymlink());
  }
  
  @Test
  /** Stat a non-existant file */
  public void testStatNonExistantFiles() throws IOException {
    Path fileAbs = new Path("/doesNotExist");
    try {
      fc.getFileLinkStatus(fileAbs);
      fail("Got FileStatus for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    try {
      fc.getLinkTarget(fileAbs);
      fail("Got link target for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
  }

  @Test
  /** Test stat'ing a regular file and directory */
  public void testStatNonLinks() throws IOException {
    Path dir   = new Path(testBaseDir1());
    Path file  = new Path(testBaseDir1()+"/file");
    createAndWriteFile(file);
    try {
      fc.getLinkTarget(dir);
      fail("Lstat'd a non-symlink");
    } catch (IOException e) {
      // Expected.
    }
    try {
      fc.getLinkTarget(file);
      fail("Lstat'd a non-symlink");
    } catch (IOException e) {
      // Expected.
    }
  }
  
  @Test
  /** Test links that link to each other */
  public void testRecursiveLinks() throws IOException {
    Path link1 = new Path(testBaseDir1()+"/link1");
    Path link2 = new Path(testBaseDir1()+"/link2");
    fc.createSymlink(link1, link2, false);
    fc.createSymlink(link2, link1, false);
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
    Path dir = new Path(testBaseDir1());
    // isFile/Directory
    assertTrue(isFile(fc, linkAbs));
    assertFalse(isDir(fc, linkAbs));

    // Check getFileStatus 
    assertFalse(fc.getFileStatus(linkAbs).isSymlink());
    assertFalse(fc.getFileStatus(linkAbs).isDirectory());
    assertEquals(fileSize, fc.getFileStatus(linkAbs).getLen());

    // Check getFileLinkStatus
    assertTrue(isSymlink(fc, linkAbs));
    assertFalse(fc.getFileLinkStatus(linkAbs).isDirectory());

    // Check getSymlink always returns a qualified target, except
    // when partially qualified paths are used (see tests below).
    assertEquals(targetQual.toString(), 
        fc.getFileLinkStatus(linkAbs).getSymlink().toString());
    assertEquals(targetQual, fc.getFileLinkStatus(linkAbs).getSymlink());
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
    assertEquals(expectedTarget, fc.getLinkTarget(linkAbs));
    
    // Now read using all path types..
    fc.setWorkingDirectory(dir);    
    readFile(new Path("linkToFile"));
    readFile(linkAbs);
    // And fully qualified.. (NB: for local fs this is partially qualified)
    readFile(new Path(testURI().toString(), linkAbs));
    // And partially qualified..
    boolean failureExpected = "file".equals(getScheme()) ? false : true;
    try {
      readFile(new Path(getScheme()+"://"+testBaseDir1()+"/linkToFile"));
      assertFalse(failureExpected);
    } catch (Exception e) {
      assertTrue(failureExpected);
    }
    
    // Now read using a different file context (for HDFS at least)
    if (!"file".equals(getScheme())) {
      FileContext localFc = FileContext.getLocalFSFileContext();
      readFile(localFc, new Path(testURI().toString(), linkAbs));
    }
  }
  
  @Test
  /** Test creating a symlink using relative paths */
  public void testCreateLinkUsingRelPaths() throws IOException {
    Path fileAbs = new Path(testBaseDir1(), "file");
    Path linkAbs = new Path(testBaseDir1(), "linkToFile");
    Path schemeAuth = new Path(testURI().toString()); 
    Path fileQual = new Path(schemeAuth, testBaseDir1()+"/file");
    createAndWriteFile(fileAbs);
    
    fc.setWorkingDirectory(new Path(testBaseDir1()));
    fc.createSymlink(new Path("file"), new Path("linkToFile"), false);
    checkLink(linkAbs, new Path("file"), fileQual);
    
    // Now rename the link's parent. Because the target was specified 
    // with a relative path the link should still resolve.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    Path fileViaDir2 = new Path(schemeAuth, testBaseDir2()+"/file");
    fc.rename(dir1, dir2, Rename.OVERWRITE);
    assertEquals(fileViaDir2, fc.getFileLinkStatus(linkViaDir2).getSymlink());
    readFile(linkViaDir2);
  }

  @Test
  /** Test creating a symlink using absolute paths */
  public void testCreateLinkUsingAbsPaths() throws IOException {
    Path fileAbs = new Path(testBaseDir1()+"/file");
    Path linkAbs = new Path(testBaseDir1()+"/linkToFile");
    Path schemeAuth = new Path(testURI().toString()); 
    Path fileQual = new Path(schemeAuth, testBaseDir1()+"/file");
    createAndWriteFile(fileAbs);

    fc.createSymlink(fileAbs, linkAbs, false);
    checkLink(linkAbs, fileAbs, fileQual);

    // Now rename the link's parent. The target doesn't change and
    // now no longer exists so accessing the link should fail.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    fc.rename(dir1, dir2, Rename.OVERWRITE);
    assertEquals(fileQual, fc.getFileLinkStatus(linkViaDir2).getSymlink());    
    try {
      readFile(linkViaDir2);
      fail("The target should not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
  } 
  
  @Test
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
    
    fc.createSymlink(fileQual, linkQual, false);
    checkLink(linkAbs, 
              "file".equals(getScheme()) ? fileAbs : fileQual, 
              fileQual);
    
    // Now rename the link's parent. The target doesn't change and
    // now no longer exists so accessing the link should fail.
    Path dir1        = new Path(testBaseDir1());
    Path dir2        = new Path(testBaseDir2());
    Path linkViaDir2 = new Path(testBaseDir2(), "linkToFile");
    fc.rename(dir1, dir2, Rename.OVERWRITE);    
    assertEquals(fileQual, fc.getFileLinkStatus(linkViaDir2).getSymlink());    
    try {
      readFile(linkViaDir2);
      fail("The target should not exist");
    } catch (FileNotFoundException x) {
      // Expected
    }
  } 
    
  @Test
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
    FileContext localFc = FileContext.getLocalFSFileContext();

    fc.createSymlink(fileWoHost, link, false);
    // Partially qualified path is stored
    assertEquals(fileWoHost, fc.getLinkTarget(linkQual));    
    // NB: We do not add an authority
    assertEquals(fileWoHost.toString(),
      fc.getFileLinkStatus(link).getSymlink().toString());
    assertEquals(fileWoHost.toString(),
      fc.getFileLinkStatus(linkQual).getSymlink().toString());
    // Ditto even from another file system
    assertEquals(fileWoHost.toString(),
      localFc.getFileLinkStatus(linkQual).getSymlink().toString());
    // Same as if we accessed a partially qualified path directly
    try { 
      readFile(link);
      fail("DFS requires URIs with schemes have an authority");
    } catch (java.lang.RuntimeException e) {
      // Expected
    }
  }

  @Test
  /** Same as above but vice versa (authority but no scheme) */
  public void testCreateLinkUsingPartQualPath2() throws IOException {
    Path link         = new Path(testBaseDir1(), "linkToFile");
    Path fileWoScheme = new Path("//"+testURI().getAuthority()+ 
                                 testBaseDir1()+"/file");
    if ("file".equals(getScheme())) {
      return;
    }
    fc.createSymlink(fileWoScheme, link, false);
    assertEquals(fileWoScheme, fc.getLinkTarget(link));
    assertEquals(fileWoScheme.toString(),
      fc.getFileLinkStatus(link).getSymlink().toString());
    try {
      readFile(link);
      fail("Accessed a file with w/o scheme");
    } catch (IOException e) {
      // Expected      
      assertEquals("No AbstractFileSystem for scheme: null", e.getMessage());
    }
  }

  @Test
  /** Lstat and readlink on a normal file and directory */
  public void testLinkStatusAndTargetWithNonLink() throws IOException {
    Path schemeAuth = new Path(testURI().toString());
    Path dir        = new Path(testBaseDir1());
    Path dirQual    = new Path(schemeAuth, dir.toString());
    Path file       = new Path(testBaseDir1(), "file");
    Path fileQual   = new Path(schemeAuth, file.toString());
    createAndWriteFile(file);
    assertEquals(fc.getFileStatus(file), fc.getFileLinkStatus(file));
    assertEquals(fc.getFileStatus(dir), fc.getFileLinkStatus(dir));
    try {
      fc.getLinkTarget(file);
      fail("Get link target on non-link should throw an IOException");
    } catch (IOException x) {
      assertEquals("Path "+fileQual+" is not a symbolic link", x.getMessage());
    }
    try {
      fc.getLinkTarget(dir);
      fail("Get link target on non-link should throw an IOException");
    } catch (IOException x) {
      assertEquals("Path "+dirQual+" is not a symbolic link", x.getMessage());
    }    
  }

  @Test
  /** Test create symlink to a directory */
  public void testCreateLinkToDirectory() throws IOException {
    Path dir1      = new Path(testBaseDir1());
    Path file      = new Path(testBaseDir1(), "file");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    createAndWriteFile(file);
    fc.createSymlink(dir1, linkToDir, false);
    assertFalse(isFile(fc, linkToDir));
    assertTrue(isDir(fc, linkToDir)); 
    assertTrue(fc.getFileStatus(linkToDir).isDirectory());
    assertTrue(fc.getFileLinkStatus(linkToDir).isSymlink());
  }
  
  @Test
  /** Test create and remove a file through a symlink */
  public void testCreateFileViaSymlink() throws IOException {
    Path dir         = new Path(testBaseDir1());
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink = new Path(linkToDir, "file");
    fc.createSymlink(dir, linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertTrue(isFile(fc, fileViaLink));
    assertFalse(isDir(fc, fileViaLink));
    assertFalse(fc.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(fc.getFileStatus(fileViaLink).isDirectory());
    readFile(fileViaLink);
    fc.delete(fileViaLink, true);
    assertFalse(exists(fc, fileViaLink));
  }
  
  @Test
  /** Test make and delete directory through a symlink */
  public void testCreateDirViaSymlink() throws IOException {
    Path dir1          = new Path(testBaseDir1());
    Path subDir        = new Path(testBaseDir1(), "subDir");
    Path linkToDir     = new Path(testBaseDir2(), "linkToDir");
    Path subDirViaLink = new Path(linkToDir, "subDir");
    fc.createSymlink(dir1, linkToDir, false);
    fc.mkdir(subDirViaLink, FileContext.DEFAULT_PERM, true);
    assertTrue(isDir(fc, subDirViaLink));
    fc.delete(subDirViaLink, false);
    assertFalse(exists(fc, subDirViaLink));
    assertFalse(exists(fc, subDir));
  }

  @Test
  /** Create symlink through a symlink */
  public void testCreateLinkViaLink() throws IOException {
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
    fc.createSymlink(dir1, linkToDir, false);
    fc.createSymlink(fileViaLink, linkToFile, false);
    assertTrue(isFile(fc, linkToFile));
    assertTrue(fc.getFileLinkStatus(linkToFile).isSymlink());
    readFile(linkToFile);
    assertEquals(fileSize, fc.getFileStatus(linkToFile).getLen());
    assertEquals(fileViaLink, fc.getLinkTarget(linkToFile));
  }

  @Test
  /** Test create symlink to a directory */
  public void testListStatusUsingLink() throws IOException {
    Path file  = new Path(testBaseDir1(), "file");
    Path link  = new Path(testBaseDir1(), "link");
    createAndWriteFile(file);
    fc.createSymlink(new Path(testBaseDir1()), link, false);
    // The size of the result is file system dependent, Hdfs is 2 (file 
    // and link) and LocalFs is 3 (file, link, file crc).
    FileStatus[] stats = fc.util().listStatus(link);
    assertTrue(stats.length == 2 || stats.length == 3);
    RemoteIterator<FileStatus> statsItor = fc.listStatus(link);
    int dirLen = 0;
    while(statsItor.hasNext()) {
      statsItor.next();
      dirLen++;
    }
    assertTrue(dirLen == 2 || dirLen == 3);
  }
  
  @Test
  /** Test create symlink using the same path */
  public void testCreateLinkTwice() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    try {
      fc.createSymlink(file, link, false);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }
  } 
  
  @Test
  /** Test access via a symlink to a symlink */
  public void testCreateLinkToLink() throws IOException {
    Path dir1        = new Path(testBaseDir1());
    Path file        = new Path(testBaseDir1(), "file");
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path linkToLink  = new Path(testBaseDir2(), "linkToLink");
    Path fileViaLink = new Path(testBaseDir2(), "linkToLink/file");
    createAndWriteFile(file);
    fc.createSymlink(dir1, linkToDir, false);
    fc.createSymlink(linkToDir, linkToLink, false);
    assertTrue(isFile(fc, fileViaLink));
    assertFalse(isDir(fc, fileViaLink));
    assertFalse(fc.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(fc.getFileStatus(fileViaLink).isDirectory());
    readFile(fileViaLink);
  }

  @Test
  /** Can not create a file with path that refers to a symlink */
  public void testCreateFileDirExistingLink() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    try {
      createAndWriteFile(link);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }
    try {
      fc.mkdir(link, FsPermission.getDefault(), false);
      fail("link already exists");
    } catch (IOException x) {
      // Expected
    }    
  } 

  @Test
  /** Test deleting and recreating a symlink */
  public void testUseLinkAferDeleteLink() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    fc.delete(link, false);
    try {
      readFile(link);        
      fail("link was deleted");
    } catch (IOException x) {
      // Expected
    }
    readFile(file);
    fc.createSymlink(file, link, false);
    readFile(link);    
  } 
  
  
  @Test
  /** Test create symlink to . */
  public void testCreateLinkToDot() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");    
    Path link = new Path(testBaseDir1(), "linkToDot");
    createAndWriteFile(file);    
    fc.setWorkingDirectory(dir);
    try {
      fc.createSymlink(new Path("."), link, false);
      fail("Created symlink to dot");
    } catch (IOException x) {
      // Expected. Path(".") resolves to "" because URI normalizes
      // the dot away and AbstractFileSystem considers "" invalid.  
    }
  }

  @Test
  /** Test create symlink to .. */
  public void testCreateLinkToDotDot() throws IOException {
    Path file        = new Path(testBaseDir1(), "test/file");
    Path dotDot      = new Path(testBaseDir1(), "test/..");
    Path linkToDir   = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink = new Path(linkToDir,      "test/file");
    // Symlink to .. is not a problem since the .. is squashed early
    assertEquals(testBaseDir1(), dotDot.toString());
    createAndWriteFile(file);
    fc.createSymlink(dotDot, linkToDir, false);
    readFile(fileViaLink);
    assertEquals(fileSize, fc.getFileStatus(fileViaLink).getLen());    
  }

  @Test
  /** Test create symlink to ../file */
  public void testCreateLinkToDotDotPrefix() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path dir  = new Path(testBaseDir1(), "test");
    Path link = new Path(testBaseDir1(), "test/link");
    createAndWriteFile(file);
    fc.mkdir(dir, FsPermission.getDefault(), false);
    fc.setWorkingDirectory(dir);
    fc.createSymlink(new Path("../file"), link, false);
    readFile(link);
    assertEquals(new Path("../file"), fc.getLinkTarget(link));
  }
  
  @Test
  /** Test rename file using a path that contains a symlink. The rename should 
   * work as if the path did not contain a symlink */
  public void testRenameFileViaSymlink() throws IOException {
    Path dir            = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink    = new Path(linkToDir, "file");
    Path fileNewViaLink = new Path(linkToDir, "fileNew");
    createAndWriteFile(file);
    fc.createSymlink(dir, linkToDir, false);
    fc.rename(fileViaLink, fileNewViaLink);
    assertFalse(exists(fc, fileViaLink));
    assertFalse(exists(fc, file));
    assertTrue(exists(fc, fileNewViaLink));
  }

  @Test
  /** Test rename a file through a symlink but this time only the 
   * destination path has an intermediate symlink. The rename should work 
   * as if the path did not contain a symlink */
  public void testRenameFileToDestViaSymlink() throws IOException {
    Path dir       = new Path(testBaseDir1());
    Path file      = new Path(testBaseDir1(), "file");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    Path subDir    = new Path(linkToDir, "subDir");
    createAndWriteFile(file);
    fc.createSymlink(dir, linkToDir, false);
    fc.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    try {
      fc.rename(file, subDir);
      fail("Renamed file to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(exists(fc, file));
  }
  
  @Test
  /** Similar tests as the previous ones but rename a directory */
  public void testRenameDirViaSymlink() throws IOException {
    Path baseDir       = new Path(testBaseDir1());
    Path dir           = new Path(baseDir, "dir");
    Path linkToDir     = new Path(testBaseDir2(), "linkToDir");
    Path dirViaLink    = new Path(linkToDir, "dir");
    Path dirNewViaLink = new Path(linkToDir, "dirNew");
    fc.mkdir(dir, FileContext.DEFAULT_PERM, false);
    fc.createSymlink(baseDir, linkToDir, false);
    assertTrue(exists(fc, dirViaLink));
    fc.rename(dirViaLink, dirNewViaLink);
    assertFalse(exists(fc, dirViaLink));
    assertFalse(exists(fc, dir));
    assertTrue(exists(fc, dirNewViaLink));
  }
    
  @Test
  /** Similar tests as the previous ones but rename a symlink */  
  public void testRenameSymlinkViaSymlink() throws IOException {
    Path baseDir        = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path link           = new Path(testBaseDir1(), "link");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path linkViaLink    = new Path(linkToDir, "link");
    Path linkNewViaLink = new Path(linkToDir, "linkNew");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    fc.createSymlink(baseDir, linkToDir, false);
    fc.rename(linkViaLink, linkNewViaLink);
    assertFalse(exists(fc, linkViaLink));
    // Check that we didn't rename the link target
    assertTrue(exists(fc, file));
    assertTrue(fc.getFileLinkStatus(linkNewViaLink).isSymlink());
    readFile(linkNewViaLink);
  }

  @Test
  /** Test rename a directory to a symlink to a directory */
  public void testRenameDirToSymlinkToDir() throws IOException {
    Path dir1      = new Path(testBaseDir1());
    Path subDir = new Path(testBaseDir2(), "subDir");
    Path linkToDir = new Path(testBaseDir2(), "linkToDir");
    fc.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    fc.createSymlink(subDir, linkToDir, false);
    try {
      fc.rename(dir1, linkToDir, Rename.OVERWRITE);
      fail("Renamed directory to a symlink");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(exists(fc, dir1));
    assertTrue(exists(fc, linkToDir));
  }

  @Test
  /** Test rename a directory to a symlink to a file */
  public void testRenameDirToSymlinkToFile() throws IOException {
    Path dir1 = new Path(testBaseDir1());
    Path file = new Path(testBaseDir2(), "file");
    Path linkToFile = new Path(testBaseDir2(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, linkToFile, false);
    try {
      fc.rename(dir1, linkToFile, Rename.OVERWRITE);
      fail("Renamed directory to a symlink");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(exists(fc, dir1));
    assertTrue(exists(fc, linkToFile));
  }

  @Test
  /** Test rename a directory to a dangling symlink */
  public void testRenameDirToDanglingSymlink() throws IOException {
    Path dir = new Path(testBaseDir1());
    Path link = new Path(testBaseDir2(), "linkToFile");
    fc.createSymlink(new Path("/doesNotExist"), link, false);
    try { 
      fc.rename(dir, link, Rename.OVERWRITE);
      fail("Renamed directory to a symlink"); 
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    assertTrue(exists(fc, dir));
    assertTrue(fc.getFileLinkStatus(link) != null);
  }

  @Test
  /** Test rename a file to a symlink to a directory */
  public void testRenameFileToSymlinkToDir() throws IOException {
    Path file   = new Path(testBaseDir1(), "file");
    Path subDir = new Path(testBaseDir1(), "subDir");
    Path link   = new Path(testBaseDir1(), "link");
    fc.mkdir(subDir, FileContext.DEFAULT_PERM, false);
    fc.createSymlink(subDir, link, false);
    createAndWriteFile(file);
    try {
      fc.rename(file, link);
      fail("Renamed file to symlink w/o overwrite"); 
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    fc.rename(file, link, Rename.OVERWRITE);
    assertFalse(exists(fc, file));
    assertTrue(exists(fc, link));
    assertTrue(isFile(fc, link));
    assertFalse(fc.getFileLinkStatus(link).isSymlink());
  }

  @Test
  /** Test rename a file to a symlink to a file */
  public void testRenameFileToSymlinkToFile() throws IOException {
    Path file1 = new Path(testBaseDir1(), "file1");
    Path file2 = new Path(testBaseDir1(), "file2");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file1);
    createAndWriteFile(file2);
    fc.createSymlink(file2, link, false);
    try {
      fc.rename(file1, link);
      fail("Renamed file to symlink w/o overwrite"); 
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    fc.rename(file1, link, Rename.OVERWRITE);
    assertFalse(exists(fc, file1));
    assertTrue(exists(fc, link));
    assertTrue(isFile(fc, link));
    assertFalse(fc.getFileLinkStatus(link).isSymlink());
  }

  @Test
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
    fc.createSymlink(new Path("/doesNotExist"), link, false);
    try {
      fc.rename(file1, link);  
    } catch (IOException e) {
      // Expected
    }
    fc.rename(file1, link, Rename.OVERWRITE);
    assertFalse(exists(fc, file1));
    assertTrue(exists(fc, link));
    assertTrue(isFile(fc, link));
    assertFalse(fc.getFileLinkStatus(link).isSymlink());    
  }

  @Test
  /** Rename a symlink to a new non-existant name */
  public void testRenameSymlinkNonExistantDest() throws IOException {
    Path file  = new Path(testBaseDir1(), "file");
    Path link1 = new Path(testBaseDir1(), "linkToFile1");
    Path link2 = new Path(testBaseDir1(), "linkToFile2");
    createAndWriteFile(file);
    fc.createSymlink(file, link1, false);
    fc.rename(link1, link2);
    assertTrue(fc.getFileLinkStatus(link2).isSymlink());
    readFile(link2);
    readFile(file);
    assertFalse(exists(fc, link1));
  }

  @Test
  /** Rename a symlink to a file that exists */
  public void testRenameSymlinkToExistingFile() throws IOException {
    Path file1 = new Path(testBaseDir1(), "file");
    Path file2 = new Path(testBaseDir1(), "someFile");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file1);
    createAndWriteFile(file2);
    fc.createSymlink(file2, link, false);
    try {
      fc.rename(link, file1);
      fail("Renamed w/o passing overwrite");
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    fc.rename(link, file1, Rename.OVERWRITE);
    assertFalse(exists(fc, link));
    assertTrue(fc.getFileLinkStatus(file1).isSymlink());
    assertEquals(file2, fc.getLinkTarget(file1));
  }

  @Test
  /** Rename a symlink to a directory that exists */
  public void testRenameSymlinkToExistingDir() throws IOException {
    Path dir1   = new Path(testBaseDir1());
    Path dir2   = new Path(testBaseDir2());
    Path subDir = new Path(testBaseDir2(), "subDir");
    Path link   = new Path(testBaseDir1(), "linkToDir");    
    fc.createSymlink(dir1, link, false);
    try {
      fc.rename(link, dir2);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    try {
      fc.rename(link, dir2, Rename.OVERWRITE);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
    // Also fails when dir2 has a sub-directory
    fc.mkdir(subDir, FsPermission.getDefault(), false);
    try {
      fc.rename(link, dir2, Rename.OVERWRITE);
      fail("Renamed link to a directory");
    } catch (IOException e) {
      // Expected. Both must be directories.
      assertTrue(unwrapException(e) instanceof IOException);
    }
  }

  @Test
  /** Rename a symlink to itself */
  public void testRenameSymlinkToItself() throws IOException {
    Path link = new Path(testBaseDir1(), "linkToFile1");
    fc.createSymlink(new Path("/doestNotExist"), link, false);
    try {
      fc.rename(link, link);
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Fails with overwrite as well
    try {
      fc.rename(link, link, Rename.OVERWRITE);
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
  }

  @Test
  /** Rename a symlink */
  public void testRenameSymlink() throws IOException {
    Path file  = new Path(testBaseDir1(), "file");
    Path link1 = new Path(testBaseDir1(), "linkToFile1");
    Path link2 = new Path(testBaseDir1(), "linkToFile2");    
    createAndWriteFile(file);
    fc.createSymlink(file, link1, false);
    fc.rename(link1, link2);
    assertTrue(fc.getFileLinkStatus(link2).isSymlink());
    assertFalse(fc.getFileStatus(link2).isDirectory());
    readFile(link2);
    readFile(file);
    try {
      createAndWriteFile(link2);
      fail("link was not renamed");
    } catch (IOException x) {
      // Expected
    } 
  }

  @Test
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
    fc.createSymlink(file, link, false);
    try {
      fc.rename(link, file);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(isFile(fc, file));
    assertTrue(exists(fc, link));
    assertTrue(isSymlink(fc, link));
    assertEquals(file, fc.getLinkTarget(link));
    try {
      fc.rename(link, file, Rename.OVERWRITE);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(isFile(fc, file));
    assertTrue(exists(fc, link));    
    assertTrue(isSymlink(fc, link));
    assertEquals(file, fc.getLinkTarget(link));    
  }
  
  @Test
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
    fc.mkdir(dir, FileContext.DEFAULT_PERM, false);
    fc.createSymlink(dir, link, false);
    try {
      fc.rename(link, dir);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(isDir(fc, dir));
    assertTrue(exists(fc, link));
    assertTrue(isSymlink(fc, link));
    assertEquals(dir, fc.getLinkTarget(link));
    try {
      fc.rename(link, dir, Rename.OVERWRITE);
      fail("Renamed symlink to its target");
    } catch (IOException e) {
      assertTrue(unwrapException(e) instanceof FileAlreadyExistsException);
    }
    // Check the rename didn't happen
    assertTrue(isDir(fc, dir));
    assertTrue(exists(fc, link));
    assertTrue(isSymlink(fc, link));
    assertEquals(dir, fc.getLinkTarget(link));
  }
  
  @Test
  /** Test rename the symlink's target */
  public void testRenameLinkTarget() throws IOException {
    Path file    = new Path(testBaseDir1(), "file");
    Path fileNew = new Path(testBaseDir1(), "fileNew");
    Path link    = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    fc.rename(file, fileNew, Rename.OVERWRITE);
    try {
      readFile(link);        
      fail("Link should be dangling");
    } catch (IOException x) {
      // Expected
    }
    fc.rename(fileNew, file, Rename.OVERWRITE);
    readFile(link);
  }

  @Test
  /** Test rename a file to path with destination that has symlink parent */
  public void testRenameFileWithDestParentSymlink() throws IOException {
    Path link  = new Path(testBaseDir1(), "link");
    Path file1 = new Path(testBaseDir1(), "file1");    
    Path file2 = new Path(testBaseDir1(), "file2");    
    Path file3 = new Path(link, "file3");
    Path dir2  = new Path(testBaseDir2());
    
    // Renaming /dir1/file1 to non-existant file /dir1/link/file3 is OK
    // if link points to a directory...
    fc.createSymlink(dir2, link, false);
    createAndWriteFile(file1);
    fc.rename(file1, file3);
    assertFalse(exists(fc, file1));
    assertTrue(exists(fc, file3));
    fc.rename(file3, file1);
    
    // But fails if link is dangling...
    fc.delete(link, false);
    fc.createSymlink(file2, link, false);    
    try {
      fc.rename(file1, file3);
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    // And if link points to a file...
    createAndWriteFile(file2);
    try {
      fc.rename(file1, file3);
    } catch (IOException e) {
      // Expected
      assertTrue(unwrapException(e) instanceof ParentNotDirectoryException);
    }    
  }
  
  @Test
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
    fc.createSymlink(baseDir, linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertTrue(exists(fc, fileViaLink));
    assertTrue(isFile(fc, fileViaLink));
    assertFalse(isDir(fc, fileViaLink));
    assertFalse(fc.getFileLinkStatus(fileViaLink).isSymlink());
    assertFalse(isDir(fc, fileViaLink));
    assertEquals(fc.getFileStatus(file),
                 fc.getFileLinkStatus(file));
    assertEquals(fc.getFileStatus(fileViaLink),
                 fc.getFileLinkStatus(fileViaLink));
    readFile(fileViaLink);
    appendToFile(fileViaLink);
    fc.rename(fileViaLink, fileNewViaLink);
    assertFalse(exists(fc, fileViaLink));
    assertTrue(exists(fc, fileNewViaLink));
    readFile(fileNewViaLink);
    assertEquals(fc.getFileBlockLocations(fileNew, 0, 1).length,
                 fc.getFileBlockLocations(fileNewViaLink, 0, 1).length);
    assertEquals(fc.getFileChecksum(fileNew),
                 fc.getFileChecksum(fileNewViaLink));
    fc.delete(fileNewViaLink, true);
    assertFalse(exists(fc, fileNewViaLink));
  }

  @Test
  /**
   * Operate on a file using a path with an intermediate symlink where
   * the link target was specified as a fully qualified path.
   */
  public void testAccessFileViaInterSymlinkQualTarget() throws IOException {
    Path baseDir        = new Path(testBaseDir1());
    Path file           = new Path(testBaseDir1(), "file");
    Path fileNew        = new Path(baseDir, "fileNew");
    Path linkToDir      = new Path(testBaseDir2(), "linkToDir");
    Path fileViaLink    = new Path(linkToDir, "file");
    Path fileNewViaLink = new Path(linkToDir, "fileNew");
    fc.createSymlink(fc.makeQualified(baseDir), linkToDir, false);
    createAndWriteFile(fileViaLink);
    assertEquals(fc.getFileStatus(file),
                 fc.getFileLinkStatus(file));
    assertEquals(fc.getFileStatus(fileViaLink),
                 fc.getFileLinkStatus(fileViaLink));
    readFile(fileViaLink);
  }

  @Test
  /**
   * Operate on a file using a path with an intermediate symlink where
   * the link target was specified as a relative path.
   */
  public void testAccessFileViaInterSymlinkRelTarget() throws IOException {
    assumeTrue(!"file".equals(getScheme()));
    Path baseDir     = new Path(testBaseDir1());
    Path dir         = new Path(testBaseDir1(), "dir");
    Path file        = new Path(dir, "file");
    Path linkToDir   = new Path(testBaseDir1(), "linkToDir");
    Path fileViaLink = new Path(linkToDir, "file");

    fc.mkdir(dir, FileContext.DEFAULT_PERM, false);
    fc.createSymlink(new Path("dir"), linkToDir, false);
    createAndWriteFile(fileViaLink);
    // Note that getFileStatus returns fully qualified paths even
    // when called on an absolute path.
    assertEquals(fc.makeQualified(file),
                 fc.getFileStatus(file).getPath());
    // In each case getFileLinkStatus returns the same FileStatus
    // as getFileStatus since we're not calling it on a link and
    // FileStatus objects are compared by Path.
    assertEquals(fc.getFileStatus(file),
                 fc.getFileLinkStatus(file));
    assertEquals(fc.getFileStatus(fileViaLink),
                 fc.getFileLinkStatus(fileViaLink));
    assertEquals(fc.getFileStatus(fileViaLink),
                 fc.getFileLinkStatus(file));
  }

  @Test
  /** Test create, list, and delete a directory through a symlink */
  public void testAccessDirViaSymlink() throws IOException {
    Path baseDir    = new Path(testBaseDir1());
    Path dir        = new Path(testBaseDir1(), "dir");
    Path linkToDir  = new Path(testBaseDir2(), "linkToDir");
    Path dirViaLink = new Path(linkToDir, "dir");
    fc.createSymlink(baseDir, linkToDir, false);
    fc.mkdir(dirViaLink, FileContext.DEFAULT_PERM, true);
    assertTrue(fc.getFileStatus(dirViaLink).isDirectory());
    FileStatus[] stats = fc.util().listStatus(dirViaLink);
    assertEquals(0, stats.length);
    RemoteIterator<FileStatus> statsItor = fc.listStatus(dirViaLink);
    assertFalse(statsItor.hasNext());
    fc.delete(dirViaLink, false);
    assertFalse(exists(fc, dirViaLink));
    assertFalse(exists(fc, dir));
  }

  @Test
  /** setTimes affects the target not the link */    
  public void testSetTimes() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    long at = fc.getFileLinkStatus(link).getAccessTime(); 
    fc.setTimes(link, 2L, 3L);
    // NB: local file systems don't implement setTimes
    if (!"file".equals(getScheme())) {
      assertEquals(at, fc.getFileLinkStatus(link).getAccessTime());
      assertEquals(3, fc.getFileStatus(file).getAccessTime());
      assertEquals(2, fc.getFileStatus(file).getModificationTime());
    }
  }
}
