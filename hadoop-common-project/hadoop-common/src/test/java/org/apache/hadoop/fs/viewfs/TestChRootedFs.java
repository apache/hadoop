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
package org.apache.hadoop.fs.viewfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ChRootedFs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class TestChRootedFs {
  FileContextTestHelper fileContextTestHelper = new FileContextTestHelper();
  FileContext fc; // The ChRoootedFs
  FileContext fcTarget; // 
  Path chrootedTo;

  @BeforeEach
  public void setUp() throws Exception {
    // create the test root on local_fs
    fcTarget = FileContext.getLocalFSFileContext();
    chrootedTo = fileContextTestHelper.getAbsoluteTestRootPath(fcTarget);
    // In case previous test was killed before cleanup
    fcTarget.delete(chrootedTo, true);
    
    fcTarget.mkdir(chrootedTo, FileContext.DEFAULT_PERM, true);

    Configuration conf = new Configuration();

    // ChRoot to the root of the testDirectory
    fc = FileContext.getFileContext(
        new ChRootedFs(fcTarget.getDefaultFileSystem(), chrootedTo), conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    fcTarget.delete(chrootedTo, true);
  }

  
  @Test
  public void testBasicPaths() {
    URI uri = fc.getDefaultFileSystem().getUri();
    assertEquals(chrootedTo.toUri(), uri);
    assertEquals(fc.makeQualified(
        new Path(System.getProperty("user.home"))),
        fc.getWorkingDirectory());
    assertEquals(fc.makeQualified(
        new Path(System.getProperty("user.home"))),
        fc.getHomeDirectory());
    /*
     * ChRootedFs as its uri like file:///chrootRoot.
     * This is questionable since path.makequalified(uri, path) ignores
     * the pathPart of a uri. So our notion of chrooted URI is questionable.
     * But if we were to fix Path#makeQualified() then  the next test should
     *  have been:

    assertEquals(
        new Path(chrootedTo + "/foo/bar").makeQualified(
            FsConstants.LOCAL_FS_URI, null),
        fc.makeQualified(new Path( "/foo/bar")));
    */
    
    assertEquals(
        new Path("/foo/bar").makeQualified(FsConstants.LOCAL_FS_URI, null),
        fc.makeQualified(new Path("/foo/bar")));
  }
  
  
  /** 
   * Test modify operations (create, mkdir, delete, etc) 
   * 
   * Verify the operation via chrootedfs (ie fc) and *also* via the
   *  target file system (ie fclocal) that has been chrooted.
   */
  @Test
  public void testCreateDelete() throws IOException {
    

    // Create file 
    fileContextTestHelper.createFileNonRecursive(fc, "/foo");
    assertTrue(isFile(fc, new Path("/foo")));
    assertTrue(isFile(fcTarget, new Path(chrootedTo, "foo")));
    
    // Create file with recursive dir
    fileContextTestHelper.createFile(fc, "/newDir/foo");
    assertTrue(isFile(fc, new Path("/newDir/foo")));
    assertTrue(isFile(fcTarget, new Path(chrootedTo, "newDir/foo")));
    
    // Delete the created file
    assertTrue(fc.delete(new Path("/newDir/foo"), false));
    assertFalse(exists(fc, new Path("/newDir/foo")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "newDir/foo")));
    
    // Create file with a 2 component dirs recursively
    fileContextTestHelper.createFile(fc, "/newDir/newDir2/foo");
    assertTrue(isFile(fc, new Path("/newDir/newDir2/foo")));
    assertTrue(isFile(fcTarget, new Path(chrootedTo, "newDir/newDir2/foo")));
    
    // Delete the created file
    assertTrue(fc.delete(new Path("/newDir/newDir2/foo"), false));
    assertFalse(exists(fc, new Path("/newDir/newDir2/foo")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "newDir/newDir2/foo")));
  }
  
  
  @Test
  public void testMkdirDelete() throws IOException {
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX"), FileContext.DEFAULT_PERM, false);
    assertTrue(isDir(fc, new Path("/dirX")));
    assertTrue(isDir(fcTarget, new Path(chrootedTo, "dirX")));
    
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX/dirY"), FileContext.DEFAULT_PERM, false);
    assertTrue(isDir(fc, new Path("/dirX/dirY")));
    assertTrue(isDir(fcTarget, new Path(chrootedTo, "dirX/dirY")));
    

    // Delete the created dir
    assertTrue(fc.delete(new Path("/dirX/dirY"), false));
    assertFalse(exists(fc, new Path("/dirX/dirY")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "dirX/dirY")));
    
    assertTrue(fc.delete(new Path("/dirX"), false));
    assertFalse(exists(fc, new Path("/dirX")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "dirX")));
    
  }
  @Test
  public void testRename() throws IOException {
    // Rename a file
    fileContextTestHelper.createFile(fc, "/newDir/foo");
    fc.rename(new Path("/newDir/foo"), new Path("/newDir/fooBar"));
    assertFalse(exists(fc, new Path("/newDir/foo")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "newDir/foo")));
    assertTrue(isFile(fc, fileContextTestHelper.getTestRootPath(fc, "/newDir/fooBar")));
    assertTrue(isFile(fcTarget, new Path(chrootedTo, "newDir/fooBar")));
    
    
    // Rename a dir
    fc.mkdir(new Path("/newDir/dirFoo"), FileContext.DEFAULT_PERM, false);
    fc.rename(new Path("/newDir/dirFoo"), new Path("/newDir/dirFooBar"));
    assertFalse(exists(fc, new Path("/newDir/dirFoo")));
    assertFalse(exists(fcTarget, new Path(chrootedTo, "newDir/dirFoo")));
    assertTrue(isDir(fc, fileContextTestHelper.getTestRootPath(fc, "/newDir/dirFooBar")));
    assertTrue(isDir(fcTarget, new Path(chrootedTo, "newDir/dirFooBar")));
  }
  
  
  /*
   * We would have liked renames across file system to fail but 
   * Unfortunately there is not way to distinguish the two file systems 
   * @throws IOException
   */
  @Test
  public void testRenameAcrossFs() throws IOException {
    fc.mkdir(new Path("/newDir/dirFoo"), FileContext.DEFAULT_PERM, true);
    // the root will get interpreted to the root of the chrooted fs.
    fc.rename(new Path("/newDir/dirFoo"), new Path("file:///dirFooBar"));
    FileContextTestHelper.isDir(fc, new Path("/dirFooBar"));
  }
  
  @Test
  public void testList() throws IOException {
    
    FileStatus fs = fc.getFileStatus(new Path("/"));
    assertTrue(fs.isDirectory());
    //  should return the full path not the chrooted path
    assertEquals(fs.getPath(), chrootedTo);
    
    // list on Slash
    
    FileStatus[] dirPaths = fc.util().listStatus(new Path("/"));

    assertEquals(0, dirPaths.length);
    
    

    fileContextTestHelper.createFileNonRecursive(fc, "/foo");
    fileContextTestHelper.createFileNonRecursive(fc, "/bar");
    fc.mkdir(new Path("/dirX"), FileContext.DEFAULT_PERM, false);
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirY"),
        FileContext.DEFAULT_PERM, false);
    fc.mkdir(new Path("/dirX/dirXX"), FileContext.DEFAULT_PERM, false);
    
    dirPaths = fc.util().listStatus(new Path("/"));
    assertEquals(4, dirPaths.length);
    
    // Note the the file status paths are the full paths on target
    fs = fileContextTestHelper.containsPath(fcTarget, "foo", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isFile());
    fs = fileContextTestHelper.containsPath(fcTarget, "bar", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isFile());
    fs = fileContextTestHelper.containsPath(fcTarget, "dirX", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory());
    fs = fileContextTestHelper.containsPath(fcTarget, "dirY", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory());
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    fc.mkdir(new Path("/testWd"), FileContext.DEFAULT_PERM, false);
    Path workDir = new Path("/testWd");
    Path fqWd = fc.makeQualified(workDir);
    fc.setWorkingDirectory(workDir);
    assertEquals(fqWd, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path("."));
    assertEquals(fqWd, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path(".."));
    assertEquals(fqWd.getParent(), fc.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path("/testWd");
    fqWd = fc.makeQualified(workDir);
    fc.setWorkingDirectory(workDir);
    assertEquals(fqWd, fc.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    Path fqAbsoluteDir = fc.makeQualified(absoluteDir);
    fc.setWorkingDirectory(relativeDir);
    assertEquals(fqAbsoluteDir, fc.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = new Path("/test/existingDir2");
    fqAbsoluteDir = fc.makeQualified(absoluteDir);
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    assertEquals(fqAbsoluteDir, fc.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    fc.create(absolutePath, EnumSet.of(CreateFlag.CREATE)).close();
    fc.open(new Path("foo")).close();
    
    // Now mkdir relative to the dir we cd'ed to
    fc.mkdir(new Path("newDir"), FileContext.DEFAULT_PERM, true);
    assertTrue(isDir(fc, new Path(absoluteDir, "newDir")));

    absoluteDir = fileContextTestHelper.getTestRootPath(fc, "nonexistingPath");
    try {
      fc.setWorkingDirectory(absoluteDir);
      fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    
    // Try a URI
    final String LOCAL_FS_ROOT_URI = "file:///tmp/test";
    absoluteDir = new Path(LOCAL_FS_ROOT_URI + "/existingDir");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    assertEquals(absoluteDir, fc.getWorkingDirectory());

  }
  
  /*
   * Test resolvePath(p) 
   */
  
  @Test
  public void testResolvePath() throws IOException {
    assertEquals(chrootedTo, fc.getDefaultFileSystem().resolvePath(new Path("/")));
    fileContextTestHelper.createFile(fc, "/foo");
    assertEquals(new Path(chrootedTo, "foo"),
        fc.getDefaultFileSystem().resolvePath(new Path("/foo"))); 
  }

  @Test
  public void testResolvePathNonExisting() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fc.getDefaultFileSystem().resolvePath(new Path("/nonExisting"));
    });
  }
 
  @Test
  public void testIsValidNameValidInBaseFs() throws Exception {
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
    Mockito.doReturn(true).when(baseFs).isValidName(Mockito.anyString());
    assertTrue(chRootedFs.isValidName("/test"));
    Mockito.verify(baseFs).isValidName("/chroot/test");
  }

  @Test
  public void testIsValidNameInvalidInBaseFs() throws Exception {
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
    Mockito.doReturn(false).when(baseFs).isValidName(Mockito.anyString());
    assertFalse(chRootedFs.isValidName("/test"));
    Mockito.verify(baseFs).isValidName("/chroot/test");
  }

  @Test
  @Timeout(value = 30)
  public void testCreateSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doReturn(snapRootPath).when(baseFs)
        .createSnapshot(chRootedSnapRootPath, "snap1");
    assertEquals(snapRootPath,
        chRootedFs.createSnapshot(snapRootPath, "snap1"));
    Mockito.verify(baseFs).createSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test
  @Timeout(value = 30)
  public void testDeleteSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doNothing().when(baseFs)
        .deleteSnapshot(chRootedSnapRootPath, "snap1");
    chRootedFs.deleteSnapshot(snapRootPath, "snap1");
    Mockito.verify(baseFs).deleteSnapshot(chRootedSnapRootPath, "snap1");
  }

  @Test
  @Timeout(value = 30)
  public void testRenameSnapshot() throws Exception {
    Path snapRootPath = new Path("/snapPath");
    Path chRootedSnapRootPath = new Path(
        Path.getPathWithoutSchemeAndAuthority(chrootedTo), "snapPath");
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, chrootedTo);
    Mockito.doNothing().when(baseFs)
        .renameSnapshot(chRootedSnapRootPath, "snapOldName", "snapNewName");
    chRootedFs.renameSnapshot(snapRootPath, "snapOldName", "snapNewName");
    Mockito.verify(baseFs).renameSnapshot(chRootedSnapRootPath, "snapOldName",
        "snapNewName");
  }
}
