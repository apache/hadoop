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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ChRootedFs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestChRootedFs {
  FileContextTestHelper fileContextTestHelper = new FileContextTestHelper();
  FileContext fc; // The ChRoootedFs
  FileContext fcTarget; // 
  Path chrootedTo;

  @Before
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

  @After
  public void tearDown() throws Exception {
    fcTarget.delete(chrootedTo, true);
  }

  
  @Test
  public void testBasicPaths() {
    URI uri = fc.getDefaultFileSystem().getUri();
    Assert.assertEquals(chrootedTo.toUri(), uri);
    Assert.assertEquals(fc.makeQualified(
        new Path(System.getProperty("user.home"))),
        fc.getWorkingDirectory());
    Assert.assertEquals(fc.makeQualified(
        new Path(System.getProperty("user.home"))),
        fc.getHomeDirectory());
    /*
     * ChRootedFs as its uri like file:///chrootRoot.
     * This is questionable since path.makequalified(uri, path) ignores
     * the pathPart of a uri. So our notion of chrooted URI is questionable.
     * But if we were to fix Path#makeQualified() then  the next test should
     *  have been:

    Assert.assertEquals(
        new Path(chrootedTo + "/foo/bar").makeQualified(
            FsConstants.LOCAL_FS_URI, null),
        fc.makeQualified(new Path( "/foo/bar")));
    */
    
    Assert.assertEquals(
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
    Assert.assertTrue(isFile(fc, new Path("/foo")));
    Assert.assertTrue(isFile(fcTarget, new Path(chrootedTo, "foo")));
    
    // Create file with recursive dir
    fileContextTestHelper.createFile(fc, "/newDir/foo");
    Assert.assertTrue(isFile(fc, new Path("/newDir/foo")));
    Assert.assertTrue(isFile(fcTarget, new Path(chrootedTo,"newDir/foo")));
    
    // Delete the created file
    Assert.assertTrue(fc.delete(new Path("/newDir/foo"), false));
    Assert.assertFalse(exists(fc, new Path("/newDir/foo")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"newDir/foo")));
    
    // Create file with a 2 component dirs recursively
    fileContextTestHelper.createFile(fc, "/newDir/newDir2/foo");
    Assert.assertTrue(isFile(fc, new Path("/newDir/newDir2/foo")));
    Assert.assertTrue(isFile(fcTarget, new Path(chrootedTo,"newDir/newDir2/foo")));
    
    // Delete the created file
    Assert.assertTrue(fc.delete(new Path("/newDir/newDir2/foo"), false));
    Assert.assertFalse(exists(fc, new Path("/newDir/newDir2/foo")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"newDir/newDir2/foo")));
  }
  
  
  @Test
  public void testMkdirDelete() throws IOException {
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX"), FileContext.DEFAULT_PERM, false);
    Assert.assertTrue(isDir(fc, new Path("/dirX")));
    Assert.assertTrue(isDir(fcTarget, new Path(chrootedTo,"dirX")));
    
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirX/dirY"), FileContext.DEFAULT_PERM, false);
    Assert.assertTrue(isDir(fc, new Path("/dirX/dirY")));
    Assert.assertTrue(isDir(fcTarget, new Path(chrootedTo,"dirX/dirY")));
    

    // Delete the created dir
    Assert.assertTrue(fc.delete(new Path("/dirX/dirY"), false));
    Assert.assertFalse(exists(fc, new Path("/dirX/dirY")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"dirX/dirY")));
    
    Assert.assertTrue(fc.delete(new Path("/dirX"), false));
    Assert.assertFalse(exists(fc, new Path("/dirX")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"dirX")));
    
  }
  @Test
  public void testRename() throws IOException {
    // Rename a file
    fileContextTestHelper.createFile(fc, "/newDir/foo");
    fc.rename(new Path("/newDir/foo"), new Path("/newDir/fooBar"));
    Assert.assertFalse(exists(fc, new Path("/newDir/foo")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"newDir/foo")));
    Assert.assertTrue(isFile(fc, fileContextTestHelper.getTestRootPath(fc,"/newDir/fooBar")));
    Assert.assertTrue(isFile(fcTarget, new Path(chrootedTo,"newDir/fooBar")));
    
    
    // Rename a dir
    fc.mkdir(new Path("/newDir/dirFoo"), FileContext.DEFAULT_PERM, false);
    fc.rename(new Path("/newDir/dirFoo"), new Path("/newDir/dirFooBar"));
    Assert.assertFalse(exists(fc, new Path("/newDir/dirFoo")));
    Assert.assertFalse(exists(fcTarget, new Path(chrootedTo,"newDir/dirFoo")));
    Assert.assertTrue(isDir(fc, fileContextTestHelper.getTestRootPath(fc,"/newDir/dirFooBar")));
    Assert.assertTrue(isDir(fcTarget, new Path(chrootedTo,"newDir/dirFooBar")));
  }
  
  
  /**
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
    Assert.assertTrue(fs.isDirectory());
    //  should return the full path not the chrooted path
    Assert.assertEquals(fs.getPath(), chrootedTo);
    
    // list on Slash
    
    FileStatus[] dirPaths = fc.util().listStatus(new Path("/"));

    Assert.assertEquals(0, dirPaths.length);
    
    

    fileContextTestHelper.createFileNonRecursive(fc, "/foo");
    fileContextTestHelper.createFileNonRecursive(fc, "/bar");
    fc.mkdir(new Path("/dirX"), FileContext.DEFAULT_PERM, false);
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc, "/dirY"),
        FileContext.DEFAULT_PERM, false);
    fc.mkdir(new Path("/dirX/dirXX"), FileContext.DEFAULT_PERM, false);
    
    dirPaths = fc.util().listStatus(new Path("/"));
    Assert.assertEquals(4, dirPaths.length);
    
    // Note the the file status paths are the full paths on target
    fs = fileContextTestHelper.containsPath(fcTarget, "foo", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isFile());
    fs = fileContextTestHelper.containsPath(fcTarget, "bar", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isFile());
    fs = fileContextTestHelper.containsPath(fcTarget, "dirX", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isDirectory());
    fs = fileContextTestHelper.containsPath(fcTarget, "dirY", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isDirectory());
  }
  
  @Test
  public void testWorkingDirectory() throws Exception {

    // First we cd to our test root
    fc.mkdir(new Path("/testWd"), FileContext.DEFAULT_PERM, false);
    Path workDir = new Path("/testWd");
    Path fqWd = fc.makeQualified(workDir);
    fc.setWorkingDirectory(workDir);
    Assert.assertEquals(fqWd, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path("."));
    Assert.assertEquals(fqWd, fc.getWorkingDirectory());

    fc.setWorkingDirectory(new Path(".."));
    Assert.assertEquals(fqWd.getParent(), fc.getWorkingDirectory());
    
    // cd using a relative path

    // Go back to our test root
    workDir = new Path("/testWd");
    fqWd = fc.makeQualified(workDir);
    fc.setWorkingDirectory(workDir);
    Assert.assertEquals(fqWd, fc.getWorkingDirectory());
    
    Path relativeDir = new Path("existingDir1");
    Path absoluteDir = new Path(workDir,"existingDir1");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    Path fqAbsoluteDir = fc.makeQualified(absoluteDir);
    fc.setWorkingDirectory(relativeDir);
    Assert.assertEquals(fqAbsoluteDir, fc.getWorkingDirectory());
    // cd using a absolute path
    absoluteDir = new Path("/test/existingDir2");
    fqAbsoluteDir = fc.makeQualified(absoluteDir);
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(fqAbsoluteDir, fc.getWorkingDirectory());
    
    // Now open a file relative to the wd we just set above.
    Path absolutePath = new Path(absoluteDir, "foo");
    fc.create(absolutePath, EnumSet.of(CreateFlag.CREATE)).close();
    fc.open(new Path("foo")).close();
    
    // Now mkdir relative to the dir we cd'ed to
    fc.mkdir(new Path("newDir"), FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(isDir(fc, new Path(absoluteDir, "newDir")));

    absoluteDir = fileContextTestHelper.getTestRootPath(fc, "nonexistingPath");
    try {
      fc.setWorkingDirectory(absoluteDir);
      Assert.fail("cd to non existing dir should have failed");
    } catch (Exception e) {
      // Exception as expected
    }
    
    // Try a URI
    final String LOCAL_FS_ROOT_URI = "file:///tmp/test";
    absoluteDir = new Path(LOCAL_FS_ROOT_URI + "/existingDir");
    fc.mkdir(absoluteDir, FileContext.DEFAULT_PERM, true);
    fc.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fc.getWorkingDirectory());

  }
  
  /*
   * Test resolvePath(p) 
   */
  
  @Test
  public void testResolvePath() throws IOException {
    Assert.assertEquals(chrootedTo, fc.getDefaultFileSystem().resolvePath(new Path("/"))); 
    fileContextTestHelper.createFile(fc, "/foo");
    Assert.assertEquals(new Path(chrootedTo, "foo"),
        fc.getDefaultFileSystem().resolvePath(new Path("/foo"))); 
  }

  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathNonExisting() throws IOException {
      fc.getDefaultFileSystem().resolvePath(new Path("/nonExisting"));
  }
 
  @Test
  public void testIsValidNameValidInBaseFs() throws Exception {
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
    Mockito.doReturn(true).when(baseFs).isValidName(Mockito.anyString());
    Assert.assertTrue(chRootedFs.isValidName("/test"));
    Mockito.verify(baseFs).isValidName("/chroot/test");
  }

  @Test
  public void testIsValidNameInvalidInBaseFs() throws Exception {
    AbstractFileSystem baseFs = Mockito.spy(fc.getDefaultFileSystem());
    ChRootedFs chRootedFs = new ChRootedFs(baseFs, new Path("/chroot"));
    Mockito.doReturn(false).when(baseFs).isValidName(Mockito.anyString());
    Assert.assertFalse(chRootedFs.isValidName("/test"));
    Mockito.verify(baseFs).isValidName("/chroot/test");
  }
}
