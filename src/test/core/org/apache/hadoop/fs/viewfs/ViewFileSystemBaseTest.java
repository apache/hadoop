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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import static org.apache.hadoop.fs.FileSystemTestHelper.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * <p>
 * A collection of tests for the {@link ViewFileSystem}.
 * This test should be used for testing ViewFileSystem that has mount links to 
 * a target file system such  localFs or Hdfs etc.

 * </p>
 * <p>
 * To test a given target file system create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fsTarget</code> 
 * to point to the file system to which you want the mount targets
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */

public class ViewFileSystemBaseTest {
  FileSystem fsView;  // the view file system - the mounts are here
  FileSystem fsTarget;  // the target file system - the mount will point here
  Path targetTestRoot;

  @Before
  public void setUp() throws Exception {
    targetTestRoot = FileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    fsTarget.delete(targetTestRoot, true);
    
    fsTarget.mkdirs(targetTestRoot);
    // Make  user and data dirs - we creates links to them in the mount table
    fsTarget.mkdirs(new Path(targetTestRoot,"user"));
    fsTarget.mkdirs(new Path(targetTestRoot,"data"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir2"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir3"));
    FileSystemTestHelper.createFile(fsTarget, new Path(targetTestRoot,"aFile"));
    
    
    // Now we use the mount fs to set links to user and dir
    // in the test root
    
    // Set up the defaultMT in the config with our mount point links
    //Configuration conf = new Configuration();
    Configuration conf = ViewFileSystemTestSetup.configWithViewfsScheme();
    ConfigUtil.addLink(conf, "/user", new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/user2", new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/data", new Path(targetTestRoot,"data").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path(targetTestRoot,"dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3",
        new Path(targetTestRoot,"dir3").toUri());
    ConfigUtil.addLink(conf, "/danglingLink",
        new Path(targetTestRoot,"missingTarget").toUri());
    ConfigUtil.addLink(conf, "/linkToAFile",
        new Path(targetTestRoot,"aFile").toUri());
    
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(FileSystemTestHelper.getTestRootPath(fsTarget), true);
  }
  
  
  @Test
  public void testBasicPaths() {
    Assert.assertEquals(FsConstants.VIEWFS_URI,
        fsView.getUri());
    Assert.assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getWorkingDirectory());
    Assert.assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getHomeDirectory());
    Assert.assertEquals(
        new Path("/foo/bar").makeQualified(FsConstants.VIEWFS_URI, null),
        fsView.makeQualified(new Path("/foo/bar")));
  }
  
  /** 
   * Test modify operations (create, mkdir, delete, etc) 
   * on the mount file system where the pathname references through
   * the mount points.  Hence these operation will modify the target
   * file system.
   * 
   * Verify the operation via mountfs (ie fSys) and *also* via the
   *  target file system (ie fSysLocal) that the mount link points-to.
   */
  @Test
  public void testOperationsThroughMountLinks() throws IOException {
    // Create file 
    FileSystemTestHelper.createFile(fsView, "/user/foo");
    Assert.assertTrue(fsView.isFile(new Path("/user/foo")));
    Assert.assertTrue(fsTarget.isFile(new Path(targetTestRoot,"user/foo")));
    
    // Delete the created file
    Assert.assertTrue(fsView.delete(new Path("/user/foo"), false));
    Assert.assertFalse(fsView.exists(new Path("/user/foo")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/foo")));
    
    // Create file with a 2 component dirs
    FileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/foo");
    Assert.assertTrue(fsView.isFile(new Path("/internalDir/linkToDir2/foo")));
    Assert.assertTrue(fsTarget.isFile(new Path(targetTestRoot,"dir2/foo")));
    
    // Delete the created file
    Assert.assertTrue(fsView.delete(new Path("/internalDir/linkToDir2/foo"), false));
    Assert.assertFalse(fsView.exists(new Path("/internalDir/linkToDir2/foo")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"dir2/foo")));
    
    
    // Create file with a 3 component dirs
    FileSystemTestHelper.createFile(fsView, "/internalDir/internalDir2/linkToDir3/foo");
    Assert.assertTrue(fsView.isFile(new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertTrue(fsTarget.isFile(new Path(targetTestRoot,"dir3/foo")));
    
    // Recursive Create file with missing dirs
    FileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/missingDir/miss2/foo");
    Assert.assertTrue(fsView.isFile(new Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
    Assert.assertTrue(fsTarget.isFile(new Path(targetTestRoot,"dir2/missingDir/miss2/foo")));

    
    // Delete the created file
    Assert.assertTrue(fsView.delete(new Path("/internalDir/internalDir2/linkToDir3/foo"), false));
    Assert.assertFalse(fsView.exists(new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"dir3/foo")));
    
      
    // mkdir
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    Assert.assertTrue(fsView.isDirectory(new Path("/user/dirX")));
    Assert.assertTrue(fsTarget.isDirectory(new Path(targetTestRoot,"user/dirX")));
    
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
    Assert.assertTrue(fsView.isDirectory(new Path("/user/dirX/dirY")));
    Assert.assertTrue(fsTarget.isDirectory(new Path(targetTestRoot,"user/dirX/dirY")));
    

    // Delete the created dir
    Assert.assertTrue(fsView.delete(new Path("/user/dirX/dirY"), false));
    Assert.assertFalse(fsView.exists(new Path("/user/dirX/dirY")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/dirX/dirY")));
    
    Assert.assertTrue(fsView.delete(new Path("/user/dirX"), false));
    Assert.assertFalse(fsView.exists(new Path("/user/dirX")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/dirX")));
    
    // Rename a file 
    FileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/user/fooBar"));
    Assert.assertFalse(fsView.exists(new Path("/user/foo")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/foo")));
    Assert.assertTrue(fsView.isFile(FileSystemTestHelper.getTestRootPath(fsView,"/user/fooBar")));
    Assert.assertTrue(fsTarget.isFile(new Path(targetTestRoot,"user/fooBar")));
    
    fsView.mkdirs(new Path("/user/dirFoo"));
    fsView.rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
    Assert.assertFalse(fsView.exists(new Path("/user/dirFoo")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/dirFoo")));
    Assert.assertTrue(fsView.isDirectory(FileSystemTestHelper.getTestRootPath(fsView,"/user/dirFooBar")));
    Assert.assertTrue(fsTarget.isDirectory(new Path(targetTestRoot,"user/dirFooBar")));
    
  }
  
  // rename across mount points that point to same target also fail 
  @Test(expected=IOException.class) 
  public void testRenameAcrossMounts1() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
    /* - code if we had wanted this to suceed
    Assert.assertFalse(fSys.exists(new Path("/user/foo")));
    Assert.assertFalse(fSysLocal.exists(new Path(targetTestRoot,"user/foo")));
    Assert.assertTrue(fSys.isFile(FileSystemTestHelper.getTestRootPath(fSys,"/user2/fooBarBar")));
    Assert.assertTrue(fSysLocal.isFile(new Path(targetTestRoot,"user/fooBarBar")));
    */
  }
  
  
  // rename across mount points fail if the mount link targets are different
  // even if the targets are part of the same target FS

  @Test(expected=IOException.class) 
  public void testRenameAcrossMounts2() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/data/fooBar"));
  }
  
  static protected boolean SupportsBlocks = false; //  local fs use 1 block
                                                   // override for HDFS
  @Test
  public void testGetBlockLocations() throws IOException {
    Path targetFilePath = new Path(targetTestRoot,"data/largeFile");
    FileSystemTestHelper.createFile(fsTarget, 
        targetFilePath, 10, 1024);
    Path viewFilePath = new Path("/data/largeFile");
    Assert.assertTrue(fsView.isFile(viewFilePath));
    BlockLocation[] viewBL = fsView.getFileBlockLocations(fsView.getFileStatus(viewFilePath), 0, 10240+100);
    Assert.assertEquals(SupportsBlocks ? 10 : 1, viewBL.length);
    BlockLocation[] targetBL = fsTarget.getFileBlockLocations(fsTarget.getFileStatus(targetFilePath), 0, 10240+100);
    compareBLs(viewBL, targetBL);
    
    
    // Same test but now get it via the FileStatus Parameter
    fsView.getFileBlockLocations(
        fsView.getFileStatus(viewFilePath), 0, 10240+100);
    targetBL = fsTarget.getFileBlockLocations(
        fsTarget.getFileStatus(targetFilePath), 0, 10240+100);
    compareBLs(viewBL, targetBL);  
  }
  
  void compareBLs(BlockLocation[] viewBL, BlockLocation[] targetBL) {
    Assert.assertEquals(targetBL.length, viewBL.length);
    int i = 0;
    for (BlockLocation vbl : viewBL) {
      Assert.assertEquals(vbl.toString(), targetBL[i].toString());
      Assert.assertEquals(targetBL[i].getOffset(), vbl.getOffset());
      Assert.assertEquals(targetBL[i].getLength(), vbl.getLength());
      i++;     
    } 
  }
  
  
  
  /**
   * Test "readOps" (e.g. list, listStatus) 
   * on internal dirs of mount table
   * These operations should succeed.
   */
  
  // test list on internal dirs of mount table 
  @Test
  public void testListOnInternalDirsOfMountTable() throws IOException {
    
    // list on Slash
    
    FileStatus[] dirPaths = fsView.listStatus(new Path("/"));
    FileStatus fs;
    Assert.assertEquals(6, dirPaths.length);
    fs = FileSystemTestHelper.containsPath(fsView, "/user", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isSymlink());
    fs = FileSystemTestHelper.containsPath(fsView, "/data", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isSymlink());
    fs = FileSystemTestHelper.containsPath(fsView, "/internalDir", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isDirectory());
    fs = FileSystemTestHelper.containsPath(fsView, "/danglingLink", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isSymlink());
    fs = FileSystemTestHelper.containsPath(fsView, "/linkToAFile", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue(fs.isSymlink());
      
      
      
      // list on internal dir
      dirPaths = fsView.listStatus(new Path("/internalDir"));
      Assert.assertEquals(2, dirPaths.length);

      fs = FileSystemTestHelper.containsPath(fsView, "/internalDir/internalDir2", dirPaths);
        Assert.assertNotNull(fs);
        Assert.assertTrue(fs.isDirectory());
      fs = FileSystemTestHelper.containsPath(fsView, "/internalDir/linkToDir2", dirPaths);
        Assert.assertNotNull(fs);
        Assert.assertTrue(fs.isSymlink());
  }
  
  @Test
  public void testListOnMountTargetDirs() throws IOException {
    FileStatus[] dirPaths = fsView.listStatus(new Path("/data"));
    FileStatus fs;
    Assert.assertEquals(0, dirPaths.length);
    
    // add a file
    FileSystemTestHelper.createFile(fsView, "/data/foo");
    dirPaths = fsView.listStatus(new Path("/data"));
    Assert.assertEquals(1, dirPaths.length);
    fs = FileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue(fs.isFile());
    
    // add a dir
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/data/dirX"));
    dirPaths = fsView.listStatus(new Path("/data"));
    Assert.assertEquals(2, dirPaths.length);
    fs = FileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue(fs.isFile());
    fs = FileSystemTestHelper.containsPath(fsView, "/data/dirX", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue(fs.isDirectory()); 
  }
      
  @Test
  public void testFileStatusOnMountLink() throws IOException {
    Assert.assertTrue(fsView.getFileStatus(new Path("/")).isDirectory());
    checkFileStatus(fsView, "/", fileType.isDir);
    checkFileStatus(fsView, "/user", fileType.isDir); // link followed => dir
    checkFileStatus(fsView, "/data", fileType.isDir);
    checkFileStatus(fsView, "/internalDir", fileType.isDir);
    checkFileStatus(fsView, "/internalDir/linkToDir2", fileType.isDir);
    checkFileStatus(fsView, "/internalDir/internalDir2/linkToDir3",
        fileType.isDir);
    checkFileStatus(fsView, "/linkToAFile", fileType.isFile);
  }
  
  @Test(expected=FileNotFoundException.class) 
  public void testgetFSonDanglingLink() throws IOException {
    fsView.getFileStatus(new Path("/danglingLink"));
  }
  
  
  @Test(expected=FileNotFoundException.class) 
  public void testgetFSonNonExistingInternalDir() throws IOException {
    fsView.getFileStatus(new Path("/internalDir/nonExisting"));
  }

  
  
  
  
  /**
   * Test modify operations (create, mkdir, rename, etc) 
   * on internal dirs of mount table
   * These operations should fail since the mount table is read-only or
   * because the internal dir that it is trying to create already
   * exits.
   */
 
 
  // Mkdir on internal mount table should fail
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirSlash() throws IOException {
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/"));
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirExisting1() throws IOException {
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/internalDir"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirExisting2() throws IOException {
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/internalDir/linkToDir2"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew() throws IOException {
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/dirNew"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew2() throws IOException {
    fsView.mkdirs(FileSystemTestHelper.getTestRootPath(fsView, "/internalDir/dirNew"));
  }
  
  // Create on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate1() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/foo"); // 1 component
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate2() throws IOException {  // 2 component
    FileSystemTestHelper.createFile(fsView, "/internalDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/missingDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir2() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/missingDir/miss2/foo");
  }
  
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir3() throws IOException {
    FileSystemTestHelper.createFile(fsView, "/internalDir/miss2/foo");
  }
  
  // Delete on internal mount table should fail
  
  @Test(expected=FileNotFoundException.class) 
  public void testInternalDeleteNonExisting() throws IOException {
      fsView.delete(new Path("/NonExisting"), false);
  }
  @Test(expected=FileNotFoundException.class) 
  public void testInternalDeleteNonExisting2() throws IOException {
      fsView.delete(new Path("/internalDir/NonExisting"), false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalDeleteExisting() throws IOException {
      fsView.delete(new Path("/internalDir"), false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalDeleteExisting2() throws IOException {
    Assert.assertTrue(
        fsView.getFileStatus(
            new Path("/internalDir/linkToDir2")).isDirectory());
    fsView.delete(new Path("/internalDir/linkToDir2"), false);
  } 
  
  
  // Rename on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalRename1() throws IOException {
    fsView.rename(new Path("/internalDir"), new Path("/newDir"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRename2() throws IOException {
    Assert.assertTrue(
        fsView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory());
    fsView.rename(new Path("/internalDir/linkToDir2"),
        new Path("/internalDir/dir1"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRename3() throws IOException {
    fsView.rename(new Path("/user"), new Path("/internalDir/linkToDir2"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRenameToSlash() throws IOException {
    fsView.rename(new Path("/internalDir/linkToDir2/foo"), new Path("/"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRenameFromSlash() throws IOException {
    fsView.rename(new Path("/"), new Path("/bar"));
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalSetOwner() throws IOException {
    fsView.setOwner(new Path("/internalDir"), "foo", "bar");
  }
}
