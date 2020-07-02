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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.TestFileUtil;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem.MountPoint;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.fs.FileSystemTestHelper.*;
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

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

abstract public class ViewFileSystemBaseTest {
  FileSystem fsView;  // the view file system - the mounts are here
  FileSystem fsTarget;  // the target file system - the mount will point here
  Path targetTestRoot;
  Configuration conf;
  final FileSystemTestHelper fileSystemTestHelper;
  private static final Logger LOG =
      LoggerFactory.getLogger(ViewFileSystemBaseTest.class);

  public ViewFileSystemBaseTest() {
      this.fileSystemTestHelper = createFileSystemHelper();
  }

  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    initializeTargetTestRoot();
    
    // Make  user and data dirs - we creates links to them in the mount table
    fsTarget.mkdirs(new Path(targetTestRoot,"user"));
    fsTarget.mkdirs(new Path(targetTestRoot,"data"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir2"));
    fsTarget.mkdirs(new Path(targetTestRoot,"dir3"));
    FileSystemTestHelper.createFile(fsTarget, new Path(targetTestRoot,"aFile"));
    
    
    // Now we use the mount fs to set links to user and dir
    // in the test root
    
    // Set up the defaultMT in the config with our mount point links
    conf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, conf);
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
  }
  
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    // In case previous test was killed before cleanup
    fsTarget.delete(targetTestRoot, true);
    
    fsTarget.mkdirs(targetTestRoot);
  }
  
  void setupMountPoints() {
    ConfigUtil.addLink(conf, "/targetRoot", targetTestRoot.toUri());
    ConfigUtil.addLink(conf, "/user", new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/user2", new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/data", new Path(targetTestRoot,"data").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path(targetTestRoot,"dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3",
        new Path(targetTestRoot,"dir3").toUri());
    ConfigUtil.addLink(conf, "/danglingLink",
        new Path(targetTestRoot, "missingTarget").toUri());
    ConfigUtil.addLink(conf, "/linkToAFile",
        new Path(targetTestRoot, "aFile").toUri());
  }
  
  @Test
  public void testGetMountPoints() {
    ViewFileSystem viewfs = (ViewFileSystem) fsView;
    MountPoint[] mountPoints = viewfs.getMountPoints();
    for (MountPoint mountPoint : mountPoints) {
      LOG.info("MountPoint: " + mountPoint.getMountedOnPath() + " => "
          + mountPoint.getTargetFileSystemURIs()[0]);
    }
    Assert.assertEquals(getExpectedMountPoints(), mountPoints.length); 
  }
  
  int getExpectedMountPoints() {
    return 8;
  }
  
  /**
   * This default implementation is when viewfs has mount points
   * into file systems, such as LocalFs that do no have delegation tokens.
   * It should be overridden for when mount points into hdfs.
   */
  @Test
  public void testGetDelegationTokens() throws IOException {
    Token<?>[] delTokens = 
        fsView.addDelegationTokens("sanjay", new Credentials());
    Assert.assertEquals(getExpectedDelegationTokenCount(), delTokens.length); 
  }
  
  int getExpectedDelegationTokenCount() {
    return 0;
  }

  @Test
  public void testGetDelegationTokensWithCredentials() throws IOException {
    Credentials credentials = new Credentials();
    List<Token<?>> delTokens =
        Arrays.asList(fsView.addDelegationTokens("sanjay", credentials));

    int expectedTokenCount = getExpectedDelegationTokenCountWithCredentials();

    Assert.assertEquals(expectedTokenCount, delTokens.size());
    Credentials newCredentials = new Credentials();
    for (int i = 0; i < expectedTokenCount / 2; i++) {
      Token<?> token = delTokens.get(i);
      newCredentials.addToken(token.getService(), token);
    }

    List<Token<?>> delTokens2 =
        Arrays.asList(fsView.addDelegationTokens("sanjay", newCredentials));
    Assert.assertEquals((expectedTokenCount + 1) / 2, delTokens2.size());
  }

  int getExpectedDelegationTokenCountWithCredentials() {
    return 0;
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

  @Test
  public void testLocatedOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(true);
  }

  @Test
  public void testOperationsThroughMountLinks() throws IOException {
    testOperationsThroughMountLinksInternal(false);
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
  private void testOperationsThroughMountLinksInternal(boolean located)
      throws IOException {
    // Create file
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.isFile(new Path("/user/foo")));
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.isFile(new Path(targetTestRoot,"user/foo")));
    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/user/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/user/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"user/foo")));
    
    // Create file with a 2 component dirs
    fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.isFile(new Path("/internalDir/linkToDir2/foo")));
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.isFile(new Path(targetTestRoot,"dir2/foo")));
    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/internalDir/linkToDir2/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/internalDir/linkToDir2/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"dir2/foo")));
    
    
    // Create file with a 3 component dirs
    fileSystemTestHelper.createFile(fsView, "/internalDir/internalDir2/linkToDir3/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.isFile(new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.isFile(new Path(targetTestRoot,"dir3/foo")));
    
    // Recursive Create file with missing dirs
    fileSystemTestHelper.createFile(fsView,
        "/internalDir/linkToDir2/missingDir/miss2/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.isFile(new Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.isFile(new Path(targetTestRoot,"dir2/missingDir/miss2/foo")));

    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",
        fsView.delete(
            new Path("/internalDir/internalDir2/linkToDir3/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"dir3/foo")));
    
      
    // mkdir
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    Assert.assertTrue("New dir should be type dir", 
        fsView.isDirectory(new Path("/user/dirX")));
    Assert.assertTrue("Target of new dir should be of type dir",
        fsTarget.isDirectory(new Path(targetTestRoot,"user/dirX")));
    
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
    Assert.assertTrue("New dir should be type dir", 
        fsView.isDirectory(new Path("/user/dirX/dirY")));
    Assert.assertTrue("Target of new dir should be of type dir",
        fsTarget.isDirectory(new Path(targetTestRoot,"user/dirX/dirY")));
    

    // Delete the created dir
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/user/dirX/dirY"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/user/dirX/dirY")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"user/dirX/dirY")));
    
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/user/dirX"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/user/dirX")));
    Assert.assertFalse(fsTarget.exists(new Path(targetTestRoot,"user/dirX")));
    
    // Rename a file 
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    fsView.rename(new Path("/user/foo"), new Path("/user/fooBar"));
    Assert.assertFalse("Renamed src should not exist", 
        fsView.exists(new Path("/user/foo")));
    Assert.assertFalse("Renamed src should not exist in target",
        fsTarget.exists(new Path(targetTestRoot,"user/foo")));
    Assert.assertTrue("Renamed dest should  exist as file",
        fsView.isFile(fileSystemTestHelper.getTestRootPath(fsView,"/user/fooBar")));
    Assert.assertTrue("Renamed dest should  exist as file in target",
        fsTarget.isFile(new Path(targetTestRoot,"user/fooBar")));
    
    fsView.mkdirs(new Path("/user/dirFoo"));
    fsView.rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
    Assert.assertFalse("Renamed src should not exist", 
        fsView.exists(new Path("/user/dirFoo")));
    Assert.assertFalse("Renamed src should not exist in target",
        fsTarget.exists(new Path(targetTestRoot,"user/dirFoo")));
    Assert.assertTrue("Renamed dest should  exist as dir",
        fsView.isDirectory(fileSystemTestHelper.getTestRootPath(fsView,"/user/dirFooBar")));
    Assert.assertTrue("Renamed dest should  exist as dir in target",
        fsTarget.isDirectory(new Path(targetTestRoot,"user/dirFooBar")));
    
    // Make a directory under a directory that's mounted from the root of another FS
    fsView.mkdirs(new Path("/targetRoot/dirFoo"));
    Assert.assertTrue(fsView.exists(new Path("/targetRoot/dirFoo")));
    boolean dirFooPresent = false;
    for (FileStatus fileStatus :
        listStatusInternal(located, new Path("/targetRoot/"))) {
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    Assert.assertTrue(dirFooPresent);
  }
  
  // rename across mount points that point to same target also fail 
  @Test
  public void testRenameAcrossMounts1() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      fsView.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }
  
  
  // rename across mount points fail if the mount link targets are different
  // even if the targets are part of the same target FS

  @Test
  public void testRenameAcrossMounts2() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      fsView.rename(new Path("/user/foo"), new Path("/data/fooBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }

  // RenameStrategy SAME_TARGET_URI_ACROSS_MOUNTPOINT enabled
  // to rename across mount points that point to same target URI
  @Test
  public void testRenameAcrossMounts3() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/user/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/user2/fooBarBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBarBar"));
  }

  // RenameStrategy SAME_FILESYSTEM_ACROSS_MOUNTPOINT enabled
  // to rename across mount points where the mount link targets are different
  // but are part of the same target FS
  @Test
  public void testRenameAcrossMounts4() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/data/fooBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/user/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/data/fooBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "data/fooBar"));
  }

  static protected boolean SupportsBlocks = false; //  local fs use 1 block
                                                   // override for HDFS
  @Test
  public void testGetBlockLocations() throws IOException {
    Path targetFilePath = new Path(targetTestRoot,"data/largeFile");
    FileSystemTestHelper.createFile(fsTarget, 
        targetFilePath, 10, 1024);
    Path viewFilePath = new Path("/data/largeFile");
    Assert.assertTrue("Created File should be type File",
        fsView.isFile(viewFilePath));
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
      assertThat(vbl.toString(), equalTo(targetBL[i].toString()));
      assertThat(vbl.getOffset(), equalTo(targetBL[i].getOffset()));
      assertThat(vbl.getLength(), equalTo(targetBL[i].getLength()));
      i++;
    } 
  }

  @Test
  public void testLocatedListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(true);
  }


  /**
   * Test "readOps" (e.g. list, listStatus) 
   * on internal dirs of mount table
   * These operations should succeed.
   */
  
  // test list on internal dirs of mount table 
  @Test
  public void testListOnInternalDirsOfMountTable() throws IOException {
    testListOnInternalDirsOfMountTableInternal(false);
  }

  private void testListOnInternalDirsOfMountTableInternal(boolean located)
      throws IOException {
    
    // list on Slash

    FileStatus[] dirPaths = listStatusInternal(located, new Path("/"));
    FileStatus fs;
    verifyRootChildren(dirPaths);

    // list on internal dir
    dirPaths = listStatusInternal(located, new Path("/internalDir"));
    Assert.assertEquals(2, dirPaths.length);

    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/internalDir2", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isDirectory());
    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir/linkToDir2",
        dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
  }

  private void verifyRootChildren(FileStatus[] dirPaths) throws IOException {
    FileStatus fs;
    Assert.assertEquals(getExpectedDirPaths(), dirPaths.length);
    fs = fileSystemTestHelper.containsPath(fsView, "/user", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileSystemTestHelper.containsPath(fsView, "/data", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileSystemTestHelper.containsPath(fsView, "/internalDir", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("A mount should appear as symlink", fs.isDirectory());
    fs = fileSystemTestHelper.containsPath(fsView, "/danglingLink", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileSystemTestHelper.containsPath(fsView, "/linkToAFile", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
  }

  int getExpectedDirPaths() {
    return 7;
  }
  
  @Test
  public void testListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(false);
  }

  @Test
  public void testLocatedListOnMountTargetDirs() throws IOException {
    testListOnMountTargetDirsInternal(true);
  }

  private void testListOnMountTargetDirsInternal(boolean located)
      throws IOException {
    final Path dataPath = new Path("/data");

    FileStatus[] dirPaths = listStatusInternal(located, dataPath);

    FileStatus fs;
    Assert.assertEquals(0, dirPaths.length);
    
    // add a file
    long len = fileSystemTestHelper.createFile(fsView, "/data/foo");
    dirPaths = listStatusInternal(located, dataPath);
    Assert.assertEquals(1, dirPaths.length);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("Created file shoudl appear as a file", fs.isFile());
    Assert.assertEquals(len, fs.getLen());
    
    // add a dir
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/data/dirX"));
    dirPaths = listStatusInternal(located, dataPath);
    Assert.assertEquals(2, dirPaths.length);
    fs = fileSystemTestHelper.containsPath(fsView, "/data/foo", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("Created file shoudl appear as a file", fs.isFile());
    fs = fileSystemTestHelper.containsPath(fsView, "/data/dirX", dirPaths);
    Assert.assertNotNull(fs);
    Assert.assertTrue("Created dir should appear as a dir", fs.isDirectory()); 
  }

  private FileStatus[] listStatusInternal(boolean located, Path dataPath) throws IOException {
    FileStatus[] dirPaths = new FileStatus[0];
    if (located) {
      RemoteIterator<LocatedFileStatus> statIter =
          fsView.listLocatedStatus(dataPath);
      ArrayList<LocatedFileStatus> tmp = new ArrayList<LocatedFileStatus>(10);
      while (statIter.hasNext()) {
        tmp.add(statIter.next());
      }
      dirPaths = tmp.toArray(dirPaths);
    } else {
      dirPaths = fsView.listStatus(dataPath);
    }
    return dirPaths;
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
  
  /*
   * Test resolvePath(p) 
   */
  
  @Test
  public void testResolvePathInternalPaths() throws IOException {
    Assert.assertEquals(new Path("/"), fsView.resolvePath(new Path("/")));
    Assert.assertEquals(new Path("/internalDir"),
        fsView.resolvePath(new Path("/internalDir")));
  }
  @Test
  public void testResolvePathMountPoints() throws IOException {
    Assert.assertEquals(new Path(targetTestRoot,"user"),
        fsView.resolvePath(new Path("/user")));
    Assert.assertEquals(new Path(targetTestRoot,"data"),
        fsView.resolvePath(new Path("/data")));
    Assert.assertEquals(new Path(targetTestRoot,"dir2"),
        fsView.resolvePath(new Path("/internalDir/linkToDir2")));
    Assert.assertEquals(new Path(targetTestRoot,"dir3"),
        fsView.resolvePath(new Path("/internalDir/internalDir2/linkToDir3")));

  }
  
  @Test
  public void testResolvePathThroughMountPoints() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    Assert.assertEquals(new Path(targetTestRoot,"user/foo"),
        fsView.resolvePath(new Path("/user/foo")));
    
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    Assert.assertEquals(new Path(targetTestRoot,"user/dirX"),
        fsView.resolvePath(new Path("/user/dirX")));

    
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX/dirY"));
    Assert.assertEquals(new Path(targetTestRoot,"user/dirX/dirY"),
        fsView.resolvePath(new Path("/user/dirX/dirY")));
  }

  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathDanglingLink() throws IOException {
    fsView.resolvePath(new Path("/danglingLink"));
  }
  
  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathMissingThroughMountPoints() throws IOException {
    fsView.resolvePath(new Path("/user/nonExisting"));
  }
  

  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathMissingThroughMountPoints2() throws IOException {
    fsView.mkdirs(
        fileSystemTestHelper.getTestRootPath(fsView, "/user/dirX"));
    fsView.resolvePath(new Path("/user/dirX/nonExisting"));
  }
  
  /**
   * Test modify operations (create, mkdir, rename, etc) 
   * on internal dirs of mount table
   * These operations should fail since the mount table is read-only or
   * because the internal dir that it is trying to create already
   * exits.
   */
 
 
  // Mkdir on existing internal mount table succeed except for /
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirSlash() throws IOException {
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/"));
  }
  
  public void testInternalMkdirExisting1() throws IOException {
    Assert.assertTrue("mkdir of existing dir should succeed", 
        fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView,
        "/internalDir")));
  }

  public void testInternalMkdirExisting2() throws IOException {
    Assert.assertTrue("mkdir of existing dir should succeed", 
        fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView,
        "/internalDir/linkToDir2")));
  }
  
  // Mkdir for new internal mount table should fail
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew() throws IOException {
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/dirNew"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew2() throws IOException {
    fsView.mkdirs(fileSystemTestHelper.getTestRootPath(fsView, "/internalDir/dirNew"));
  }
  
  // Create File on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate1() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/foo"); // 1 component
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate2() throws IOException {  // 2 component
    fileSystemTestHelper.createFile(fsView, "/internalDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/missingDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir2() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/missingDir/miss2/foo");
  }
  
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir3() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/internalDir/miss2/foo");
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
    fsView.getFileStatus(
            new Path("/internalDir/linkToDir2")).isDirectory();
    fsView.delete(new Path("/internalDir/linkToDir2"), false);
  } 
  
  @Test
  public void testMkdirOfMountLink() throws IOException {
    // data exists - mkdirs returns true even though no permission in internal
    // mount table
    Assert.assertTrue("mkdir of existing mount link should succeed", 
        fsView.mkdirs(new Path("/data")));
  }
  
  
  // Rename on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalRename1() throws IOException {
    fsView.rename(new Path("/internalDir"), new Path("/newDir"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRename2() throws IOException {
    fsView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory();
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
  
  @Test
  public void testCreateNonRecursive() throws IOException {
    Path path = fileSystemTestHelper.getTestRootPath(fsView, "/user/foo");
    fsView.createNonRecursive(path, false, 1024, (short)1, 1024L, null);
    FileStatus status = fsView.getFileStatus(new Path("/user/foo"));
    Assert.assertTrue("Created file should be type file",
        fsView.isFile(new Path("/user/foo")));
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.isFile(new Path(targetTestRoot, "user/foo")));
  }

  @Test
  public void testRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(false);
  }

  @Test
  public void testLocatedRootReadableExecutable() throws IOException {
    testRootReadableExecutableInternal(true);
  }

  private void testRootReadableExecutableInternal(boolean located)
      throws IOException {
    // verify executable permission on root: cd /
    //
    Assert.assertFalse("In root before cd",
        fsView.getWorkingDirectory().isRoot());
    fsView.setWorkingDirectory(new Path("/"));
    Assert.assertTrue("Not in root dir after cd",
      fsView.getWorkingDirectory().isRoot());

    // verify readable
    //
    verifyRootChildren(listStatusInternal(located,
        fsView.getWorkingDirectory()));

    // verify permissions
    //
    final FileStatus rootStatus =
        fsView.getFileStatus(fsView.getWorkingDirectory());
    final FsPermission perms = rootStatus.getPermission();

    Assert.assertTrue("User-executable permission not set!",
        perms.getUserAction().implies(FsAction.EXECUTE));
    Assert.assertTrue("User-readable permission not set!",
        perms.getUserAction().implies(FsAction.READ));
    Assert.assertTrue("Group-executable permission not set!",
        perms.getGroupAction().implies(FsAction.EXECUTE));
    Assert.assertTrue("Group-readable permission not set!",
        perms.getGroupAction().implies(FsAction.READ));
    Assert.assertTrue("Other-executable permission not set!",
        perms.getOtherAction().implies(FsAction.EXECUTE));
    Assert.assertTrue("Other-readable permission not set!",
        perms.getOtherAction().implies(FsAction.READ));
  }

  /**
   * Verify the behavior of ACL operations on paths above the root of
   * any mount table entry.
   */

  @Test(expected=AccessControlException.class)
  public void testInternalModifyAclEntries() throws IOException {
    fsView.modifyAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveAclEntries() throws IOException {
    fsView.removeAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveDefaultAcl() throws IOException {
    fsView.removeDefaultAcl(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveAcl() throws IOException {
    fsView.removeAcl(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalSetAcl() throws IOException {
    fsView.setAcl(new Path("/internalDir"), new ArrayList<AclEntry>());
  }

  @Test
  public void testInternalGetAclStatus() throws IOException {
    final UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
    AclStatus aclStatus = fsView.getAclStatus(new Path("/internalDir"));
    assertEquals(aclStatus.getOwner(), currentUser.getUserName());
    assertEquals(aclStatus.getGroup(), currentUser.getGroupNames()[0]);
    assertEquals(aclStatus.getEntries(),
        AclUtil.getMinimalAcl(PERMISSION_555));
    assertFalse(aclStatus.isStickyBit());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalSetXAttr() throws IOException {
    fsView.setXAttr(new Path("/internalDir"), "xattrName", null);
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttr() throws IOException {
    fsView.getXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttrs() throws IOException {
    fsView.getXAttrs(new Path("/internalDir"));
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttrsWithNames() throws IOException {
    fsView.getXAttrs(new Path("/internalDir"), new ArrayList<String>());
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalListXAttr() throws IOException {
    fsView.listXAttrs(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveXAttr() throws IOException {
    fsView.removeXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot1() throws IOException {
    fsView.createSnapshot(new Path("/internalDir"));
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot2() throws IOException {
    fsView.createSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalRenameSnapshot() throws IOException {
    fsView.renameSnapshot(new Path("/internalDir"), "snapOldName",
        "snapNewName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalDeleteSnapshot() throws IOException {
    fsView.deleteSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalSetStoragePolicy() throws IOException {
    fsView.setStoragePolicy(new Path("/internalDir"), "HOT");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalUnsetStoragePolicy() throws IOException {
    fsView.unsetStoragePolicy(new Path("/internalDir"));
  }

  @Test(expected = AccessControlException.class)
  public void testInternalSatisfyStoragePolicy() throws IOException {
    fsView.satisfyStoragePolicy(new Path("/internalDir"));
  }

  @Test(expected = NotInMountpointException.class)
  public void testInternalgetStoragePolicy() throws IOException {
    fsView.getStoragePolicy(new Path("/internalDir"));
  }

  @Test
  public void testInternalGetAllStoragePolicies() throws IOException {
    Collection<? extends BlockStoragePolicySpi> policies =
        fsView.getAllStoragePolicies();
    for (FileSystem fs : fsView.getChildFileSystems()) {
      try {
        for (BlockStoragePolicySpi s : fs.getAllStoragePolicies()) {
          assertTrue("Missing policy: " + s, policies.contains(s));
        }
      } catch (UnsupportedOperationException e) {
        // ignore
      }
    }
  }

  @Test
  public void testConfLinkSlash() throws Exception {
    String clusterName = "ClusterX";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);

    Configuration newConf = new Configuration();
    ConfigUtil.addLink(newConf, clusterName, "/",
        new Path(targetTestRoot, "/").toUri());

    String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + clusterName + ".";
    try {
      FileSystem.get(viewFsUri, newConf);
      fail("ViewFileSystem should error out on mount table entry: "
          + mtPrefix + Constants.CONFIG_VIEWFS_LINK + "." + "/");
    } catch (Exception e) {
      if (e instanceof UnsupportedFileSystemException) {
        String msg = " Use " + Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH +
            " instead";
        assertThat(e.getMessage(), containsString(msg));
      } else {
        fail("Unexpected exception: " + e.getMessage());
      }
    }
  }

  @Test
  public void testTrashRoot() throws IOException {

    Path mountDataRootPath = new Path("/data");
    Path fsTargetFilePath = new Path("debug.log");
    Path mountDataFilePath = new Path(mountDataRootPath, fsTargetFilePath);
    Path mountDataNonExistingFilePath = new Path(mountDataRootPath, "no.log");
    fileSystemTestHelper.createFile(fsTarget, fsTargetFilePath);

    // Get Trash roots for paths via ViewFileSystem handle
    Path mountDataRootTrashPath = fsView.getTrashRoot(mountDataRootPath);
    Path mountDataFileTrashPath = fsView.getTrashRoot(mountDataFilePath);

    // Get Trash roots for the same set of paths via the mounted filesystem
    Path fsTargetRootTrashRoot = fsTarget.getTrashRoot(mountDataRootPath);
    Path fsTargetFileTrashPath = fsTarget.getTrashRoot(mountDataFilePath);

    // Verify if Trash roots from ViewFileSystem matches that of the ones
    // from the target mounted FileSystem.
    assertEquals(mountDataRootTrashPath.toUri().getPath(),
        fsTargetRootTrashRoot.toUri().getPath());
    assertEquals(mountDataFileTrashPath.toUri().getPath(),
        fsTargetFileTrashPath.toUri().getPath());
    assertEquals(mountDataRootTrashPath.toUri().getPath(),
        mountDataFileTrashPath.toUri().getPath());


    // Verify trash root for an non-existing file but on a valid mountpoint.
    Path trashRoot = fsView.getTrashRoot(mountDataNonExistingFilePath);
    assertEquals(mountDataRootTrashPath.toUri().getPath(),
        trashRoot.toUri().getPath());

    // Verify trash root for invalid mounts.
    Path invalidMountRootPath = new Path("/invalid_mount");
    Path invalidMountFilePath = new Path(invalidMountRootPath, "debug.log");
    try {
      fsView.getTrashRoot(invalidMountRootPath);
      fail("ViewFileSystem getTashRoot should fail for non-mountpoint paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }
    try {
      fsView.getTrashRoot(invalidMountFilePath);
      fail("ViewFileSystem getTashRoot should fail for non-mountpoint paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }
    try {
      fsView.getTrashRoot(null);
      fail("ViewFileSystem getTashRoot should fail for empty paths.");
    } catch (NotInMountpointException e) {
      //expected exception
    }

    // Move the file to trash
    FileStatus fileStatus = fsTarget.getFileStatus(fsTargetFilePath);
    Configuration newConf = new Configuration(conf);
    newConf.setLong("fs.trash.interval", 1000);
    Trash lTrash = new Trash(fsTarget, newConf);
    boolean trashed = lTrash.moveToTrash(fsTargetFilePath);
    Assert.assertTrue("File " + fileStatus + " move to " +
        "trash failed.", trashed);

    // Verify ViewFileSystem trash roots shows the ones from
    // target mounted FileSystem.
    Assert.assertTrue("", fsView.getTrashRoots(true).size() > 0);
  }

  @Test(expected = NotInMountpointException.class)
  public void testViewFileSystemUtil() throws Exception {
    Configuration newConf = new Configuration(conf);

    FileSystem fileSystem = FileSystem.get(FsConstants.LOCAL_FS_URI,
        newConf);
    Assert.assertFalse("Unexpected FileSystem: " + fileSystem,
        ViewFileSystemUtil.isViewFileSystem(fileSystem));

    fileSystem = FileSystem.get(FsConstants.VIEWFS_URI,
        newConf);
    Assert.assertTrue("Unexpected FileSystem: " + fileSystem,
        ViewFileSystemUtil.isViewFileSystem(fileSystem));

    // Case 1: Verify FsStatus of root path returns all MountPoints status.
    Map<MountPoint, FsStatus> mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, InodeTree.SlashPath);
    Assert.assertEquals(getExpectedMountPoints(), mountPointFsStatusMap.size());

    // Case 2: Verify FsStatus of an internal dir returns all
    // MountPoints status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/internalDir"));
    Assert.assertEquals(getExpectedMountPoints(), mountPointFsStatusMap.size());

    // Case 3: Verify FsStatus of a matching MountPoint returns exactly
    // the corresponding MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/user"));
    Assert.assertEquals(1, mountPointFsStatusMap.size());
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      Assert.assertEquals(entry.getKey().getMountedOnPath().toString(),
          "/user");
    }

    // Case 4: Verify FsStatus of a path over a MountPoint returns the
    // corresponding MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("/user/cloud"));
    Assert.assertEquals(1, mountPointFsStatusMap.size());
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      Assert.assertEquals(entry.getKey().getMountedOnPath().toString(),
          "/user");
    }

    // Case 5: Verify FsStatus of any level of an internal dir
    // returns all MountPoints status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem,
            new Path("/internalDir/internalDir2"));
    Assert.assertEquals(getExpectedMountPoints(), mountPointFsStatusMap.size());

    // Case 6: Verify FsStatus of a MountPoint URI returns
    // the MountPoint status.
    mountPointFsStatusMap =
        ViewFileSystemUtil.getStatus(fileSystem, new Path("viewfs:/user/"));
    Assert.assertEquals(1, mountPointFsStatusMap.size());
    for (Entry<MountPoint, FsStatus> entry : mountPointFsStatusMap.entrySet()) {
      Assert.assertEquals(entry.getKey().getMountedOnPath().toString(),
          "/user");
    }

    // Case 7: Verify FsStatus of a non MountPoint path throws exception
    ViewFileSystemUtil.getStatus(fileSystem, new Path("/non-existing"));
  }

  @Test
  public void testCheckOwnerWithFileStatus()
      throws IOException, InterruptedException {
    final UserGroupInformation userUgi = UserGroupInformation
        .createUserForTesting("user@HADOOP.COM", new String[]{"hadoop"});
    userUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String doAsUserName = ugi.getUserName();
        assertEquals(doAsUserName, "user@HADOOP.COM");
        FileSystem vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
        FileStatus stat = vfs.getFileStatus(new Path("/internalDir"));
        assertEquals(userUgi.getShortUserName(), stat.getOwner());
        return null;
      }
    });
  }

  @Test
  public void testUsed() throws IOException {
    try {
      fsView.getUsed();
      fail("ViewFileSystem getUsed() should fail for slash root path when the" +
          " slash root mount point is not configured.");
    } catch (NotInMountpointException e) {
      // expected exception.
    }
    long usedSpaceByPathViaViewFs = fsView.getUsed(new Path("/user"));
    long usedSpaceByPathViaTargetFs =
        fsTarget.getUsed(new Path(targetTestRoot, "user"));
    assertEquals("Space used not matching between ViewFileSystem and " +
        "the mounted FileSystem!",
        usedSpaceByPathViaTargetFs, usedSpaceByPathViaViewFs);

    Path mountDataRootPath = new Path("/data");
    String fsTargetFileName = "debug.log";
    Path fsTargetFilePath = new Path(targetTestRoot, "data/debug.log");
    Path mountDataFilePath = new Path(mountDataRootPath, fsTargetFileName);
    fileSystemTestHelper.createFile(fsTarget, fsTargetFilePath);

    usedSpaceByPathViaViewFs = fsView.getUsed(mountDataFilePath);
    usedSpaceByPathViaTargetFs = fsTarget.getUsed(fsTargetFilePath);
    assertEquals("Space used not matching between ViewFileSystem and " +
        "the mounted FileSystem!",
        usedSpaceByPathViaTargetFs, usedSpaceByPathViaViewFs);
  }

  @Test
  public void testLinkTarget() throws Exception {

    Assume.assumeTrue(fsTarget.supportsSymlinks() &&
        fsTarget.areSymlinksEnabled());

    // Symbolic link
    final String targetFileName = "debug.log";
    final String linkFileName = "debug.link";
    final Path targetFile = new Path(targetTestRoot, targetFileName);
    final Path symLink = new Path(targetTestRoot, linkFileName);

    FileSystemTestHelper.createFile(fsTarget, targetFile);
    fsTarget.createSymlink(targetFile, symLink, false);

    final Path mountTargetRootPath = new Path("/targetRoot");
    final Path mountTargetSymLinkPath = new Path(mountTargetRootPath,
        linkFileName);
    final Path expectedMountLinkTarget = fsTarget.makeQualified(
        new Path(targetTestRoot, targetFileName));
    final Path actualMountLinkTarget = fsView.getLinkTarget(
        mountTargetSymLinkPath);

    assertEquals("Resolved link target path not matching!",
        expectedMountLinkTarget, actualMountLinkTarget);

    // Relative symbolic link
    final String relativeFileName = "dir2/../" + targetFileName;
    final String link2FileName = "dir2/rel.link";
    final Path relTargetFile = new Path(targetTestRoot, relativeFileName);
    final Path relativeSymLink = new Path(targetTestRoot, link2FileName);
    fsTarget.createSymlink(relTargetFile, relativeSymLink, true);

    final Path mountTargetRelativeSymLinkPath = new Path(mountTargetRootPath,
        link2FileName);
    final Path expectedMountRelLinkTarget = fsTarget.makeQualified(
        new Path(targetTestRoot, relativeFileName));
    final Path actualMountRelLinkTarget = fsView.getLinkTarget(
        mountTargetRelativeSymLinkPath);

    assertEquals("Resolved relative link target path not matching!",
        expectedMountRelLinkTarget, actualMountRelLinkTarget);

    try {
      fsView.getLinkTarget(new Path("/linkToAFile"));
      fail("Resolving link target for a ViewFs mount link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      assertThat(e.getMessage(),
          containsString("not a symbolic link"));
    }

    try {
      fsView.getLinkTarget(fsView.makeQualified(
          new Path(mountTargetRootPath, targetFileName)));
      fail("Resolving link target for a non sym link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      assertThat(e.getMessage(),
          containsString("not a symbolic link"));
    }

    try {
      fsView.getLinkTarget(new Path("/targetRoot/non-existing-file"));
      fail("Resolving link target for a non existing link should fail!");
    } catch (Exception e) {
      LOG.info("Expected exception: " + e);
      assertThat(e.getMessage(),
          containsString("File does not exist:"));
    }
  }

  @Test
  public void testViewFileSystemInnerCache() throws Exception {
    ViewFileSystem.InnerCache cache =
        new ViewFileSystem.InnerCache(new FsGetter());
    FileSystem fs = cache.get(fsTarget.getUri(), conf);

    // InnerCache caches filesystem.
    assertSame(fs, cache.get(fsTarget.getUri(), conf));

    // InnerCache and FileSystem.CACHE are independent.
    assertNotSame(fs, FileSystem.get(fsTarget.getUri(), conf));

    // close InnerCache.
    cache.closeAll();
    try {
      fs.exists(new Path("/"));
      if (!(fs instanceof LocalFileSystem)) {
        // Ignore LocalFileSystem because it can still be used after close.
        fail("Expect Filesystem closed exception");
      }
    } catch (IOException e) {
      assertExceptionContains("Filesystem closed", e);
    }
  }

  @Test
  public void testCloseChildrenFileSystem() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());
    config.setBoolean("fs.viewfs.impl.disable.cache", false);
    URI uri = new URI("viewfs://" + clusterName + "/");

    ViewFileSystem viewFs = (ViewFileSystem) FileSystem.get(uri, config);
    assertTrue("viewfs should have at least one child fs.",
        viewFs.getChildFileSystems().length > 0);
    // viewFs is cached in FileSystem.CACHE
    assertSame(viewFs, FileSystem.get(uri, config));

    // child fs is not cached in FileSystem.CACHE
    FileSystem child = viewFs.getChildFileSystems()[0];
    assertNotSame(child, FileSystem.get(child.getUri(), config));

    viewFs.close();
    for (FileSystem childfs : viewFs.getChildFileSystems()) {
      try {
        childfs.exists(new Path("/"));
        if (!(childfs instanceof LocalFileSystem)) {
          // Ignore LocalFileSystem because it can still be used after close.
          fail("Expect Filesystem closed exception");
        }
      } catch (IOException e) {
        assertExceptionContains("Filesystem closed", e);
      }
    }
  }

  @Test
  public void testChildrenFileSystemLeak() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());

    final int cacheSize = TestFileUtil.getCacheSize();
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem
        .get(new URI("viewfs://" + clusterName + "/"), config);
    assertEquals(cacheSize + 1, TestFileUtil.getCacheSize());
    viewFs.close();
    assertEquals(cacheSize, TestFileUtil.getCacheSize());
  }

  @Test
  public void testDeleteOnExit() throws Exception {
    final String clusterName = "cluster" + new Random().nextInt();
    Configuration config = new Configuration(conf);
    ConfigUtil.addLink(config, clusterName, "/user",
        new Path(targetTestRoot, "user").toUri());

    Path testDir = new Path("/user/testDeleteOnExit");
    Path realTestPath = new Path(targetTestRoot, "user/testDeleteOnExit");
    ViewFileSystem viewFs = (ViewFileSystem) FileSystem
        .get(new URI("viewfs://" + clusterName + "/"), config);
    viewFs.mkdirs(testDir);
    assertTrue(viewFs.exists(testDir));
    assertTrue(fsTarget.exists(realTestPath));

    viewFs.deleteOnExit(testDir);
    viewFs.close();
    assertFalse(fsTarget.exists(realTestPath));
  }

  @Test
  public void testGetContentSummary() throws IOException {
    ContentSummary summaryBefore =
        fsView.getContentSummary(new Path("/internalDir"));
    String expected = "GET CONTENT SUMMARY";
    Path filePath =
        new Path("/internalDir/internalDir2/linkToDir3", "foo");

    try (FSDataOutputStream outputStream = fsView.create(filePath)) {
      outputStream.write(expected.getBytes());
    }

    Path newDirPath = new Path("/internalDir/linkToDir2", "bar");
    fsView.mkdirs(newDirPath);

    ContentSummary summaryAfter =
        fsView.getContentSummary(new Path("/internalDir"));
    assertEquals("The file count didn't match",
        summaryBefore.getFileCount() + 1,
        summaryAfter.getFileCount());
    assertEquals("The size didn't match",
        summaryBefore.getLength() + expected.length(),
        summaryAfter.getLength());
    assertEquals("The directory count didn't match",
        summaryBefore.getDirectoryCount() + 1,
        summaryAfter.getDirectoryCount());
  }

  @Test
  public void testGetContentSummaryWithFileInLocalFS() throws Exception {
    ContentSummary summaryBefore =
        fsView.getContentSummary(new Path("/internalDir"));
    String expected = "GET CONTENT SUMMARY";
    File localFile = temporaryFolder.newFile("localFile");
    try (FileOutputStream fos = new FileOutputStream(localFile)) {
      fos.write(expected.getBytes());
    }
    ConfigUtil.addLink(conf,
        "/internalDir/internalDir2/linkToLocalFile", localFile.toURI());

    try (FileSystem fs = FileSystem.get(FsConstants.VIEWFS_URI, conf)) {
      ContentSummary summaryAfter =
          fs.getContentSummary(new Path("/internalDir"));
      assertEquals("The file count didn't match",
          summaryBefore.getFileCount() + 1,
          summaryAfter.getFileCount());
      assertEquals("The directory count didn't match",
          summaryBefore.getLength() + expected.length(),
          summaryAfter.getLength());
    }
  }
}
