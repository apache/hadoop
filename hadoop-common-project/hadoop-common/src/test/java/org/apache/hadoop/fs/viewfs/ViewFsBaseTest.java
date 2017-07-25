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

import static org.apache.hadoop.fs.FileContextTestHelper.checkFileLinkStatus;
import static org.apache.hadoop.fs.FileContextTestHelper.checkFileStatus;
import static org.apache.hadoop.fs.FileContextTestHelper.exists;
import static org.apache.hadoop.fs.FileContextTestHelper.isDir;
import static org.apache.hadoop.fs.FileContextTestHelper.isFile;
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_555;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FileContextTestHelper.fileType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.viewfs.ViewFs.MountPoint;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * <p>
 * A collection of tests for the {@link ViewFs}.
 * This test should be used for testing ViewFs that has mount links to 
 * a target file system such  localFs or Hdfs etc.

 * </p>
 * <p>
 * To test a given target file system create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fcTarget</code> 
 * to point to the file system to which you want the mount targets
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */
abstract public class ViewFsBaseTest {
  FileContext fcView; // the view file system - the mounts are here
  FileContext fcTarget; // the target file system - the mount will point here
  Path targetTestRoot;
  Configuration conf;
  FileContext xfcViewWithAuthority; // same as fsView but with authority
  URI schemeWithAuthority;
  final FileContextTestHelper fileContextTestHelper = createFileContextHelper();

  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  @Before
  public void setUp() throws Exception {
    initializeTargetTestRoot();
    
    // Make  user and data dirs - we creates links to them in the mount table
    fcTarget.mkdir(new Path(targetTestRoot,"user"),
        FileContext.DEFAULT_PERM, true);
    fcTarget.mkdir(new Path(targetTestRoot,"data"),
        FileContext.DEFAULT_PERM, true);
    fcTarget.mkdir(new Path(targetTestRoot,"dir2"),
        FileContext.DEFAULT_PERM, true);
    fcTarget.mkdir(new Path(targetTestRoot,"dir3"),
        FileContext.DEFAULT_PERM, true);
    FileContextTestHelper.createFile(fcTarget, new Path(targetTestRoot,"aFile"));
    
    
    // Now we use the mount fs to set links to user and dir
    // in the test root
    
    // Set up the defaultMT in the config with our mount point links
    conf = new Configuration();
    ConfigUtil.addLink(conf, "/targetRoot", targetTestRoot.toUri());
    ConfigUtil.addLink(conf, "/user",
        new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/user2",
        new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/data",
        new Path(targetTestRoot,"data").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path(targetTestRoot,"dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3",
        new Path(targetTestRoot,"dir3").toUri());
    ConfigUtil.addLink(conf, "/danglingLink",
        new Path(targetTestRoot,"missingTarget").toUri());
    ConfigUtil.addLink(conf, "/linkToAFile",
        new Path(targetTestRoot,"aFile").toUri());
    
    fcView = FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
    // Also try viewfs://default/    - note authority is name of mount table
  }
  
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fileContextTestHelper.getAbsoluteTestRootPath(fcTarget);
    // In case previous test was killed before cleanup
    fcTarget.delete(targetTestRoot, true);
    
    fcTarget.mkdir(targetTestRoot, FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    fcTarget.delete(fileContextTestHelper.getTestRootPath(fcTarget), true);
  }
  
  @Test
  public void testGetMountPoints() {
    ViewFs viewfs = (ViewFs) fcView.getDefaultFileSystem();
    MountPoint[] mountPoints = viewfs.getMountPoints();
    Assert.assertEquals(8, mountPoints.length);
  }
  
  int getExpectedDelegationTokenCount() {
    return 0;
  }
  
  /**
   * This default implementation is when viewfs has mount points
   * into file systems, such as LocalFs that do no have delegation tokens.
   * It should be overridden for when mount points into hdfs.
   */
  @Test
  public void testGetDelegationTokens() throws IOException {
    List<Token<?>> delTokens = 
        fcView.getDelegationTokens(new Path("/"), "sanjay");
    Assert.assertEquals(getExpectedDelegationTokenCount(), delTokens.size());
  }

  
  @Test
  public void testBasicPaths() {
    Assert.assertEquals(FsConstants.VIEWFS_URI,
        fcView.getDefaultFileSystem().getUri());
    Assert.assertEquals(fcView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fcView.getWorkingDirectory());
    Assert.assertEquals(fcView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fcView.getHomeDirectory());
    Assert.assertEquals(
        new Path("/foo/bar").makeQualified(FsConstants.VIEWFS_URI, null),
        fcView.makeQualified(new Path("/foo/bar")));
  }
  
  /** 
   * Test modify operations (create, mkdir, delete, etc) 
   * on the mount file system where the pathname references through
   * the mount points.  Hence these operation will modify the target
   * file system.
   * 
   * Verify the operation via mountfs (ie fc) and *also* via the
   *  target file system (ie fclocal) that the mount link points-to.
   */
  @Test
  public void testOperationsThroughMountLinks() throws IOException {
    // Create file 
    fileContextTestHelper.createFileNonRecursive(fcView, "/user/foo");
    Assert.assertTrue("Create file should be file",
		isFile(fcView, new Path("/user/foo")));
    Assert.assertTrue("Target of created file should be type file",
        isFile(fcTarget, new Path(targetTestRoot,"user/foo")));
    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",
        fcView.delete(new Path("/user/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        exists(fcView, new Path("/user/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        exists(fcTarget, new Path(targetTestRoot,"user/foo")));
    
    // Create file with a 2 component dirs
    fileContextTestHelper.createFileNonRecursive(fcView,
        "/internalDir/linkToDir2/foo");
    Assert.assertTrue("Created file should be type file",
        isFile(fcView, new Path("/internalDir/linkToDir2/foo")));
    Assert.assertTrue("Target of created file should be type file",
        isFile(fcTarget, new Path(targetTestRoot,"dir2/foo")));
    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",
        fcView.delete(new Path("/internalDir/linkToDir2/foo"),false));
    Assert.assertFalse("File should not exist after deletion",
        exists(fcView, new Path("/internalDir/linkToDir2/foo")));
    Assert.assertFalse("Target should not exist after deletion",
        exists(fcTarget, new Path(targetTestRoot,"dir2/foo")));
    
    
    // Create file with a 3 component dirs
    fileContextTestHelper.createFileNonRecursive(fcView,
        "/internalDir/internalDir2/linkToDir3/foo");
    Assert.assertTrue("Created file should be of type file", 
        isFile(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertTrue("Target of created file should also be type file",
        isFile(fcTarget, new Path(targetTestRoot,"dir3/foo")));
    
    // Recursive Create file with missing dirs
    fileContextTestHelper.createFile(fcView,
        "/internalDir/linkToDir2/missingDir/miss2/foo");
    Assert.assertTrue("Created file should be of type file",
      isFile(fcView, new Path("/internalDir/linkToDir2/missingDir/miss2/foo")));
    Assert.assertTrue("Target of created file should also be type file",
        isFile(fcTarget, new Path(targetTestRoot,"dir2/missingDir/miss2/foo")));

    
    // Delete the created file
    Assert.assertTrue("Delete should succeed",  fcView.delete(
        new Path("/internalDir/internalDir2/linkToDir3/foo"), false));
    Assert.assertFalse("Deleted File should not exist", 
        exists(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")));
    Assert.assertFalse("Target of deleted file should not exist", 
        exists(fcTarget, new Path(targetTestRoot,"dir3/foo")));
    
      
    // mkdir
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
        FileContext.DEFAULT_PERM, false);
    Assert.assertTrue("New dir should be type dir", 
        isDir(fcView, new Path("/user/dirX")));
    Assert.assertTrue("Target of new dir should be of type dir",
        isDir(fcTarget, new Path(targetTestRoot,"user/dirX")));
    
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"),
        FileContext.DEFAULT_PERM, false);
    Assert.assertTrue("New dir should be type dir", 
        isDir(fcView, new Path("/user/dirX/dirY")));
    Assert.assertTrue("Target of new dir should be of type dir",
        isDir(fcTarget,new Path(targetTestRoot,"user/dirX/dirY")));
    

    // Delete the created dir
    Assert.assertTrue("Delete should succeed",
        fcView.delete(new Path("/user/dirX/dirY"), false));
    Assert.assertFalse("Deleted File should not exist",
        exists(fcView, new Path("/user/dirX/dirY")));
    Assert.assertFalse("Deleted Target should not exist", 
        exists(fcTarget, new Path(targetTestRoot,"user/dirX/dirY")));
    
    Assert.assertTrue("Delete should succeed",
        fcView.delete(new Path("/user/dirX"), false));
    Assert.assertFalse("Deleted File should not exist",
        exists(fcView, new Path("/user/dirX")));
    Assert.assertFalse("Deleted Target should not exist",
        exists(fcTarget, new Path(targetTestRoot,"user/dirX")));
    
    // Rename a file 
    fileContextTestHelper.createFile(fcView, "/user/foo");
    fcView.rename(new Path("/user/foo"), new Path("/user/fooBar"));
    Assert.assertFalse("Renamed src should not exist", 
        exists(fcView, new Path("/user/foo")));
    Assert.assertFalse(exists(fcTarget, new Path(targetTestRoot,"user/foo")));
    Assert.assertTrue(isFile(fcView,
        fileContextTestHelper.getTestRootPath(fcView,"/user/fooBar")));
    Assert.assertTrue(isFile(fcTarget, new Path(targetTestRoot,"user/fooBar")));
    
    fcView.mkdir(new Path("/user/dirFoo"), FileContext.DEFAULT_PERM, false);
    fcView.rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
    Assert.assertFalse("Renamed src should not exist",
        exists(fcView, new Path("/user/dirFoo")));
    Assert.assertFalse("Renamed src should not exist in target",
        exists(fcTarget, new Path(targetTestRoot,"user/dirFoo")));
    Assert.assertTrue("Renamed dest should  exist as dir",
        isDir(fcView,
        fileContextTestHelper.getTestRootPath(fcView,"/user/dirFooBar")));
    Assert.assertTrue("Renamed dest should  exist as dir in target",
        isDir(fcTarget,new Path(targetTestRoot,"user/dirFooBar")));
    
    // Make a directory under a directory that's mounted from the root of another FS
    fcView.mkdir(new Path("/targetRoot/dirFoo"), FileContext.DEFAULT_PERM, false);
    Assert.assertTrue(exists(fcView, new Path("/targetRoot/dirFoo")));
    boolean dirFooPresent = false;
    RemoteIterator<FileStatus> dirContents = fcView.listStatus(new Path(
        "/targetRoot/"));
    while (dirContents.hasNext()) {
      FileStatus fileStatus = dirContents.next();
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    Assert.assertTrue(dirFooPresent);
    RemoteIterator<LocatedFileStatus> dirLocatedContents =
        fcView.listLocatedStatus(new Path("/targetRoot/"));
    dirFooPresent = false;
    while (dirLocatedContents.hasNext()) {
      FileStatus fileStatus = dirLocatedContents.next();
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    Assert.assertTrue(dirFooPresent);
  }
  
  // rename across mount points that point to same target also fail 
  @Test
  public void testRenameAcrossMounts1() throws IOException {
    fileContextTestHelper.createFile(fcView, "/user/foo");
    try {
      fcView.rename(new Path("/user/foo"), new Path("/user2/fooBarBar"));
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
    fileContextTestHelper.createFile(fcView, "/user/foo");
    try {
      fcView.rename(new Path("/user/foo"), new Path("/data/fooBar"));
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

    FileContext fcView2 =
        FileContext.getFileContext(FsConstants.VIEWFS_URI, conf2);
    String user1Path = "/user/foo";
    fileContextTestHelper.createFile(fcView2, user1Path);
    String user2Path = "/user2/fooBarBar";
    Path user2Dst = new Path(user2Path);
    fcView2.rename(new Path(user1Path), user2Dst);
    ContractTestUtils
        .assertPathDoesNotExist(fcView2, "src should not exist after rename",
            new Path(user1Path));
    ContractTestUtils
        .assertPathDoesNotExist(fcTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fcView2,
        fileContextTestHelper.getTestRootPath(fcView2, user2Path));
    ContractTestUtils
        .assertIsFile(fcTarget, new Path(targetTestRoot, "user/fooBarBar"));
  }

  // RenameStrategy SAME_FILESYSTEM_ACROSS_MOUNTPOINT enabled
  // to rename across mount points if the mount link targets are different
  // but are part of the same target FS
  @Test
  public void testRenameAcrossMounts4() throws IOException {
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT
            .toString());
    FileContext fcView2 =
        FileContext.getFileContext(FsConstants.VIEWFS_URI, conf2);
    String userPath = "/user/foo";
    fileContextTestHelper.createFile(fcView2, userPath);
    String anotherMountPath = "/data/fooBar";
    Path anotherDst = new Path(anotherMountPath);
    fcView2.rename(new Path(userPath), anotherDst);

    ContractTestUtils
        .assertPathDoesNotExist(fcView2, "src should not exist after rename",
            new Path(userPath));
    ContractTestUtils
        .assertPathDoesNotExist(fcTarget, "src should not exist after rename",
            new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fcView2,
        fileContextTestHelper.getTestRootPath(fcView2, anotherMountPath));
    ContractTestUtils
        .assertIsFile(fcView2, new Path(targetTestRoot, "data/fooBar"));
  }

  static protected boolean SupportsBlocks = false; //  local fs use 1 block
                                                   // override for HDFS
  @Test
  public void testGetBlockLocations() throws IOException {
    Path targetFilePath = new Path(targetTestRoot,"data/largeFile");
    FileContextTestHelper.createFile(fcTarget, targetFilePath, 10, 1024);
    Path viewFilePath = new Path("/data/largeFile");
    checkFileStatus(fcView, viewFilePath.toString(), fileType.isFile);
    BlockLocation[] viewBL = fcView.getFileBlockLocations(viewFilePath,
        0, 10240+100);
    Assert.assertEquals(SupportsBlocks ? 10 : 1, viewBL.length);
    BlockLocation[] targetBL = fcTarget.getFileBlockLocations(targetFilePath, 0, 10240+100);
    compareBLs(viewBL, targetBL);
    
    
    // Same test but now get it via the FileStatus Parameter
    fcView.getFileBlockLocations(viewFilePath, 0, 10240+100);
    targetBL = fcTarget.getFileBlockLocations(targetFilePath, 0, 10240+100);
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
    
    FileStatus[] dirPaths = fcView.util().listStatus(new Path("/"));
    FileStatus fs;
    Assert.assertEquals(7, dirPaths.length);
    fs = fileContextTestHelper.containsPath(fcView, "/user", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileContextTestHelper.containsPath(fcView, "/data", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileContextTestHelper.containsPath(fcView, "/internalDir", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("InternalDirs should appear as dir", fs.isDirectory());
    fs = fileContextTestHelper.containsPath(fcView, "/danglingLink", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
    fs = fileContextTestHelper.containsPath(fcView, "/linkToAFile", dirPaths);
      Assert.assertNotNull(fs);
      Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
      
      
      
      // list on internal dir
      dirPaths = fcView.util().listStatus(new Path("/internalDir"));
      Assert.assertEquals(2, dirPaths.length);

      fs = fileContextTestHelper.containsPath(fcView,
          "/internalDir/internalDir2", dirPaths);
        Assert.assertNotNull(fs);
        Assert.assertTrue("InternalDirs should appear as dir",fs.isDirectory());
      fs = fileContextTestHelper.containsPath(fcView,
          "/internalDir/linkToDir2", dirPaths);
        Assert.assertNotNull(fs);
        Assert.assertTrue("A mount should appear as symlink", fs.isSymlink());
  }
      
  @Test
  public void testFileStatusOnMountLink() throws IOException {
    Assert.assertTrue("Slash should appear as dir", 
        fcView.getFileStatus(new Path("/")).isDirectory());
    checkFileStatus(fcView, "/", fileType.isDir);
    checkFileStatus(fcView, "/user", fileType.isDir);
    checkFileStatus(fcView, "/data", fileType.isDir);
    checkFileStatus(fcView, "/internalDir", fileType.isDir);
    checkFileStatus(fcView, "/internalDir/linkToDir2", fileType.isDir);
    checkFileStatus(fcView, "/internalDir/internalDir2/linkToDir3", fileType.isDir);
    checkFileStatus(fcView, "/linkToAFile", fileType.isFile);

    try {
      fcView.getFileStatus(new Path("/danglingLink"));
      Assert.fail("Excepted a not found exception here");
    } catch ( FileNotFoundException e) {
      // as excepted
    }
  }
  
  @Test
  public void testGetFileChecksum() throws AccessControlException,
      UnresolvedLinkException, IOException {
    AbstractFileSystem mockAFS = mock(AbstractFileSystem.class);
    InodeTree.ResolveResult<AbstractFileSystem> res =
      new InodeTree.ResolveResult<AbstractFileSystem>(null, mockAFS , null,
        new Path("someFile"));
    @SuppressWarnings("unchecked")
    InodeTree<AbstractFileSystem> fsState = mock(InodeTree.class);
    when(fsState.resolve(anyString(), anyBoolean())).thenReturn(res);
    ViewFs vfs = mock(ViewFs.class);
    vfs.fsState = fsState;

    when(vfs.getFileChecksum(new Path("/tmp/someFile")))
      .thenCallRealMethod();
    vfs.getFileChecksum(new Path("/tmp/someFile"));

    verify(mockAFS).getFileChecksum(new Path("someFile"));
  }

  @Test(expected=FileNotFoundException.class) 
  public void testgetFSonDanglingLink() throws IOException {
    fcView.getFileStatus(new Path("/danglingLink"));
  }
  
  
  @Test(expected=FileNotFoundException.class) 
  public void testgetFSonNonExistingInternalDir() throws IOException {
    fcView.getFileStatus(new Path("/internalDir/nonExisting"));
  }
  
  @Test
  public void testgetFileLinkStatus() throws IOException {
    checkFileLinkStatus(fcView, "/user", fileType.isSymlink);
    checkFileLinkStatus(fcView, "/data", fileType.isSymlink);
    checkFileLinkStatus(fcView, "/internalDir/linkToDir2", fileType.isSymlink);
    checkFileLinkStatus(fcView, "/internalDir/internalDir2/linkToDir3",
        fileType.isSymlink);
    checkFileLinkStatus(fcView, "/linkToAFile", fileType.isSymlink);
    checkFileLinkStatus(fcView, "/internalDir", fileType.isDir);
    checkFileLinkStatus(fcView, "/internalDir/internalDir2", fileType.isDir);
  }
  
  @Test(expected=FileNotFoundException.class) 
  public void testgetFileLinkStatusonNonExistingInternalDir()
    throws IOException {
    fcView.getFileLinkStatus(new Path("/internalDir/nonExisting"));
  }
  
  @Test
  public void testSymlinkTarget() throws IOException {

    // get link target`
    Assert.assertEquals(fcView.getLinkTarget(new Path("/user")),
        (new Path(targetTestRoot,"user")));
    Assert.assertEquals(fcView.getLinkTarget(new Path("/data")),
        (new Path(targetTestRoot,"data")));
    Assert.assertEquals(
        fcView.getLinkTarget(new Path("/internalDir/linkToDir2")),
        (new Path(targetTestRoot,"dir2")));
    Assert.assertEquals(
        fcView.getLinkTarget(new Path("/internalDir/internalDir2/linkToDir3")),
        (new Path(targetTestRoot,"dir3")));
    Assert.assertEquals(fcView.getLinkTarget(new Path("/linkToAFile")),
        (new Path(targetTestRoot,"aFile")));
  }
  
  @Test(expected=IOException.class) 
  public void testgetLinkTargetOnNonLink() throws IOException {
    fcView.getLinkTarget(new Path("/internalDir/internalDir2"));
  }
  
  /*
   * Test resolvePath(p) 
   * TODO In the tests below replace 
   * fcView.getDefaultFileSystem().resolvePath() fcView.resolvePath()
   */
  
  @Test
  public void testResolvePathInternalPaths() throws IOException {
    Assert.assertEquals(new Path("/"), fcView.resolvePath(new Path("/")));
    Assert.assertEquals(new Path("/internalDir"),
                          fcView.resolvePath(new Path("/internalDir")));
  }
  @Test
  public void testResolvePathMountPoints() throws IOException {
    Assert.assertEquals(new Path(targetTestRoot,"user"),
                          fcView.resolvePath(new Path("/user")));
    Assert.assertEquals(new Path(targetTestRoot,"data"),
        fcView.resolvePath(new Path("/data")));
    Assert.assertEquals(new Path(targetTestRoot,"dir2"),
        fcView.resolvePath(new Path("/internalDir/linkToDir2")));
    Assert.assertEquals(new Path(targetTestRoot,"dir3"),
        fcView.resolvePath(new Path("/internalDir/internalDir2/linkToDir3")));

  }
  
  @Test
  public void testResolvePathThroughMountPoints() throws IOException {
    fileContextTestHelper.createFile(fcView, "/user/foo");
    Assert.assertEquals(new Path(targetTestRoot,"user/foo"),
                          fcView.resolvePath(new Path("/user/foo")));
    
    fcView.mkdir(
        fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
        FileContext.DEFAULT_PERM, false);
    Assert.assertEquals(new Path(targetTestRoot,"user/dirX"),
        fcView.resolvePath(new Path("/user/dirX")));

    
    fcView.mkdir(
        fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"),
        FileContext.DEFAULT_PERM, false);
    Assert.assertEquals(new Path(targetTestRoot,"user/dirX/dirY"),
        fcView.resolvePath(new Path("/user/dirX/dirY")));
  }

  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathDanglingLink() throws IOException {
      fcView.resolvePath(new Path("/danglingLink"));
  }
  
  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathMissingThroughMountPoints() throws IOException {
    fcView.resolvePath(new Path("/user/nonExisting"));
  }
  

  @Test(expected=FileNotFoundException.class) 
  public void testResolvePathMissingThroughMountPoints2() throws IOException {
    fcView.mkdir(
        fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
        FileContext.DEFAULT_PERM, false);
    fcView.resolvePath(new Path("/user/dirX/nonExisting"));
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
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/"),
        FileContext.DEFAULT_PERM, false);
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirExisting1() throws IOException {
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir"),
        FileContext.DEFAULT_PERM, false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirExisting2() throws IOException {
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView,
        "/internalDir/linkToDir2"),
        FileContext.DEFAULT_PERM, false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew() throws IOException {
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/dirNew"),
        FileContext.DEFAULT_PERM, false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalMkdirNew2() throws IOException {
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir/dirNew"),
        FileContext.DEFAULT_PERM, false);
  }
  
  // Create on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate1() throws IOException {
    fileContextTestHelper.createFileNonRecursive(fcView, "/foo"); // 1 component
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreate2() throws IOException {  // 2 component
    fileContextTestHelper.createFileNonRecursive(fcView, "/internalDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir() throws IOException {
    fileContextTestHelper.createFile(fcView, "/missingDir/foo");
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir2() throws IOException {
    fileContextTestHelper.createFile(fcView, "/missingDir/miss2/foo");
  }
  
  
  @Test(expected=AccessControlException.class) 
  public void testInternalCreateMissingDir3() throws IOException {
    fileContextTestHelper.createFile(fcView, "/internalDir/miss2/foo");
  }
  
  // Delete on internal mount table should fail
  
  @Test(expected=FileNotFoundException.class) 
  public void testInternalDeleteNonExisting() throws IOException {
      fcView.delete(new Path("/NonExisting"), false);
  }
  @Test(expected=FileNotFoundException.class) 
  public void testInternalDeleteNonExisting2() throws IOException {
      fcView.delete(new Path("/internalDir/NonExisting"), false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalDeleteExisting() throws IOException {
      fcView.delete(new Path("/internalDir"), false);
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalDeleteExisting2() throws IOException {
    Assert.assertTrue("Delete of link to dir should succeed",
        fcView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory());
    fcView.delete(new Path("/internalDir/linkToDir2"), false);
  } 
  
  
  // Rename on internal mount table should fail
  
  @Test(expected=AccessControlException.class) 
  public void testInternalRename1() throws IOException {
    fcView.rename(new Path("/internalDir"), new Path("/newDir"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRename2() throws IOException {
    Assert.assertTrue("linkTODir2 should be a dir", 
        fcView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory());
    fcView.rename(new Path("/internalDir/linkToDir2"),
        new Path("/internalDir/dir1"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRename3() throws IOException {
    fcView.rename(new Path("/user"), new Path("/internalDir/linkToDir2"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRenameToSlash() throws IOException {
    fcView.rename(new Path("/internalDir/linkToDir2/foo"), new Path("/"));
  }
  @Test(expected=AccessControlException.class) 
  public void testInternalRenameFromSlash() throws IOException {
    fcView.rename(new Path("/"), new Path("/bar"));
  }
  
  @Test(expected=AccessControlException.class) 
  public void testInternalSetOwner() throws IOException {
    fcView.setOwner(new Path("/internalDir"), "foo", "bar");
  }

  /**
   * Verify the behavior of ACL operations on paths above the root of
   * any mount table entry.
   */

  @Test(expected=AccessControlException.class)
  public void testInternalModifyAclEntries() throws IOException {
    fcView.modifyAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveAclEntries() throws IOException {
    fcView.removeAclEntries(new Path("/internalDir"),
        new ArrayList<AclEntry>());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveDefaultAcl() throws IOException {
    fcView.removeDefaultAcl(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveAcl() throws IOException {
    fcView.removeAcl(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalSetAcl() throws IOException {
    fcView.setAcl(new Path("/internalDir"), new ArrayList<AclEntry>());
  }

  @Test
  public void testInternalGetAclStatus() throws IOException {
    final UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
    AclStatus aclStatus = fcView.getAclStatus(new Path("/internalDir"));
    assertEquals(aclStatus.getOwner(), currentUser.getUserName());
    assertEquals(aclStatus.getGroup(), currentUser.getGroupNames()[0]);
    assertEquals(aclStatus.getEntries(),
        AclUtil.getMinimalAcl(PERMISSION_555));
    assertFalse(aclStatus.isStickyBit());
  }

  @Test(expected=AccessControlException.class)
  public void testInternalSetXAttr() throws IOException {
    fcView.setXAttr(new Path("/internalDir"), "xattrName", null);
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttr() throws IOException {
    fcView.getXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttrs() throws IOException {
    fcView.getXAttrs(new Path("/internalDir"));
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalGetXAttrsWithNames() throws IOException {
    fcView.getXAttrs(new Path("/internalDir"), new ArrayList<String>());
  }

  @Test(expected=NotInMountpointException.class)
  public void testInternalListXAttr() throws IOException {
    fcView.listXAttrs(new Path("/internalDir"));
  }

  @Test(expected=AccessControlException.class)
  public void testInternalRemoveXAttr() throws IOException {
    fcView.removeXAttr(new Path("/internalDir"), "xattrName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot1() throws IOException {
    fcView.createSnapshot(new Path("/internalDir"));
  }

  @Test(expected = AccessControlException.class)
  public void testInternalCreateSnapshot2() throws IOException {
    fcView.createSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalRenameSnapshot() throws IOException {
    fcView.renameSnapshot(new Path("/internalDir"), "snapOldName",
        "snapNewName");
  }

  @Test(expected = AccessControlException.class)
  public void testInternalDeleteSnapshot() throws IOException {
    fcView.deleteSnapshot(new Path("/internalDir"), "snap1");
  }

  @Test
  public void testOwnerForInternalDir()
      throws IOException, InterruptedException, URISyntaxException {
    final UserGroupInformation userUgi = UserGroupInformation
        .createUserForTesting("user@HADOOP.COM", new String[]{"hadoop"});
    userUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException, URISyntaxException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String doAsUserName = ugi.getUserName();
        assertEquals(doAsUserName, "user@HADOOP.COM");
        FileContext
            viewFS = FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
        FileStatus stat = viewFS.getFileStatus(new Path("/internalDir"));
        assertEquals(userUgi.getShortUserName(), stat.getOwner());
        return null;
      }
    });
  }

  @Test
  public void testRespectsServerDefaults() throws Exception {
    FsServerDefaults targetDefs =
        fcTarget.getDefaultFileSystem().getServerDefaults(new Path("/"));
    FsServerDefaults viewDefs =
        fcView.getDefaultFileSystem().getServerDefaults(new Path("/data"));
    assertEquals(targetDefs.getReplication(), viewDefs.getReplication());
    assertEquals(targetDefs.getBlockSize(), viewDefs.getBlockSize());
    assertEquals(targetDefs.getBytesPerChecksum(),
        viewDefs.getBytesPerChecksum());
    assertEquals(targetDefs.getFileBufferSize(),
        viewDefs.getFileBufferSize());
    assertEquals(targetDefs.getWritePacketSize(),
        viewDefs.getWritePacketSize());
    assertEquals(targetDefs.getEncryptDataTransfer(),
        viewDefs.getEncryptDataTransfer());
    assertEquals(targetDefs.getTrashInterval(), viewDefs.getTrashInterval());
    assertEquals(targetDefs.getChecksumType(), viewDefs.getChecksumType());

    fcView.create(new Path("/data/file"), EnumSet.of(CreateFlag.CREATE))
        .close();
    FileStatus stat =
        fcTarget.getFileStatus(new Path(targetTestRoot, "data/file"));
    assertEquals(targetDefs.getReplication(), stat.getReplication());
  }

  @Test
  public void testServerDefaultsInternalDir() throws Exception {
    FsServerDefaults localDefs = LocalConfigKeys.getServerDefaults();
    FsServerDefaults viewDefs = fcView
        .getDefaultFileSystem().getServerDefaults(new Path("/internalDir"));
    assertEquals(localDefs.getReplication(), viewDefs.getReplication());
    assertEquals(localDefs.getBlockSize(), viewDefs.getBlockSize());
    assertEquals(localDefs.getBytesPerChecksum(),
        viewDefs.getBytesPerChecksum());
    assertEquals(localDefs.getFileBufferSize(),
        viewDefs.getFileBufferSize());
    assertEquals(localDefs.getWritePacketSize(),
        viewDefs.getWritePacketSize());
    assertEquals(localDefs.getEncryptDataTransfer(),
        viewDefs.getEncryptDataTransfer());
    assertEquals(localDefs.getTrashInterval(), viewDefs.getTrashInterval());
    assertEquals(localDefs.getChecksumType(), viewDefs.getChecksumType());
  }

  // Confirm that listLocatedStatus is delegated properly to the underlying
  // AbstractFileSystem to allow for optimizations
  @Test
  public void testListLocatedStatus() throws IOException {
    final Path mockTarget = new Path("mockfs://listLocatedStatus/foo");
    final Path mountPoint = new Path("/fooMount");
    final Configuration newConf = new Configuration();
    newConf.setClass("fs.AbstractFileSystem.mockfs.impl", MockFs.class,
        AbstractFileSystem.class);
    ConfigUtil.addLink(newConf, mountPoint.toString(), mockTarget.toUri());
    FileContext.getFileContext(URI.create("viewfs:///"), newConf)
        .listLocatedStatus(mountPoint);
    AbstractFileSystem mockFs = MockFs.getMockFs(mockTarget.toUri());
    verify(mockFs).listLocatedStatus(new Path(mockTarget.toUri().getPath()));
    verify(mockFs, never()).listStatus(any(Path.class));
    verify(mockFs, never()).listStatusIterator(any(Path.class));
  }

  // Confirm that listStatus is delegated properly to the underlying
  // AbstractFileSystem's listStatusIterator to allow for optimizations
  @Test
  public void testListStatusIterator() throws IOException {
    final Path mockTarget = new Path("mockfs://listStatusIterator/foo");
    final Path mountPoint = new Path("/fooMount");
    final Configuration newConf = new Configuration();
    newConf.setClass("fs.AbstractFileSystem.mockfs.impl", MockFs.class,
        AbstractFileSystem.class);
    ConfigUtil.addLink(newConf, mountPoint.toString(), mockTarget.toUri());
    FileContext.getFileContext(URI.create("viewfs:///"), newConf)
        .listStatus(mountPoint);
    AbstractFileSystem mockFs = MockFs.getMockFs(mockTarget.toUri());
    verify(mockFs).listStatusIterator(new Path(mockTarget.toUri().getPath()));
    verify(mockFs, never()).listStatus(any(Path.class));
  }

  static class MockFs extends ChRootedFs {
    private static Map<String, AbstractFileSystem> fsCache = new HashMap<>();
    MockFs(URI uri, Configuration conf) throws URISyntaxException {
      super(getMockFs(uri), new Path("/"));
    }
    static AbstractFileSystem getMockFs(URI uri) {
      AbstractFileSystem mockFs = fsCache.get(uri.getAuthority());
      if (mockFs == null) {
        mockFs = mock(AbstractFileSystem.class);
        when(mockFs.getUri()).thenReturn(uri);
        when(mockFs.getUriDefaultPort()).thenReturn(1);
        when(mockFs.getUriPath(any(Path.class))).thenCallRealMethod();
        when(mockFs.isValidName(anyString())).thenReturn(true);
        fsCache.put(uri.getAuthority(), mockFs);
      }
      return mockFs;
    }
  }
}
