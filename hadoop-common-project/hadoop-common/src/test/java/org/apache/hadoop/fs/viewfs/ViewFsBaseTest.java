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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
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
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


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
  protected static final String MOUNT_TABLE_NAME = "mycluster";

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

  @BeforeEach
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
    conf.set(
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE_NAME_KEY,
        MOUNT_TABLE_NAME);
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

  @AfterEach
  public void tearDown() throws Exception {
    fcTarget.delete(fileContextTestHelper.getTestRootPath(fcTarget), true);
  }
  
  @Test
  public void testGetMountPoints() {
    ViewFs viewfs = (ViewFs) fcView.getDefaultFileSystem();
    MountPoint[] mountPoints = viewfs.getMountPoints();
    assertEquals(8, mountPoints.length);
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
    assertEquals(getExpectedDelegationTokenCount(), delTokens.size());
  }

  
  @Test
  public void testBasicPaths() {
    assertEquals(FsConstants.VIEWFS_URI,
        fcView.getDefaultFileSystem().getUri());
    assertEquals(fcView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fcView.getWorkingDirectory());
    assertEquals(fcView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fcView.getHomeDirectory());
    assertEquals(
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
    assertTrue(isFile(fcView, new Path("/user/foo")),
        "Create file should be file");
    assertTrue(isFile(fcTarget, new Path(targetTestRoot, "user/foo")),
        "Target of created file should be type file");
    
    // Delete the created file
    assertTrue(fcView.delete(new Path("/user/foo"), false),
        "Delete should succeed");
    assertFalse(exists(fcView, new Path("/user/foo")), "File should not exist after delete");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "user/foo")),
        "Target File should not exist after delete");
    
    // Create file with a 2 component dirs
    fileContextTestHelper.createFileNonRecursive(fcView,
        "/internalDir/linkToDir2/foo");
    assertTrue(isFile(fcView, new Path("/internalDir/linkToDir2/foo")),
        "Created file should be type file");
    assertTrue(isFile(fcTarget, new Path(targetTestRoot, "dir2/foo")),
        "Target of created file should be type file");
    
    // Delete the created file
    assertTrue(fcView.delete(new Path("/internalDir/linkToDir2/foo"), false),
        "Delete should succeed");
    assertFalse(exists(fcView, new Path("/internalDir/linkToDir2/foo")),
        "File should not exist after deletion");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "dir2/foo")),
        "Target should not exist after deletion");
    
    
    // Create file with a 3 component dirs
    fileContextTestHelper.createFileNonRecursive(fcView,
        "/internalDir/internalDir2/linkToDir3/foo");
    assertTrue(isFile(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")),
        "Created file should be of type file");
    assertTrue(isFile(fcTarget, new Path(targetTestRoot, "dir3/foo")),
        "Target of created file should also be type file");
    
    // Recursive Create file with missing dirs
    fileContextTestHelper.createFile(fcView,
        "/internalDir/linkToDir2/missingDir/miss2/foo");
    assertTrue(isFile(fcView, new Path("/internalDir/linkToDir2/missingDir/miss2/foo")),
        "Created file should be of type file");
    assertTrue(isFile(fcTarget, new Path(targetTestRoot, "dir2/missingDir/miss2/foo")),
        "Target of created file should also be type file");

    
    // Delete the created file
    assertTrue(fcView.delete(new Path("/internalDir/internalDir2/linkToDir3/foo"), false),
        "Delete should succeed");
    assertFalse(exists(fcView, new Path("/internalDir/internalDir2/linkToDir3/foo")),
        "Deleted File should not exist");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "dir3/foo")),
        "Target of deleted file should not exist");
    
      
    // mkdir
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
        FileContext.DEFAULT_PERM, false);
    assertTrue(isDir(fcView, new Path("/user/dirX")), "New dir should be type dir");
    assertTrue(isDir(fcTarget, new Path(targetTestRoot, "user/dirX")),
        "Target of new dir should be of type dir");
    
    fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"),
        FileContext.DEFAULT_PERM, false);
    assertTrue(isDir(fcView, new Path("/user/dirX/dirY")),
        "New dir should be type dir");
    assertTrue(isDir(fcTarget, new Path(targetTestRoot, "user/dirX/dirY")),
        "Target of new dir should be of type dir");
    

    // Delete the created dir
    assertTrue(fcView.delete(new Path("/user/dirX/dirY"), false),
        "Delete should succeed");
    assertFalse(exists(fcView, new Path("/user/dirX/dirY")),
        "Deleted File should not exist");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "user/dirX/dirY")),
        "Deleted Target should not exist");
    
    assertTrue(fcView.delete(new Path("/user/dirX"), false), "Delete should succeed");
    assertFalse(exists(fcView, new Path("/user/dirX")),
        "Deleted File should not exist");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "user/dirX")),
        "Deleted Target should not exist");
    
    // Rename a file 
    fileContextTestHelper.createFile(fcView, "/user/foo");
    fcView.rename(new Path("/user/foo"), new Path("/user/fooBar"));
    assertFalse(exists(fcView, new Path("/user/foo")), "Renamed src should not exist");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "user/foo")));
    assertTrue(isFile(fcView,
        fileContextTestHelper.getTestRootPath(fcView,"/user/fooBar")));
    assertTrue(isFile(fcTarget, new Path(targetTestRoot, "user/fooBar")));
    
    fcView.mkdir(new Path("/user/dirFoo"), FileContext.DEFAULT_PERM, false);
    fcView.rename(new Path("/user/dirFoo"), new Path("/user/dirFooBar"));
    assertFalse(exists(fcView, new Path("/user/dirFoo")),
        "Renamed src should not exist");
    assertFalse(exists(fcTarget, new Path(targetTestRoot, "user/dirFoo")),
        "Renamed src should not exist in target");
    assertTrue(isDir(fcView, fileContextTestHelper.getTestRootPath(fcView, "/user/dirFooBar")),
        "Renamed dest should  exist as dir");
    assertTrue(isDir(fcTarget, new Path(targetTestRoot, "user/dirFooBar")),
        "Renamed dest should  exist as dir in target");
    
    // Make a directory under a directory that's mounted from the root of another FS
    fcView.mkdir(new Path("/targetRoot/dirFoo"), FileContext.DEFAULT_PERM, false);
    assertTrue(exists(fcView, new Path("/targetRoot/dirFoo")));
    boolean dirFooPresent = false;
    RemoteIterator<FileStatus> dirContents = fcView.listStatus(new Path(
        "/targetRoot/"));
    while (dirContents.hasNext()) {
      FileStatus fileStatus = dirContents.next();
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    assertTrue(dirFooPresent);
    RemoteIterator<LocatedFileStatus> dirLocatedContents =
        fcView.listLocatedStatus(new Path("/targetRoot/"));
    dirFooPresent = false;
    while (dirLocatedContents.hasNext()) {
      FileStatus fileStatus = dirLocatedContents.next();
      if (fileStatus.getPath().getName().equals("dirFoo")) {
        dirFooPresent = true;
      }
    }
    assertTrue(dirFooPresent);
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
    assertEquals(SupportsBlocks ? 10 : 1, viewBL.length);
    BlockLocation[] targetBL = fcTarget.getFileBlockLocations(targetFilePath, 0, 10240+100);
    compareBLs(viewBL, targetBL);
    
    
    // Same test but now get it via the FileStatus Parameter
    fcView.getFileBlockLocations(viewFilePath, 0, 10240+100);
    targetBL = fcTarget.getFileBlockLocations(targetFilePath, 0, 10240+100);
    compareBLs(viewBL, targetBL);  
  }
  
  void compareBLs(BlockLocation[] viewBL, BlockLocation[] targetBL) {
    assertEquals(targetBL.length, viewBL.length);
    int i = 0;
    for (BlockLocation vbl : viewBL) {
      assertThat(vbl.toString()).isEqualTo(targetBL[i].toString());
      assertThat(vbl.getOffset()).isEqualTo(targetBL[i].getOffset());
      assertThat(vbl.getLength()).isEqualTo(targetBL[i].getLength());
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
    assertEquals(7, dirPaths.length);
    fs = fileContextTestHelper.containsPath(fcView, "/user", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileContextTestHelper.containsPath(fcView, "/data", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileContextTestHelper.containsPath(fcView, "/internalDir", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory(), "InternalDirs should appear as dir");
    fs = fileContextTestHelper.containsPath(fcView, "/danglingLink", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
    fs = fileContextTestHelper.containsPath(fcView, "/linkToAFile", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
      
      
      
    // list on internal dir
    dirPaths = fcView.util().listStatus(new Path("/internalDir"));
    assertEquals(2, dirPaths.length);

    fs = fileContextTestHelper.containsPath(fcView, "/internalDir/internalDir2", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isDirectory(), "InternalDirs should appear as dir");
    fs = fileContextTestHelper.containsPath(fcView, "/internalDir/linkToDir2", dirPaths);
    assertNotNull(fs);
    assertTrue(fs.isSymlink(), "A mount should appear as symlink");
  }
      
  @Test
  public void testFileStatusOnMountLink() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      assertTrue(fcView.getFileStatus(new Path("/")).isDirectory(),
          "Slash should appear as dir");
      checkFileStatus(fcView, "/", fileType.isDir);
      checkFileStatus(fcView, "/user", fileType.isDir);
      checkFileStatus(fcView, "/data", fileType.isDir);
      checkFileStatus(fcView, "/internalDir", fileType.isDir);
      checkFileStatus(fcView, "/internalDir/linkToDir2", fileType.isDir);
      checkFileStatus(fcView, "/internalDir/internalDir2/linkToDir3", fileType.isDir);
      checkFileStatus(fcView, "/linkToAFile", fileType.isFile);
      fcView.getFileStatus(new Path("/danglingLink"));
    });
  }
  
  @Test
  public void testGetFileChecksum() throws AccessControlException,
      UnresolvedLinkException, IOException, URISyntaxException {
    AbstractFileSystem mockAFS = mock(AbstractFileSystem.class);
    InodeTree.ResolveResult<AbstractFileSystem> res =
        new InodeTree.ResolveResult<AbstractFileSystem>(null, mockAFS, null,
            new Path("someFile"), true);
    @SuppressWarnings("unchecked")
    InodeTree<AbstractFileSystem> fsState = mock(InodeTree.class);
    when(fsState.resolve(anyString(), anyBoolean())).thenReturn(res);
    ViewFs vfs = new ViewFs(conf);
    vfs.fsState = fsState;

    vfs.getFileChecksum(new Path("/tmp/someFile"));
    verify(mockAFS).getFileChecksum(new Path("someFile"));
  }

  @Test
  public void testgetFSonDanglingLink() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.getFileStatus(new Path("/danglingLink"));
    });
  }
  
  
  @Test
  public void testgetFSonNonExistingInternalDir() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.getFileStatus(new Path("/internalDir/nonExisting"));
    });
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
  
  @Test
  public void testgetFileLinkStatusonNonExistingInternalDir()
    throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.getFileLinkStatus(new Path("/internalDir/nonExisting"));
    });
  }
  
  @Test
  public void testSymlinkTarget() throws IOException {

    // get link target`
    assertEquals(fcView.getLinkTarget(new Path("/user")),
        (new Path(targetTestRoot,"user")));
    assertEquals(fcView.getLinkTarget(new Path("/data")),
        (new Path(targetTestRoot,"data")));
    assertEquals(
        fcView.getLinkTarget(new Path("/internalDir/linkToDir2")),
        (new Path(targetTestRoot,"dir2")));
    assertEquals(
        fcView.getLinkTarget(new Path("/internalDir/internalDir2/linkToDir3")),
        (new Path(targetTestRoot,"dir3")));
    assertEquals(fcView.getLinkTarget(new Path("/linkToAFile")),
        (new Path(targetTestRoot,"aFile")));
  }
  
  @Test
  public void testgetLinkTargetOnNonLink() throws IOException {
    assertThrows(IOException.class, () -> {
      fcView.getLinkTarget(new Path("/internalDir/internalDir2"));
    });
  }
  
  /*
   * Test resolvePath(p) 
   * TODO In the tests below replace 
   * fcView.getDefaultFileSystem().resolvePath() fcView.resolvePath()
   */
  
  @Test
  public void testResolvePathInternalPaths() throws IOException {
    assertEquals(new Path("/"), fcView.resolvePath(new Path("/")));
    assertEquals(new Path("/internalDir"),
                          fcView.resolvePath(new Path("/internalDir")));
  }
  @Test
  public void testResolvePathMountPoints() throws IOException {
    assertEquals(new Path(targetTestRoot, "user"),
        fcView.resolvePath(new Path("/user")));
    assertEquals(new Path(targetTestRoot, "data"),
        fcView.resolvePath(new Path("/data")));
    assertEquals(new Path(targetTestRoot, "dir2"),
        fcView.resolvePath(new Path("/internalDir/linkToDir2")));
    assertEquals(new Path(targetTestRoot, "dir3"),
        fcView.resolvePath(new Path("/internalDir/internalDir2/linkToDir3")));

  }
  
  @Test
  public void testResolvePathThroughMountPoints() throws IOException {
    fileContextTestHelper.createFile(fcView, "/user/foo");
    assertEquals(new Path(targetTestRoot, "user/foo"),
        fcView.resolvePath(new Path("/user/foo")));
    
    fcView.mkdir(
        fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
          FileContext.DEFAULT_PERM, false);
    assertEquals(new Path(targetTestRoot, "user/dirX"),
        fcView.resolvePath(new Path("/user/dirX")));

    
    fcView.mkdir(
        fileContextTestHelper.getTestRootPath(fcView, "/user/dirX/dirY"),
          FileContext.DEFAULT_PERM, false);
    assertEquals(new Path(targetTestRoot, "user/dirX/dirY"),
        fcView.resolvePath(new Path("/user/dirX/dirY")));
  }

  @Test
  public void testResolvePathDanglingLink() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.resolvePath(new Path("/danglingLink"));
    });
  }
  
  @Test
  public void testResolvePathMissingThroughMountPoints() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.resolvePath(new Path("/user/nonExisting"));
    });
  }
  

  @Test
  public void testResolvePathMissingThroughMountPoints2() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/user/dirX"),
          FileContext.DEFAULT_PERM, false);
      fcView.resolvePath(new Path("/user/dirX/nonExisting"));
    });
  }
  
  
  /**
   * Test modify operations (create, mkdir, rename, etc) 
   * on internal dirs of mount table
   * These operations should fail since the mount table is read-only or
   * because the internal dir that it is trying to create already
   * exits.
   */
 
 
  // Mkdir on internal mount table should fail
  @Test
  public void testInternalMkdirSlash() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/"),
          FileContext.DEFAULT_PERM, false);
    });
  }
  
  @Test
  public void testInternalMkdirExisting1() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir"),
          FileContext.DEFAULT_PERM, false);
    });
  }
  @Test
  public void testInternalMkdirExisting2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView,
          "/internalDir/linkToDir2"),
          FileContext.DEFAULT_PERM, false);
    });
  }
  @Test
  public void testInternalMkdirNew() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/dirNew"),
          FileContext.DEFAULT_PERM, false);
    });
  }
  @Test
  public void testInternalMkdirNew2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.mkdir(fileContextTestHelper.getTestRootPath(fcView, "/internalDir/dirNew"),
          FileContext.DEFAULT_PERM, false);
    });
  }
  
  // Create on internal mount table should fail
  
  @Test
  public void testInternalCreate1() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fileContextTestHelper.createFileNonRecursive(fcView, "/foo");
    });
  }
  
  @Test
  public void testInternalCreate2() throws IOException {  // 2 component
    assertThrows(AccessControlException.class, () -> {
      fileContextTestHelper.createFileNonRecursive(fcView, "/internalDir/foo");
    });
  }
  
  @Test
  public void testInternalCreateMissingDir() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fileContextTestHelper.createFile(fcView, "/missingDir/foo");
    });
  }
  
  @Test
  public void testInternalCreateMissingDir2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fileContextTestHelper.createFile(fcView, "/missingDir/miss2/foo");
    });
  }
  
  
  @Test
  public void testInternalCreateMissingDir3() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fileContextTestHelper.createFile(fcView, "/internalDir/miss2/foo");
    });
  }
  
  // Delete on internal mount table should fail
  
  @Test
  public void testInternalDeleteNonExisting() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.delete(new Path("/NonExisting"), false);
    });
  }
  @Test
  public void testInternalDeleteNonExisting2() throws IOException {
    assertThrows(FileNotFoundException.class, () -> {
      fcView.delete(new Path("/internalDir/NonExisting"), false);
    });
  }
  @Test
  public void testInternalDeleteExisting() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.delete(new Path("/internalDir"), false);
    });
  }
  @Test
  public void testInternalDeleteExisting2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      assertTrue(fcView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory(),
          "Delete of link to dir should succeed");
      fcView.delete(new Path("/internalDir/linkToDir2"), false);
    });
  } 
  
  
  // Rename on internal mount table should fail
  
  @Test
  public void testInternalRename1() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.rename(new Path("/internalDir"), new Path("/newDir"));
    });
  }
  @Test
  public void testInternalRename2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      assertTrue(fcView.getFileStatus(new Path("/internalDir/linkToDir2")).isDirectory(),
          "linkTODir2 should be a dir");
      fcView.rename(new Path("/internalDir/linkToDir2"), new Path("/internalDir/dir1"));
    });
  }
  @Test
  public void testInternalRename3() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.rename(new Path("/user"), new Path("/internalDir/linkToDir2"));
    });
  }
  @Test
  public void testInternalRenameToSlash() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.rename(new Path("/internalDir/linkToDir2/foo"), new Path("/"));
    });
  }
  @Test
  public void testInternalRenameFromSlash() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.rename(new Path("/"), new Path("/bar"));
    });
  }
  
  @Test
  public void testInternalSetOwner() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.setOwner(new Path("/internalDir"), "foo", "bar");
    });
  }

  /**
   * Verify the behavior of ACL operations on paths above the root of
   * any mount table entry.
   */

  @Test
  public void testInternalModifyAclEntries() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.modifyAclEntries(new Path("/internalDir"), new ArrayList<AclEntry>());
    });
  }

  @Test
  public void testInternalRemoveAclEntries() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.removeAclEntries(new Path("/internalDir"), new ArrayList<AclEntry>());
    });
  }

  @Test
  public void testInternalRemoveDefaultAcl() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.removeDefaultAcl(new Path("/internalDir"));
    });
  }

  @Test
  public void testInternalRemoveAcl() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.removeAcl(new Path("/internalDir"));
    });
  }

  @Test
  public void testInternalSetAcl() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.setAcl(new Path("/internalDir"), new ArrayList<AclEntry>());
    });
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

  @Test
  public void testInternalSetXAttr() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.setXAttr(new Path("/internalDir"), "xattrName", null);
    });
  }

  @Test
  public void testInternalGetXAttr() throws IOException {
    assertThrows(NotInMountpointException.class, () -> {
      fcView.getXAttr(new Path("/internalDir"), "xattrName");
    });
  }

  @Test
  public void testInternalGetXAttrs() throws IOException {
    assertThrows(NotInMountpointException.class, () -> {
      fcView.getXAttrs(new Path("/internalDir"));
    });
  }

  @Test
  public void testInternalGetXAttrsWithNames() throws IOException {
    assertThrows(NotInMountpointException.class, () -> {
      fcView.getXAttrs(new Path("/internalDir"), new ArrayList<String>());
    });
  }

  @Test
  public void testInternalListXAttr() throws IOException {
    assertThrows(NotInMountpointException.class, () -> {
      fcView.listXAttrs(new Path("/internalDir"));
    });
  }

  @Test
  public void testInternalRemoveXAttr() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.removeXAttr(new Path("/internalDir"), "xattrName");
    });
  }

  @Test
  public void testInternalCreateSnapshot1() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.createSnapshot(new Path("/internalDir"));
    });
  }

  @Test
  public void testInternalCreateSnapshot2() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.createSnapshot(new Path("/internalDir"), "snap1");
    });
  }

  @Test
  public void testInternalRenameSnapshot() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.renameSnapshot(new Path("/internalDir"), "snapOldName",
          "snapNewName");
    });
  }

  @Test
  public void testInternalDeleteSnapshot() throws IOException {
    assertThrows(AccessControlException.class, () -> {
      fcView.deleteSnapshot(new Path("/internalDir"), "snap1");
    });
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

  @Test
  public void testListStatusWithNoGroups() throws Exception {
    final UserGroupInformation userUgi = UserGroupInformation
        .createUserForTesting("user@HADOOP.COM", new String[] {});
    userUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        URI viewFsUri = new URI(
            FsConstants.VIEWFS_SCHEME, MOUNT_TABLE_NAME, "/", null, null);
        FileSystem vfs = FileSystem.get(viewFsUri, conf);
        LambdaTestUtils.intercept(IOException.class,
            "There is no primary group for UGI", () -> vfs
                .listStatus(new Path(viewFsUri.toString() + "internalDir")));
        return null;
      }
    });
  }

}
