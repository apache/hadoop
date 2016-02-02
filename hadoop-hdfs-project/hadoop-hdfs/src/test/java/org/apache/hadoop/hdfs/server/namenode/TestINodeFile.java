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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class TestINodeFile {
  // Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
  static {
    FileSystem.enableSymlinks();
  }
  public static final Log LOG = LogFactory.getLog(TestINodeFile.class);

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private static final PermissionStatus perm = new PermissionStatus(
      "userName", null, FsPermission.getDefault());
  private short replication;
  private long preferredBlockSize = 1024;

  static public INodeFile createINodeFile(long id) {
    return new INodeFile(id, ("file" + id).getBytes(), perm, 0L, 0L, null,
        (short)3, 1024L);
  }

  static void toCompleteFile(INodeFile file) {
    file.toCompleteFile(Time.now(), 0, (short)1);
  }

  INodeFile createINodeFile(short replication, long preferredBlockSize) {
    return new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, replication, preferredBlockSize);
  }

  private static INodeFile createINodeFile(byte storagePolicyID) {
    return new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, (short)3, 1024L, storagePolicyID, false);
  }

  @Test
  public void testStoragePolicyID () {
    for(byte i = 0; i < 16; i++) {
      final INodeFile f = createINodeFile(i);
      assertEquals(i, f.getStoragePolicyID());
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testStoragePolicyIdBelowLowerBound () throws IllegalArgumentException {
    createINodeFile((byte)-1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testStoragePolicyIdAboveUpperBound () throws IllegalArgumentException {
    createINodeFile((byte)16);
  }

  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getFileReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testReplicationBelowLowerBound ()
              throws IllegalArgumentException {
    replication = -1;
    preferredBlockSize = 128*1024*1024;
    createINodeFile(replication, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
   assertEquals("True has to be returned in this case", preferredBlockSize,
        inf.getPreferredBlockSize());
 }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
                 inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = -1;
    createINodeFile(replication, preferredBlockSize);
  } 

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE+1;
    createINodeFile(replication, preferredBlockSize);
 }

  @Test
  public void testGetFullPathName() {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    inf.setLocalName(DFSUtil.string2Bytes("f"));

    INodeDirectory root = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID,
        INodeDirectory.ROOT_NAME, perm, 0L);
    INodeDirectory dir = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID,
        DFSUtil.string2Bytes("d"), perm, 0L);

    assertEquals("f", inf.getFullPathName());

    dir.addChild(inf);
    assertEquals("d"+Path.SEPARATOR+"f", inf.getFullPathName());
    
    root.addChild(dir);
    assertEquals(Path.SEPARATOR+"d"+Path.SEPARATOR+"f", inf.getFullPathName());
    assertEquals(Path.SEPARATOR+"d", dir.getFullPathName());

    assertEquals(Path.SEPARATOR, root.getFullPathName());
  }
  
  /**
   * FSDirectory#unprotectedSetQuota creates a new INodeDirectoryWithQuota to
   * replace the original INodeDirectory. Before HDFS-4243, the parent field of
   * all the children INodes of the target INodeDirectory is not changed to
   * point to the new INodeDirectoryWithQuota. This testcase tests this
   * scenario.
   */
  @Test
  public void testGetFullPathNameAfterSetQuota() throws Exception {
    long fileLen = 1024;
    replication = 3;
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(replication).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      FSDirectory fsdir = fsn.getFSDirectory();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // Create a file for test
      final Path dir = new Path("/dir");
      final Path file = new Path(dir, "file");
      DFSTestUtil.createFile(dfs, file, fileLen, replication, 0L);

      // Check the full path name of the INode associating with the file
      INode fnode = fsdir.getINode(file.toString());
      assertEquals(file.toString(), fnode.getFullPathName());
      
      // Call FSDirectory#unprotectedSetQuota which calls
      // INodeDirectory#replaceChild
      dfs.setQuota(dir, Long.MAX_VALUE - 1, replication * fileLen * 10);
      INodeDirectory dirNode = getDir(fsdir, dir);
      assertEquals(dir.toString(), dirNode.getFullPathName());
      assertTrue(dirNode.isWithQuota());
      
      final Path newDir = new Path("/newdir");
      final Path newFile = new Path(newDir, "file");
      // Also rename dir
      dfs.rename(dir, newDir, Options.Rename.OVERWRITE);
      // /dir/file now should be renamed to /newdir/file
      fnode = fsdir.getINode(newFile.toString());
      // getFullPathName can return correct result only if the parent field of
      // child node is set correctly
      assertEquals(newFile.toString(), fnode.getFullPathName());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testConcatBlocks() {
    INodeFile origFile = createINodeFiles(1, "origfile")[0];
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 1L);

    INodeFile[] appendFiles = createINodeFiles(4, "appendfile");
    BlockManager bm = Mockito.mock(BlockManager.class);
    origFile.concatBlocks(appendFiles, bm);
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 5L);
  }
  
  /** 
   * Creates the required number of files with one block each
   * @param nCount Number of INodes to create
   * @return Array of INode files
   */
  private INodeFile[] createINodeFiles(int nCount, String fileNamePrefix) {
    if(nCount <= 0)
      return new INodeFile[1];

    replication = 3;
    preferredBlockSize = 128 * 1024 * 1024;
    INodeFile[] iNodes = new INodeFile[nCount];
    for (int i = 0; i < nCount; i++) {
      iNodes[i] = new INodeFile(i, null, perm, 0L, 0L, null, replication,
          preferredBlockSize);
      iNodes[i].setLocalName(DFSUtil.string2Bytes(fileNamePrefix + i));
      BlockInfo newblock = new BlockInfoContiguous(replication);
      iNodes[i].addBlock(newblock);
    }
    
    return iNodes;
  }

  /**
   * Test for the static {@link INodeFile#valueOf(INode, String)}
   * and {@link INodeFileUnderConstruction#valueOf(INode, String)} methods.
   * @throws IOException 
   */
  @Test
  public void testValueOf () throws IOException {
    final String path = "/testValueOf";
    final short replication = 3;

    {//cast from null
      final INode from = null;

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Directory does not exist"));
      }
    }

    {//cast from INodeFile
      final INode from = createINodeFile(replication, preferredBlockSize);

     //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(PathIsNotDirectoryException e) {
        // Expected
      }
    }

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFile(
          HdfsConstants.GRANDFATHER_INODE_ID, null, perm, 0L, 0L, null, replication,
          1024L);
      from.asFile().toUnderConstruction("client", "machine");
    
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(PathIsNotDirectoryException expected) {
        // expected
      }
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(HdfsConstants.GRANDFATHER_INODE_ID, null,
          perm, 0L);

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeDirectory, should success
      final INodeDirectory d = INodeDirectory.valueOf(from, path);
      assertTrue(d == from);
    }
  }

  /**
   * This test verifies inode ID counter and inode map functionality.
   */
  @Test
  public void testInodeId() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNamesystem();
      long lastId = fsn.dir.getLastInodeId();

      // Ensure root has the correct inode ID
      // Last inode ID should be root inode ID and inode map size should be 1
      int inodeCount = 1;
      long expectedLastInodeId = INodeId.ROOT_INODE_ID;
      assertEquals(fsn.dir.rootDir.getId(), INodeId.ROOT_INODE_ID);
      assertEquals(expectedLastInodeId, lastId);
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // Create a directory
      // Last inode ID and inode map size should increase by 1
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test1");
      assertTrue(fs.mkdirs(path));
      assertEquals(++expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(++inodeCount, fsn.dir.getInodeMapSize());

      // Create a file
      // Last inode ID and inode map size should increase by 1
      NamenodeProtocols nnrpc = cluster.getNameNodeRpc();
      DFSTestUtil.createFile(fs, new Path("/test1/file"), 1024, (short) 1, 0);
      assertEquals(++expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(++inodeCount, fsn.dir.getInodeMapSize());
      
      // Ensure right inode ID is returned in file status
      HdfsFileStatus fileStatus = nnrpc.getFileInfo("/test1/file");
      assertEquals(expectedLastInodeId, fileStatus.getFileId());

      // Rename a directory
      // Last inode ID and inode map size should not change
      Path renamedPath = new Path("/test2");
      assertTrue(fs.rename(path, renamedPath));
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      
      // Delete test2/file and test2 and ensure inode map size decreases
      assertTrue(fs.delete(renamedPath, true));
      inodeCount -= 2;
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      
      // Create and concat /test/file1 /test/file2
      // Create /test1/file1 and /test1/file2
      String file1 = "/test1/file1";
      String file2 = "/test1/file2";
      DFSTestUtil.createFile(fs, new Path(file1), 512, (short) 1, 0);
      DFSTestUtil.createFile(fs, new Path(file2), 512, (short) 1, 0);
      inodeCount += 3; // test1, file1 and file2 are created
      expectedLastInodeId += 3;
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      // Concat the /test1/file1 /test1/file2 into /test1/file2
      nnrpc.concat(file2, new String[] {file1});
      inodeCount--; // file1 and file2 are concatenated to file2
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertTrue(fs.delete(new Path("/test1"), true));
      inodeCount -= 2; // test1 and file2 is deleted
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // Make sure editlog is loaded correctly 
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNamesystem();
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // Create two inodes test2 and test2/file2
      DFSTestUtil.createFile(fs, new Path("/test2/file2"), 1024, (short) 1, 0);
      expectedLastInodeId += 2;
      inodeCount += 2;
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // create /test3, and /test3/file.
      // /test3/file is a file under construction
      FSDataOutputStream outStream = fs.create(new Path("/test3/file"));
      assertTrue(outStream != null);
      expectedLastInodeId += 2;
      inodeCount += 2;
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // Apply editlogs to fsimage, ensure inodeUnderConstruction is handled
      fsn.enterSafeMode(false);
      fsn.saveNamespace(0, 0);
      fsn.leaveSafeMode(false);

      outStream.close();

      // The lastInodeId in fsimage should remain the same after reboot
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNamesystem();
      assertEquals(expectedLastInodeId, fsn.dir.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testWriteToDeletedFile() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    Path path = new Path("/test1");
    assertTrue(fs.mkdirs(path));

    int size = conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    byte[] data = new byte[size];

    // Create one file
    Path filePath = new Path("/test1/file");
    FSDataOutputStream fos = fs.create(filePath);

    // Delete the file
    fs.delete(filePath, false);

    // Add new block should fail since /test1/file has been deleted.
    try {
      fos.write(data, 0, data.length);
      // make sure addBlock() request gets to NN immediately
      fos.hflush();

      fail("Write should fail after delete");
    } catch (Exception e) {
      /* Ignore */
    } finally {
      cluster.shutdown();
    }
  }
  
  private Path getInodePath(long inodeId, String remainingPath) {
    StringBuilder b = new StringBuilder();
    b.append(Path.SEPARATOR).append(FSDirectory.DOT_RESERVED_STRING)
        .append(Path.SEPARATOR).append(FSDirectory.DOT_INODES_STRING)
        .append(Path.SEPARATOR).append(inodeId).append(Path.SEPARATOR)
        .append(remainingPath);
    Path p = new Path(b.toString());
    LOG.info("Inode path is " + p);
    return p;
  }
  
  /**
   * Tests for addressing files using /.reserved/.inodes/<inodeID> in file system
   * operations.
   */
  @Test
  public void testInodeIdBasedPaths() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      NamenodeProtocols nnRpc = cluster.getNameNodeRpc();
      
      // FileSystem#mkdirs "/testInodeIdBasedPaths"
      Path baseDir = getInodePath(INodeId.ROOT_INODE_ID, "testInodeIdBasedPaths");
      Path baseDirRegPath = new Path("/testInodeIdBasedPaths");
      fs.mkdirs(baseDir);
      fs.exists(baseDir);
      long baseDirFileId = nnRpc.getFileInfo(baseDir.toString()).getFileId();
      
      // FileSystem#create file and FileSystem#close
      Path testFileInodePath = getInodePath(baseDirFileId, "test1");
      Path testFileRegularPath = new Path(baseDir, "test1");
      final int testFileBlockSize = 1024;
      FileSystemTestHelper.createFile(fs, testFileInodePath, 1, testFileBlockSize);
      assertTrue(fs.exists(testFileInodePath));
      
      // FileSystem#setPermission
      FsPermission perm = new FsPermission((short)0666);
      fs.setPermission(testFileInodePath, perm);
      
      // FileSystem#getFileStatus and FileSystem#getPermission
      FileStatus fileStatus = fs.getFileStatus(testFileInodePath);
      assertEquals(perm, fileStatus.getPermission());
      
      // FileSystem#setOwner
      fs.setOwner(testFileInodePath, fileStatus.getOwner(), fileStatus.getGroup());
      
      // FileSystem#setTimes
      fs.setTimes(testFileInodePath, 0, 0);
      fileStatus = fs.getFileStatus(testFileInodePath);
      assertEquals(0, fileStatus.getModificationTime());
      assertEquals(0, fileStatus.getAccessTime());
      
      // FileSystem#setReplication
      fs.setReplication(testFileInodePath, (short)3);
      fileStatus = fs.getFileStatus(testFileInodePath);
      assertEquals(3, fileStatus.getReplication());
      fs.setReplication(testFileInodePath, (short)1);
      
      // ClientProtocol#getPreferredBlockSize
      assertEquals(testFileBlockSize,
          nnRpc.getPreferredBlockSize(testFileInodePath.toString()));

      /*
       * HDFS-6749 added missing calls to FSDirectory.resolvePath in the
       * following four methods. The calls below ensure that
       * /.reserved/.inodes paths work properly. No need to check return
       * values as these methods are tested elsewhere.
       */
      {
        fs.isFileClosed(testFileInodePath);
        fs.getAclStatus(testFileInodePath);
        fs.getXAttrs(testFileInodePath);
        fs.listXAttrs(testFileInodePath);
        fs.access(testFileInodePath, FsAction.READ_WRITE);
      }
      
      // symbolic link related tests
      
      // Reserved path is not allowed as a target
      String invalidTarget = new Path(baseDir, "invalidTarget").toString();
      String link = new Path(baseDir, "link").toString();
      testInvalidSymlinkTarget(nnRpc, invalidTarget, link);
      
      // Test creating a link using reserved inode path
      String validTarget = "/validtarget";
      testValidSymlinkTarget(nnRpc, validTarget, link);
      
      // FileSystem#append
      fs.append(testFileInodePath);
      // DistributedFileSystem#recoverLease
      
      fs.recoverLease(testFileInodePath);
      
      // Namenode#getBlockLocations
      LocatedBlocks l1 = nnRpc.getBlockLocations(testFileInodePath.toString(),
          0, Long.MAX_VALUE);
      LocatedBlocks l2 = nnRpc.getBlockLocations(testFileRegularPath.toString(),
          0, Long.MAX_VALUE);
      checkEquals(l1, l2);
      
      // FileSystem#rename - both the variants
      Path renameDst = getInodePath(baseDirFileId, "test2");
      fileStatus = fs.getFileStatus(testFileInodePath);
      // Rename variant 1: rename and rename bacck
      fs.rename(testFileInodePath, renameDst);
      fs.rename(renameDst, testFileInodePath);
      assertEquals(fileStatus, fs.getFileStatus(testFileInodePath));
      
      // Rename variant 2: rename and rename bacck
      fs.rename(testFileInodePath, renameDst, Rename.OVERWRITE);
      fs.rename(renameDst, testFileInodePath, Rename.OVERWRITE);
      assertEquals(fileStatus, fs.getFileStatus(testFileInodePath));
      
      // FileSystem#getContentSummary
      assertEquals(fs.getContentSummary(testFileRegularPath).toString(),
          fs.getContentSummary(testFileInodePath).toString());
      
      // FileSystem#listFiles
      checkEquals(fs.listFiles(baseDirRegPath, false),
          fs.listFiles(baseDir, false));
      
      // FileSystem#delete
      fs.delete(testFileInodePath, true);
      assertFalse(fs.exists(testFileInodePath));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void testInvalidSymlinkTarget(NamenodeProtocols nnRpc,
      String invalidTarget, String link) throws IOException {
    try {
      FsPermission perm = FsPermission.createImmutable((short)0755);
      nnRpc.createSymlink(invalidTarget, link, perm, false);
      fail("Symbolic link creation of target " + invalidTarget + " should fail");
    } catch (InvalidPathException expected) {
      // Expected
    }
  }

  private void testValidSymlinkTarget(NamenodeProtocols nnRpc, String target,
      String link) throws IOException {
    FsPermission perm = FsPermission.createImmutable((short)0755);
    nnRpc.createSymlink(target, link, perm, false);
    assertEquals(target, nnRpc.getLinkTarget(link));
  }
  
  private static void checkEquals(LocatedBlocks l1, LocatedBlocks l2) {
    List<LocatedBlock> list1 = l1.getLocatedBlocks();
    List<LocatedBlock> list2 = l2.getLocatedBlocks();
    assertEquals(list1.size(), list2.size());
    
    for (int i = 0; i < list1.size(); i++) {
      LocatedBlock b1 = list1.get(i);
      LocatedBlock b2 = list2.get(i);
      assertEquals(b1.getBlock(), b2.getBlock());
      assertEquals(b1.getBlockSize(), b2.getBlockSize());
    }
  }

  private static void checkEquals(RemoteIterator<LocatedFileStatus> i1,
      RemoteIterator<LocatedFileStatus> i2) throws IOException {
    while (i1.hasNext()) {
      assertTrue(i2.hasNext());
      
      // Compare all the fields but the path name, which is relative
      // to the original path from listFiles.
      LocatedFileStatus l1 = i1.next();
      LocatedFileStatus l2 = i2.next();
      assertEquals(l1.getAccessTime(), l2.getAccessTime());
      assertEquals(l1.getBlockSize(), l2.getBlockSize());
      assertEquals(l1.getGroup(), l2.getGroup());
      assertEquals(l1.getLen(), l2.getLen());
      assertEquals(l1.getModificationTime(), l2.getModificationTime());
      assertEquals(l1.getOwner(), l2.getOwner());
      assertEquals(l1.getPermission(), l2.getPermission());
      assertEquals(l1.getReplication(), l2.getReplication());
    }
    assertFalse(i2.hasNext());
  }
  
  /**
   * Check /.reserved path is reserved and cannot be created.
   */
  @Test
  public void testReservedFileNames() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      // First start a cluster with reserved file names check turned off
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      
      // Creation of directory or file with reserved path names is disallowed
      ensureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
      ensureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
      Path reservedPath = new Path("/.reserved");
      
      // Loading of fsimage or editlog with /.reserved directory should fail
      // Mkdir "/.reserved reserved path with reserved path check turned off
      FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
      fs.mkdirs(reservedPath);
      assertTrue(fs.isDirectory(reservedPath));
      ensureReservedFileNamesCannotBeLoaded(cluster);

      // Loading of fsimage or editlog with /.reserved file should fail
      // Create file "/.reserved reserved path with reserved path check turned off
      FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
      ensureClusterRestartSucceeds(cluster);
      fs.delete(reservedPath, true);
      DFSTestUtil.createFile(fs, reservedPath, 10, (short)1, 0L);
      assertTrue(!fs.isDirectory(reservedPath));
      ensureReservedFileNamesCannotBeLoaded(cluster);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void ensureReservedFileNamesCannotBeCreated(FileSystem fs, String name,
      boolean isDir) {
    // Creation of directory or file with reserved path names is disallowed
    Path reservedPath = new Path(name);
    try {
      if (isDir) {
        fs.mkdirs(reservedPath);
      } else {
        DFSTestUtil.createFile(fs, reservedPath, 10, (short) 1, 0L);
      }
      fail((isDir ? "mkdir" : "create file") + " should be disallowed");
    } catch (Exception expected) {
      // ignored
    }
  }
  
  private void ensureReservedFileNamesCannotBeLoaded(MiniDFSCluster cluster)
      throws IOException {
    // Turn on reserved file name checking. Loading of edits should fail
    FSDirectory.CHECK_RESERVED_FILE_NAMES = true;
    ensureClusterRestartFails(cluster);

    // Turn off reserved file name checking and successfully load edits
    FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
    ensureClusterRestartSucceeds(cluster);

    // Turn on reserved file name checking. Loading of fsimage should fail
    FSDirectory.CHECK_RESERVED_FILE_NAMES = true;
    ensureClusterRestartFails(cluster);
  }
  
  private void ensureClusterRestartFails(MiniDFSCluster cluster) {
    try {
      cluster.restartNameNode();
      fail("Cluster should not have successfully started");
    } catch (Exception expected) {
      LOG.info("Expected exception thrown " + expected);
    }
    assertFalse(cluster.isClusterUp());
  }
  
  private void ensureClusterRestartSucceeds(MiniDFSCluster cluster)
      throws IOException {
    cluster.restartNameNode();
    cluster.waitActive();
    assertTrue(cluster.isClusterUp());
  }
  
  /**
   * For a given path, build a tree of INodes and return the leaf node.
   */
  private INode createTreeOfInodes(String path) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(path);
    FsPermission perm = FsPermission.createImmutable((short)0755);
    PermissionStatus permstatus = PermissionStatus.createImmutable("", "", perm);
    
    long id = 0;
    INodeDirectory prev = new INodeDirectory(++id, new byte[0], permstatus, 0);
    INodeDirectory dir = null;
    for (byte[] component : components) {
      if (component.length == 0) {
        continue;
      }
      System.out.println("Adding component " + DFSUtil.bytes2String(component));
      dir = new INodeDirectory(++id, component, permstatus, 0);
      prev.addChild(dir, false, Snapshot.CURRENT_STATE_ID);
      prev = dir;
    }
    return dir; // Last Inode in the chain
  }
  
  /**
   * Test for {@link FSDirectory#getPathComponents(INode)}
   */
  @Test
  public void testGetPathFromInode() throws QuotaExceededException {
    String path = "/a/b/c";
    INode inode = createTreeOfInodes(path);
    byte[][] expected = INode.getPathComponents(path);
    byte[][] actual = FSDirectory.getPathComponents(inode);
    DFSTestUtil.checkComponentsEquals(expected, actual);
  }
  
  /**
   * Tests for {@link FSDirectory#resolvePath(String, byte[][], FSDirectory)}
   */
  @Test
  public void testInodePath() throws IOException {
    // For a non .inodes path the regular components are returned
    String path = "/a/b/c";
    INode inode = createTreeOfInodes(path);
    // For an any inode look up return inode corresponding to "c" from /a/b/c
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    Mockito.doReturn(inode).when(fsd).getInode(Mockito.anyLong());
    
    // Null components
    assertEquals("/test", FSDirectory.resolvePath("/test", null, fsd));
    
    // Tests for FSDirectory#resolvePath()
    // Non inode regular path
    byte[][] components = INode.getPathComponents(path);
    String resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with no trailing separator
    components = INode.getPathComponents("/.reserved/.inodes/1");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with trailing separator
    components = INode.getPathComponents("/.reserved/.inodes/1/");
    assertEquals(path, resolvedPath);
    
    // Inode relative path
    components = INode.getPathComponents("/.reserved/.inodes/1/d/e/f");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals("/a/b/c/d/e/f", resolvedPath);
    
    // A path with just .inodes  returns the path as is
    String testPath = "/.reserved/.inodes";
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals(testPath, resolvedPath);
    
    // Root inode path
    testPath = "/.reserved/.inodes/" + INodeId.ROOT_INODE_ID;
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals("/", resolvedPath);
    
    // An invalid inode path should remain unresolved
    testPath = "/.invalid/.inodes/1";
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals(testPath, resolvedPath);
    
    // Test path with nonexistent(deleted or wrong id) inode
    Mockito.doReturn(null).when(fsd).getInode(Mockito.anyLong());
    testPath = "/.reserved/.inodes/1234";
    components = INode.getPathComponents(testPath);
    try {
      String realPath = FSDirectory.resolvePath(testPath, components, fsd);
      fail("Path should not be resolved:" + realPath);
    } catch (IOException e) {
      assertTrue(e instanceof FileNotFoundException);
    }
  }
  
  private static INodeDirectory getDir(final FSDirectory fsdir, final Path dir)
      throws IOException {
    final String dirStr = dir.toString();
    return INodeDirectory.valueOf(fsdir.getINode(dirStr), dirStr);
  }

  /**
   * Test whether the inode in inodeMap has been replaced after regular inode
   * replacement
   */
  @Test
  public void testInodeReplacement() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final DistributedFileSystem hdfs = cluster.getFileSystem();
      final FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();

      final Path dir = new Path("/dir");
      hdfs.mkdirs(dir);
      INodeDirectory dirNode = getDir(fsdir, dir);
      INode dirNodeFromNode = fsdir.getInode(dirNode.getId());
      assertSame(dirNode, dirNodeFromNode);

      // set quota to dir, which leads to node replacement
      hdfs.setQuota(dir, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
      dirNode = getDir(fsdir, dir);
      assertTrue(dirNode.isWithQuota());
      // the inode in inodeMap should also be replaced
      dirNodeFromNode = fsdir.getInode(dirNode.getId());
      assertSame(dirNode, dirNodeFromNode);

      hdfs.setQuota(dir, -1, -1);
      dirNode = getDir(fsdir, dir);
      // the inode in inodeMap should also be replaced
      dirNodeFromNode = fsdir.getInode(dirNode.getId());
      assertSame(dirNode, dirNodeFromNode);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testDotdotInodePath() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final DistributedFileSystem hdfs = cluster.getFileSystem();
      final FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();

      final Path dir = new Path("/dir");
      hdfs.mkdirs(dir);
      long dirId = fsdir.getINode(dir.toString()).getId();
      long parentId = fsdir.getINode("/").getId();
      String testPath = "/.reserved/.inodes/" + dirId + "/..";

      client = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
      HdfsFileStatus status = client.getFileInfo(testPath);
      assertTrue(parentId == status.getFileId());
      
      // Test root's parent is still root
      testPath = "/.reserved/.inodes/" + parentId + "/..";
      status = client.getFileInfo(testPath);
      assertTrue(parentId == status.getFileId());
      
    } finally {
      IOUtils.cleanup(LOG, client);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  @Test
  public void testLocationLimitInListingOps() throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 9); // 3 blocks * 3 replicas
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      final DistributedFileSystem hdfs = cluster.getFileSystem();
      ArrayList<String> source = new ArrayList<String>();

      // tmp1 holds files with 3 blocks, 3 replicas
      // tmp2 holds files with 3 blocks, 1 replica
      hdfs.mkdirs(new Path("/tmp1"));
      hdfs.mkdirs(new Path("/tmp2"));

      source.add("f1");
      source.add("f2");

      int numEntries = source.size();
      for (int j=0;j<numEntries;j++) {
          DFSTestUtil.createFile(hdfs, new Path("/tmp1/"+source.get(j)), 4096,
          3*1024-100, 1024, (short) 3, 0);
      }

      byte[] start = HdfsFileStatus.EMPTY_NAME;
      for (int j=0;j<numEntries;j++) {
          DirectoryListing dl = cluster.getNameNodeRpc().getListing("/tmp1",
              start, true);
          assertTrue(dl.getPartialListing().length == 1);
          for (int i=0;i<dl.getPartialListing().length; i++) {
              source.remove(dl.getPartialListing()[i].getLocalName());
          }
          start = dl.getLastName();
      }
      // Verify we have listed all entries in the directory.
      assertTrue(source.size() == 0);

      // Now create 6 files, each with 3 locations. Should take 2 iterations of 3
      source.add("f1");
      source.add("f2");
      source.add("f3");
      source.add("f4");
      source.add("f5");
      source.add("f6");
      numEntries = source.size();
      for (int j=0;j<numEntries;j++) {
          DFSTestUtil.createFile(hdfs, new Path("/tmp2/"+source.get(j)), 4096,
          3*1024-100, 1024, (short) 1, 0);
      }

      start = HdfsFileStatus.EMPTY_NAME;
      for (int j=0;j<numEntries/3;j++) {
          DirectoryListing dl = cluster.getNameNodeRpc().getListing("/tmp2",
              start, true);
          assertTrue(dl.getPartialListing().length == 3);
          for (int i=0;i<dl.getPartialListing().length; i++) {
              source.remove(dl.getPartialListing()[i].getLocalName());
          }
          start = dl.getLastName();
      }
      // Verify we have listed all entries in tmp2.
      assertTrue(source.size() == 0);
  } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFilesInGetListingOps() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final DistributedFileSystem hdfs = cluster.getFileSystem();
      final FSDirectory fsdir = cluster.getNamesystem().getFSDirectory();

      hdfs.mkdirs(new Path("/tmp"));
      DFSTestUtil.createFile(hdfs, new Path("/tmp/f1"), 0, (short) 1, 0);
      DFSTestUtil.createFile(hdfs, new Path("/tmp/f2"), 0, (short) 1, 0);
      DFSTestUtil.createFile(hdfs, new Path("/tmp/f3"), 0, (short) 1, 0);

      DirectoryListing dl = cluster.getNameNodeRpc().getListing("/tmp",
          HdfsFileStatus.EMPTY_NAME, false);
      assertTrue(dl.getPartialListing().length == 3);

      String f2 = new String("f2");
      dl = cluster.getNameNodeRpc().getListing("/tmp", f2.getBytes(), false);
      assertTrue(dl.getPartialListing().length == 1);

      INode f2INode = fsdir.getINode("/tmp/f2");
      String f2InodePath = "/.reserved/.inodes/" + f2INode.getId();
      dl = cluster.getNameNodeRpc().getListing("/tmp", f2InodePath.getBytes(),
          false);
      assertTrue(dl.getPartialListing().length == 1);

      // Test the deleted startAfter file
      hdfs.delete(new Path("/tmp/f2"), false);
      try {
        dl = cluster.getNameNodeRpc().getListing("/tmp",
            f2InodePath.getBytes(), false);
        fail("Didn't get exception for the deleted startAfter token.");
      } catch (IOException e) {
        assertTrue(e instanceof DirectoryListingStartAfterNotFoundException);
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFileUnderConstruction() {
    replication = 3;
    final INodeFile file = new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID, null,
        perm, 0L, 0L, null, replication, 1024L);
    assertFalse(file.isUnderConstruction());

    final String clientName = "client";
    final String clientMachine = "machine";
    file.toUnderConstruction(clientName, clientMachine);
    assertTrue(file.isUnderConstruction());
    FileUnderConstructionFeature uc = file.getFileUnderConstructionFeature();
    assertEquals(clientName, uc.getClientName());
    assertEquals(clientMachine, uc.getClientMachine());

    toCompleteFile(file);
    assertFalse(file.isUnderConstruction());
  }

  @Test
  public void testXAttrFeature() {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    ImmutableList.Builder<XAttr> builder = new ImmutableList.Builder<XAttr>();
    XAttr xAttr = new XAttr.Builder().setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(new byte[]{0x31, 0x32, 0x33}).build();
    builder.add(xAttr);
    XAttrFeature f = new XAttrFeature(builder.build());
    inf.addXAttrFeature(f);
    XAttrFeature f1 = inf.getXAttrFeature();
    assertEquals(xAttr, f1.getXAttrs().get(0));
    inf.removeXAttrFeature();
    f1 = inf.getXAttrFeature();
    assertEquals(f1, null);
  }

  @Test
  public void testClearBlocks() {
    INodeFile toBeCleared = createINodeFiles(1, "toBeCleared")[0];
    assertEquals(1, toBeCleared.getBlocks().length);
    toBeCleared.clearBlocks();
    assertTrue(toBeCleared.getBlocks().length == 0);
  }
}
