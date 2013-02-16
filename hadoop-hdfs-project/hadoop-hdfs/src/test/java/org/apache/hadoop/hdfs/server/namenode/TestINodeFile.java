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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.junit.Test;

public class TestINodeFile {

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private String userName = "Test";
  private short replication;
  private long preferredBlockSize;

  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(INodeId.GRANDFATHER_INODE_ID,
        new PermissionStatus(userName, null, FsPermission.getDefault()), null,
        replication, 0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getBlockReplication());
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
    new INodeFile(INodeId.GRANDFATHER_INODE_ID, new PermissionStatus(userName,
        null, FsPermission.getDefault()), null, replication, 0L, 0L,
        preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(INodeId.GRANDFATHER_INODE_ID,
        new PermissionStatus(userName, null, FsPermission.getDefault()), null,
        replication, 0L, 0L, preferredBlockSize);
   assertEquals("True has to be returned in this case", preferredBlockSize,
        inf.getPreferredBlockSize());
 }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = new INodeFile(INodeId.GRANDFATHER_INODE_ID,
        new PermissionStatus(userName, null, FsPermission.getDefault()), null,
        replication, 0L, 0L, preferredBlockSize);
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
    new INodeFile(INodeId.GRANDFATHER_INODE_ID, new PermissionStatus(userName,
        null, FsPermission.getDefault()), null, replication, 0L, 0L,
        preferredBlockSize);
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
    new INodeFile(INodeId.GRANDFATHER_INODE_ID, new PermissionStatus(userName,
        null, FsPermission.getDefault()), null, replication, 0L, 0L,
        preferredBlockSize);
 }

  @Test
  public void testGetFullPathName() {
    PermissionStatus perms = new PermissionStatus(
      userName, null, FsPermission.getDefault());

    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(INodeId.GRANDFATHER_INODE_ID, perms, null,
        replication, 0L, 0L, preferredBlockSize);
    inf.setLocalName("f");

    INodeDirectory root = new INodeDirectory(INodeId.GRANDFATHER_INODE_ID,
        INodeDirectory.ROOT_NAME, perms);
    INodeDirectory dir = new INodeDirectory(INodeId.GRANDFATHER_INODE_ID, "d",
        perms);

    assertEquals("f", inf.getFullPathName());
    assertEquals("", inf.getLocalParentDir());

    dir.addChild(inf, false);
    assertEquals("d"+Path.SEPARATOR+"f", inf.getFullPathName());
    assertEquals("d", inf.getLocalParentDir());
    
    root.addChild(dir, false);
    assertEquals(Path.SEPARATOR+"d"+Path.SEPARATOR+"f", inf.getFullPathName());
    assertEquals(Path.SEPARATOR+"d", dir.getFullPathName());

    assertEquals(Path.SEPARATOR, root.getFullPathName());
    assertEquals(Path.SEPARATOR, root.getLocalParentDir());
    
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
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        replication).build();
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
    final Path newDir = new Path("/newdir");
    final Path newFile = new Path(newDir, "file");
    // Also rename dir
    dfs.rename(dir, newDir, Options.Rename.OVERWRITE);
    // /dir/file now should be renamed to /newdir/file
    fnode = fsdir.getINode(newFile.toString());
    // getFullPathName can return correct result only if the parent field of
    // child node is set correctly
    assertEquals(newFile.toString(), fnode.getFullPathName());
  }
  
  @Test
  public void testAppendBlocks() {
    INodeFile origFile = createINodeFiles(1, "origfile")[0];
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 1L);

    INodeFile[] appendFiles =   createINodeFiles(4, "appendfile");
    origFile.appendBlocks(appendFiles, getTotalBlocks(appendFiles));
    assertEquals("Number of blocks didn't match", origFile.numBlocks(), 5L);
  }

  /** 
   * Gives the count of blocks for a given number of files
   * @param files Array of INode files
   * @return total count of blocks
   */
  private int getTotalBlocks(INodeFile[] files) {
    int nBlocks=0;
    for(int i=0; i < files.length; i++) {
       nBlocks += files[i].numBlocks();
    }
    return nBlocks;
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
      PermissionStatus perms = new PermissionStatus(userName, null,
          FsPermission.getDefault());
      iNodes[i] = new INodeFile(i, perms, null, replication, 0L, 0L,
          preferredBlockSize);
      iNodes[i].setLocalName(fileNamePrefix +  Integer.toString(i));
      BlockInfo newblock = new BlockInfo(replication);
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
    final PermissionStatus perm = new PermissionStatus(
        userName, null, FsPermission.getDefault());
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

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
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
      final INode from = new INodeFile(INodeId.GRANDFATHER_INODE_ID, perm,
          null, replication, 0L, 0L, preferredBlockSize);

     //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(IOException ioe) {
        assertTrue(ioe.getMessage().contains("File is not under construction"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(PathIsNotDirectoryException e) {
      }
    }

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFileUnderConstruction(
          INodeId.GRANDFATHER_INODE_ID, perm, replication, 0L, 0L, "client",
          "machine", null);
    
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should success
      final INodeFileUnderConstruction u = INodeFileUnderConstruction.valueOf(
          from, path);
      assertTrue(u == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(PathIsNotDirectoryException e) {
      }
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(INodeId.GRANDFATHER_INODE_ID, perm,
          0L);

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
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
   * Verify root always has inode id 1001 and new formated fsimage has last
   * allocated inode id 1000. Validate correct lastInodeId is persisted.
   * @throws IOException
   */
  @Test
  public void testInodeId() throws IOException {

    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    cluster.waitActive();
    
    FSNamesystem fsn = cluster.getNamesystem();
    long lastId = fsn.getLastInodeId();

    assertTrue(lastId == 1001);

    // Create one directory and the last inode id should increase to 1002
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("/test1");
    assertTrue(fs.mkdirs(path));
    assertTrue(fsn.getLastInodeId() == 1002);

    // Use namenode rpc to create a file
    NamenodeProtocols nnrpc = cluster.getNameNodeRpc();
    HdfsFileStatus fileStatus = nnrpc.create("/test1/file", new FsPermission(
        (short) 0755), "client",
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true,
        (short) 1, 128 * 1024 * 1024L);
    assertTrue(fsn.getLastInodeId() == 1003);
    assertTrue(fileStatus.getFileId() == 1003);

    // Rename doesn't increase inode id
    Path renamedPath = new Path("/test2");
    fs.rename(path, renamedPath);
    assertTrue(fsn.getLastInodeId() == 1003);

    cluster.restartNameNode();
    cluster.waitActive();
    // Make sure empty editlog can be handled
    cluster.restartNameNode();
    cluster.waitActive();
    assertTrue(fsn.getLastInodeId() == 1003);
  }

  @Test
  public void testWriteToRenamedFile() throws IOException {

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

    // Rename /test1 to test2, and recreate /test1/file
    Path renamedPath = new Path("/test2");
    fs.rename(path, renamedPath);
    fs.create(filePath, (short) 1);

    // Add new block should fail since /test1/file has a different fileId
    try {
      fos.write(data, 0, data.length);
      // make sure addBlock() request gets to NN immediately
      fos.hflush();

      fail("Write should fail after rename");
    } catch (Exception e) {
      /* Ignore */
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
