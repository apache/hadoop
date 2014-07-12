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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test FSImage save/load when Snapshot is supported
 */
public class TestFSImageWithSnapshot {
  {
    SnapshotTestHelper.disableLogs();
    ((Log4JLogger)INode.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final int BLOCKSIZE = 1024;
  static final long txid = 1;

  private final Path dir = new Path("/TestSnapshot");
  private static final String testDir =
      System.getProperty("test.build.data", "build/test/data");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create a temp fsimage file for testing.
   * @param dir The directory where the fsimage file resides
   * @param imageTxId The transaction id of the fsimage
   * @return The file of the image file
   */
  private File getImageFile(String dir, long imageTxId) {
    return new File(dir, String.format("%s_%019d", NameNodeFile.IMAGE,
        imageTxId));
  }
  
  /** 
   * Create a temp file for dumping the fsdir
   * @param dir directory for the temp file
   * @param suffix suffix of of the temp file
   * @return the temp file
   */
  private File getDumpTreeFile(String dir, String suffix) {
    return new File(dir, String.format("dumpTree_%s", suffix));
  }
  
  /** 
   * Dump the fsdir tree to a temp file
   * @param fileSuffix suffix of the temp file for dumping
   * @return the temp file
   */
  private File dumpTree2File(String fileSuffix) throws IOException {
    File file = getDumpTreeFile(testDir, fileSuffix);
    SnapshotTestHelper.dumpTree2File(fsn.getFSDirectory(), file);
    return file;
  }
  
  /** Append a file without closing the output stream */
  private HdfsDataOutputStream appendFileWithoutClosing(Path file, int length)
      throws IOException {
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    HdfsDataOutputStream out = (HdfsDataOutputStream) hdfs.append(file);
    out.write(toAppend);
    return out;
  }
  
  /** Save the fsimage to a temp file */
  private File saveFSImageToTempFile() throws IOException {
    SaveNamespaceContext context = new SaveNamespaceContext(fsn, txid,
        new Canceler());
    FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    File imageFile = getImageFile(testDir, txid);
    fsn.readLock();
    try {
      saver.save(imageFile, compression);
    } finally {
      fsn.readUnlock();
    }
    return imageFile;
  }
  
  /** Load the fsimage from a temp file */
  private void loadFSImageFromTempFile(File imageFile) throws IOException {
    FSImageFormat.LoaderDelegator loader = FSImageFormat.newLoader(conf, fsn);
    fsn.writeLock();
    fsn.getFSDirectory().writeLock();
    try {
      loader.load(imageFile);
      FSImage.updateCountForQuota(
          INodeDirectory.valueOf(fsn.getFSDirectory().getINode("/"), "/"));
    } finally {
      fsn.getFSDirectory().writeUnlock();
      fsn.writeUnlock();
    }
  }
  
  /**
   * Test when there is snapshot taken on root
   */
  @Test
  public void testSnapshotOnRoot() throws Exception {
    final Path root = new Path("/");
    hdfs.allowSnapshot(root);
    hdfs.createSnapshot(root, "s1");
    
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    
    INodeDirectory rootNode = fsn.dir.getINode4Write(root.toString())
        .asDirectory();
    assertTrue("The children list of root should be empty", 
        rootNode.getChildrenList(Snapshot.CURRENT_STATE_ID).isEmpty());
    // one snapshot on root: s1
    List<DirectoryDiff> diffList = rootNode.getDiffs().asList();
    assertEquals(1, diffList.size());
    Snapshot s1 = rootNode.getSnapshot(DFSUtil.string2Bytes("s1"));
    assertEquals(s1.getId(), diffList.get(0).getSnapshotId());
    
    // check SnapshotManager's snapshottable directory list
    assertEquals(1, fsn.getSnapshotManager().getNumSnapshottableDirs());
    SnapshottableDirectoryStatus[] sdirs = fsn.getSnapshotManager()
        .getSnapshottableDirListing(null);
    assertEquals(root, sdirs[0].getFullPath());
    
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  /**
   * Testing steps:
   * <pre>
   * 1. Creating/modifying directories/files while snapshots are being taken.
   * 2. Dump the FSDirectory tree of the namesystem.
   * 3. Save the namesystem to a temp file (FSImage saving).
   * 4. Restart the cluster and format the namesystem.
   * 5. Load the namesystem from the temp file (FSImage loading).
   * 6. Dump the FSDirectory again and compare the two dumped string.
   * </pre>
   */
  @Test
  public void testSaveLoadImage() throws Exception {
    int s = 0;
    // make changes to the namesystem
    hdfs.mkdirs(dir);
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s" + ++s);
    Path sub1 = new Path(dir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.setPermission(sub1, new FsPermission((short)0777));
    Path sub11 = new Path(sub1, "sub11");
    hdfs.mkdirs(sub11);
    checkImage(s);

    hdfs.createSnapshot(dir, "s" + ++s);
    Path sub1file1 = new Path(sub1, "sub1file1");
    Path sub1file2 = new Path(sub1, "sub1file2");
    DFSTestUtil.createFile(hdfs, sub1file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub1file2, BLOCKSIZE, REPLICATION, seed);
    checkImage(s);
    
    hdfs.createSnapshot(dir, "s" + ++s);
    Path sub2 = new Path(dir, "sub2");
    Path sub2file1 = new Path(sub2, "sub2file1");
    Path sub2file2 = new Path(sub2, "sub2file2");
    DFSTestUtil.createFile(hdfs, sub2file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub2file2, BLOCKSIZE, REPLICATION, seed);
    checkImage(s);

    hdfs.createSnapshot(dir, "s" + ++s);
    hdfs.setReplication(sub1file1, (short) (REPLICATION - 1));
    hdfs.delete(sub1file2, true);
    hdfs.setOwner(sub2, "dr.who", "unknown");
    hdfs.delete(sub2file1, true);
    checkImage(s);
    
    hdfs.createSnapshot(dir, "s" + ++s);
    Path sub1_sub2file2 = new Path(sub1, "sub2file2");
    hdfs.rename(sub2file2, sub1_sub2file2);
    
    hdfs.rename(sub1file1, sub2file1);
    checkImage(s);
    
    hdfs.rename(sub2file1, sub2file2);
    checkImage(s);
  }

  void checkImage(int s) throws IOException {
    final String name = "s" + s;

    // dump the fsdir tree
    File fsnBefore = dumpTree2File(name + "_before");
    
    // save the namesystem to a temp file
    File imageFile = saveFSImageToTempFile();
    
    long numSdirBefore = fsn.getNumSnapshottableDirs();
    long numSnapshotBefore = fsn.getNumSnapshots();
    SnapshottableDirectoryStatus[] dirBefore = hdfs.getSnapshottableDirListing();

    // shutdown the cluster
    cluster.shutdown();

    // dump the fsdir tree
    File fsnBetween = dumpTree2File(name + "_between");
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnBetween, true);

    // restart the cluster, and format the cluster
    cluster = new MiniDFSCluster.Builder(conf).format(true)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    
    // load the namesystem from the temp file
    loadFSImageFromTempFile(imageFile);
    
    // dump the fsdir tree again
    File fsnAfter = dumpTree2File(name + "_after");
    
    // compare two dumped tree
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnAfter, true);
    
    long numSdirAfter = fsn.getNumSnapshottableDirs();
    long numSnapshotAfter = fsn.getNumSnapshots();
    SnapshottableDirectoryStatus[] dirAfter = hdfs.getSnapshottableDirListing();
    
    Assert.assertEquals(numSdirBefore, numSdirAfter);
    Assert.assertEquals(numSnapshotBefore, numSnapshotAfter);
    Assert.assertEquals(dirBefore.length, dirAfter.length);
    List<String> pathListBefore = new ArrayList<String>();
    for (SnapshottableDirectoryStatus sBefore : dirBefore) {
      pathListBefore.add(sBefore.getFullPath().toString());
    }
    for (SnapshottableDirectoryStatus sAfter : dirAfter) {
      Assert.assertTrue(pathListBefore.contains(sAfter.getFullPath().toString()));
    }
  }
  
  /**
   * Test the fsimage saving/loading while file appending.
   */
  @Test (timeout=60000)
  public void testSaveLoadImageWithAppending() throws Exception {
    Path sub1 = new Path(dir, "sub1");
    Path sub1file1 = new Path(sub1, "sub1file1");
    Path sub1file2 = new Path(sub1, "sub1file2");
    DFSTestUtil.createFile(hdfs, sub1file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub1file2, BLOCKSIZE, REPLICATION, seed);
    
    // 1. create snapshot s0
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s0");
    
    // 2. create snapshot s1 before appending sub1file1 finishes
    HdfsDataOutputStream out = appendFileWithoutClosing(sub1file1, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    // also append sub1file2
    DFSTestUtil.appendFile(hdfs, sub1file2, BLOCKSIZE);
    hdfs.createSnapshot(dir, "s1");
    out.close();
    
    // 3. create snapshot s2 before appending finishes
    out = appendFileWithoutClosing(sub1file1, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    hdfs.createSnapshot(dir, "s2");
    out.close();
    
    // 4. save fsimage before appending finishes
    out = appendFileWithoutClosing(sub1file1, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    // dump fsdir
    File fsnBefore = dumpTree2File("before");
    // save the namesystem to a temp file
    File imageFile = saveFSImageToTempFile();
    
    // 5. load fsimage and compare
    // first restart the cluster, and format the cluster
    out.close();
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(true)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    // then load the fsimage
    loadFSImageFromTempFile(imageFile);
    
    // dump the fsdir tree again
    File fsnAfter = dumpTree2File("after");
    
    // compare two dumped tree
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnAfter, true);
  }
  
  /**
   * Test the fsimage loading while there is file under construction.
   */
  @Test (timeout=60000)
  public void testLoadImageWithAppending() throws Exception {
    Path sub1 = new Path(dir, "sub1");
    Path sub1file1 = new Path(sub1, "sub1file1");
    Path sub1file2 = new Path(sub1, "sub1file2");
    DFSTestUtil.createFile(hdfs, sub1file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub1file2, BLOCKSIZE, REPLICATION, seed);
    
    hdfs.allowSnapshot(dir);
    hdfs.createSnapshot(dir, "s0");
    
    HdfsDataOutputStream out = appendFileWithoutClosing(sub1file1, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));      
    
    // save namespace and restart cluster
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }
  
  /**
   * Test fsimage loading when 1) there is an empty file loaded from fsimage,
   * and 2) there is later an append operation to be applied from edit log.
   */
  @Test (timeout=60000)
  public void testLoadImageWithEmptyFile() throws Exception {
    // create an empty file
    Path file = new Path(dir, "file");
    FSDataOutputStream out = hdfs.create(file);
    out.close();
    
    // save namespace
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    
    // append to the empty file
    out = hdfs.append(file);
    out.write(1);
    out.close();
    
    // restart cluster
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
    
    FileStatus status = hdfs.getFileStatus(file);
    assertEquals(1, status.getLen());
  }
  
  /**
   * Testing a special case with snapshots. When the following steps happen:
   * <pre>
   * 1. Take snapshot s1 on dir.
   * 2. Create new dir and files under subsubDir, which is descendant of dir.
   * 3. Take snapshot s2 on dir.
   * 4. Delete subsubDir.
   * 5. Delete snapshot s2.
   * </pre>
   * When we merge the diff from s2 to s1 (since we deleted s2), we need to make
   * sure all the files/dirs created after s1 should be destroyed. Otherwise
   * we may save these files/dirs to the fsimage, and cause FileNotFound 
   * Exception while loading fsimage.  
   */
  @Test (timeout=300000)
  public void testSaveLoadImageAfterSnapshotDeletion()
      throws Exception {
    // create initial dir and subdir
    Path dir = new Path("/dir");
    Path subDir = new Path(dir, "subdir");
    Path subsubDir = new Path(subDir, "subsubdir");
    hdfs.mkdirs(subsubDir);
    
    // take snapshots on subdir and dir
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    
    // create new dir under initial dir
    Path newDir = new Path(subsubDir, "newdir");
    Path newFile = new Path(newDir, "newfile");
    hdfs.mkdirs(newDir);
    DFSTestUtil.createFile(hdfs, newFile, BLOCKSIZE, REPLICATION, seed);
    
    // create another snapshot
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s2");
    
    // delete subsubdir
    hdfs.delete(subsubDir, true);
    
    // delete snapshot s2
    hdfs.deleteSnapshot(dir, "s2");
    
    // restart cluster
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(false).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    
    // save namespace to fsimage
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(false)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }
}