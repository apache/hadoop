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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestOpenFilesWithSnapshot {
  private static final Log LOG =
      LogFactory.getLog(TestOpenFilesWithSnapshot.class.getName());
  private final Configuration conf = new Configuration();
  MiniDFSCluster cluster = null;
  DistributedFileSystem fs = null;

  private static final long SEED = 0;
  private static final short REPLICATION = 3;
  private static final long BLOCKSIZE = 1024;
  private static final long BUFFERLEN = BLOCKSIZE / 2;
  private static final long FILELEN = BLOCKSIZE * 2;

  @Before
  public void setup() throws IOException {
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    conf.set("dfs.blocksize", "1048576");
    fs = cluster.getFileSystem();
  }

  @After
  public void teardown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

  }

  @Test
  public void testUCFileDeleteWithSnapShot() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);

    // delete files separately
    fs.delete(new Path("/test/test/test2"), true);
    fs.delete(new Path("/test/test/test3"), true);
    restartNameNode();
  }

  @Test
  public void testParentDirWithUCFileDeleteWithSnapShot() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);

    // delete parent directory
    fs.delete(new Path("/test/test"), true);
    restartNameNode();
  }

  @Test
  public void testWithCheckpoint() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);
    fs.delete(new Path("/test/test"), true);
    restartNameNode();
    
    // read snapshot file after restart
    String test2snapshotPath = Snapshot.getSnapshotPath(path.toString(),
        "s1/test/test2");
    DFSTestUtil.readFile(fs, new Path(test2snapshotPath));
    String test3snapshotPath = Snapshot.getSnapshotPath(path.toString(),
        "s1/test/test3");
    DFSTestUtil.readFile(fs, new Path(test3snapshotPath));
  }

  @Test
  public void testFilesDeletionWithCheckpoint() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);
    fs.delete(new Path("/test/test/test2"), true);
    fs.delete(new Path("/test/test/test3"), true);
    restartNameNode();
    
    // read snapshot file after restart
    String test2snapshotPath = Snapshot.getSnapshotPath(path.toString(),
        "s1/test/test2");
    DFSTestUtil.readFile(fs, new Path(test2snapshotPath));
    String test3snapshotPath = Snapshot.getSnapshotPath(path.toString(),
        "s1/test/test3");
    DFSTestUtil.readFile(fs, new Path(test3snapshotPath));
  }

  private void doWriteAndAbort(DistributedFileSystem fs, Path path)
      throws IOException {
    fs.mkdirs(path);
    fs.allowSnapshot(path);
    DFSTestUtil
        .createFile(fs, new Path("/test/test1"), 100, (short) 2, 100024L);
    DFSTestUtil
        .createFile(fs, new Path("/test/test2"), 100, (short) 2, 100024L);
    Path file = new Path("/test/test/test2");
    FSDataOutputStream out = fs.create(file);
    for (int i = 0; i < 2; i++) {
      long count = 0;
      while (count < 1048576) {
        out.writeBytes("hell");
        count += 4;
      }
    }
    ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
        .of(SyncFlag.UPDATE_LENGTH));
    DFSTestUtil.abortStream((DFSOutputStream) out.getWrappedStream());
    Path file2 = new Path("/test/test/test3");
    FSDataOutputStream out2 = fs.create(file2);
    for (int i = 0; i < 2; i++) {
      long count = 0;
      while (count < 1048576) {
        out2.writeBytes("hell");
        count += 4;
      }
    }
    ((DFSOutputStream) out2.getWrappedStream()).hsync(EnumSet
        .of(SyncFlag.UPDATE_LENGTH));
    DFSTestUtil.abortStream((DFSOutputStream) out2.getWrappedStream());
    fs.createSnapshot(path, "s1");
  }

  @Test
  public void testOpenFilesWithMultipleSnapshots() throws Exception {
    doTestMultipleSnapshots(true);
  }

  @Test
  public void testOpenFilesWithMultipleSnapshotsWithoutCheckpoint()
      throws Exception {
    doTestMultipleSnapshots(false);
  }

  private void doTestMultipleSnapshots(boolean saveNamespace)
      throws IOException {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);
    fs.createSnapshot(path, "s2");
    fs.delete(new Path("/test/test"), true);
    fs.deleteSnapshot(path, "s2");
    cluster.triggerBlockReports();
    if (saveNamespace) {
      NameNode nameNode = cluster.getNameNode();
      NameNodeAdapter.enterSafeMode(nameNode, false);
      NameNodeAdapter.saveNamespace(nameNode);
      NameNodeAdapter.leaveSafeMode(nameNode);
    }
    cluster.restartNameNode(true);
  }
  
  @Test
  public void testOpenFilesWithRename() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);

    // check for zero sized blocks
    Path fileWithEmptyBlock = new Path("/test/test/test4");
    fs.create(fileWithEmptyBlock);
    NamenodeProtocols nameNodeRpc = cluster.getNameNodeRpc();
    String clientName = fs.getClient().getClientName();
    // create one empty block
    nameNodeRpc.addBlock(fileWithEmptyBlock.toString(), clientName, null, null,
        HdfsConstants.GRANDFATHER_INODE_ID, null, null);
    fs.createSnapshot(path, "s2");

    fs.rename(new Path("/test/test"), new Path("/test/test-renamed"));
    fs.delete(new Path("/test/test-renamed"), true);
    restartNameNode();
  }

  private void createFile(final Path filePath) throws IOException {
    DFSTestUtil.createFile(fs, filePath, (int) BUFFERLEN,
        FILELEN, BLOCKSIZE, REPLICATION, SEED);
  }

  private int writeToStream(final FSDataOutputStream outputStream, byte[] buf)
      throws IOException {
    outputStream.write(buf);
    ((HdfsDataOutputStream)outputStream).hsync(
        EnumSet.of(SyncFlag.UPDATE_LENGTH));
    return buf.length;
  }

  /**
   * Test open files under snapshot directories are getting captured
   * in snapshots as a truly immutable copy. Verify open files outside
   * of snapshot directory not getting affected.
   *
   * \- level_0_A
   *   \- level_1_C
   *     +- appA.log         (open file, not under snap root)
   *     \- level_2_E        (Snapshottable Dir)
   *       \- level_3_G
   *         +- flume.log    (open file, under snap root)
   * \- level_0_B
   *   +- appB.log         (open file, not under snap root)
   *   \- level_2_D        (Snapshottable Dir)
   *     +- hbase.log      (open file, under snap root)
   */
  @Test (timeout = 120000)
  public void testPointInTimeSnapshotCopiesForOpenFiles() throws Exception {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES,
        true);
    // Construct the directory tree
    final Path level0A = new Path("/level_0_A");
    final Path level0B = new Path("/level_0_B");
    final Path level1C = new Path(level0A, "level_1_C");
    final Path level1D = new Path(level0B, "level_1_D");
    final Path level2E = new Path(level1C, "level_2_E");
    final Path level3G = new Path(level2E, "level_3_G");
    Set<Path> dirPaths = new HashSet<>(Arrays.asList(level0A, level0B,
        level1C, level1D, level2E, level3G));
    for (Path dirPath : dirPaths) {
      fs.mkdirs(dirPath);
    }

    // String constants
    final Path flumeSnapRootDir = level2E;
    final Path hbaseSnapRootDir = level1D;
    final String flumeFileName = "flume.log";
    final String hbaseFileName = "hbase.log";
    final String appAFileName = "appA.log";
    final String appBFileName = "appB.log";
    final String flumeSnap1Name = "flume_snap_s1";
    final String flumeSnap2Name = "flume_snap_s2";
    final String flumeSnap3Name = "flume_snap_s3";
    final String hbaseSnap1Name = "hbase_snap_s1";
    final String hbaseSnap2Name = "hbase_snap_s2";
    final String hbaseSnap3Name = "hbase_snap_s3";
    final String flumeRelPathFromSnapDir = "level_3_G/" + flumeFileName;

    // Create files and open a stream
    final Path flumeFile = new Path(level3G, flumeFileName);
    createFile(flumeFile);
    FSDataOutputStream flumeOutputStream = fs.append(flumeFile);

    final Path hbaseFile = new Path(level1D, hbaseFileName);
    createFile(hbaseFile);
    FSDataOutputStream hbaseOutputStream = fs.append(hbaseFile);

    final Path appAFile = new Path(level1C, appAFileName);
    createFile(appAFile);
    FSDataOutputStream appAOutputStream = fs.append(appAFile);

    final Path appBFile = new Path(level0B, appBFileName);
    createFile(appBFile);
    FSDataOutputStream appBOutputStream = fs.append(appBFile);

    final long appAFileInitialLength = fs.getFileStatus(appAFile).getLen();
    final long appBFileInitialLength = fs.getFileStatus(appBFile).getLen();

    // Create Snapshot S1
    final Path flumeS1Dir = SnapshotTestHelper.createSnapshot(
        fs, flumeSnapRootDir, flumeSnap1Name);
    final Path flumeS1Path = new Path(flumeS1Dir, flumeRelPathFromSnapDir);
    final Path hbaseS1Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap1Name);
    final Path hbaseS1Path = new Path(hbaseS1Dir, hbaseFileName);

    final long flumeFileLengthAfterS1 = fs.getFileStatus(flumeFile).getLen();
    final long hbaseFileLengthAfterS1 = fs.getFileStatus(hbaseFile).getLen();

    // Verify if Snap S1 file lengths are same as the the live ones
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS1,
        fs.getFileStatus(hbaseS1Path).getLen());
    Assert.assertEquals(appAFileInitialLength,
        fs.getFileStatus(appAFile).getLen());
    Assert.assertEquals(appBFileInitialLength,
        fs.getFileStatus(appBFile).getLen());

    long flumeFileWrittenDataLength = flumeFileLengthAfterS1;
    long hbaseFileWrittenDataLength = hbaseFileLengthAfterS1;
    long appAFileWrittenDataLength = appAFileInitialLength;

    int newWriteLength = (int) (BLOCKSIZE * 1.5);
    byte[] buf = new byte[newWriteLength];
    Random random = new Random();
    random.nextBytes(buf);

    // Write more data to flume and hbase files only
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);
    hbaseFileWrittenDataLength += writeToStream(hbaseOutputStream, buf);

    // Create Snapshot S2
    final Path flumeS2Dir = SnapshotTestHelper.createSnapshot(
        fs, flumeSnapRootDir, flumeSnap2Name);
    final Path flumeS2Path = new Path(flumeS2Dir, flumeRelPathFromSnapDir);
    final Path hbaseS2Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap2Name);
    final Path hbaseS2Path = new Path(hbaseS2Dir, hbaseFileName);

    // Verify live files lengths are same as all data written till now
    final long flumeFileLengthAfterS2 = fs.getFileStatus(flumeFile).getLen();
    final long hbaseFileLengthAfterS2 = fs.getFileStatus(hbaseFile).getLen();
    Assert.assertEquals(flumeFileWrittenDataLength, flumeFileLengthAfterS2);
    Assert.assertEquals(hbaseFileWrittenDataLength, hbaseFileLengthAfterS2);

    // Verify if Snap S2 file lengths are same as the live ones
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS2,
        fs.getFileStatus(hbaseS2Path).getLen());
    Assert.assertEquals(appAFileInitialLength,
        fs.getFileStatus(appAFile).getLen());
    Assert.assertEquals(appBFileInitialLength,
        fs.getFileStatus(appBFile).getLen());

    // Write more data to appA file only
    newWriteLength = (int) (BLOCKSIZE * 2.5);
    buf = new byte[newWriteLength];
    random.nextBytes(buf);
    appAFileWrittenDataLength += writeToStream(appAOutputStream, buf);

    // Verify other open files are not affected in their snapshots
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());
    Assert.assertEquals(appAFileWrittenDataLength,
        fs.getFileStatus(appAFile).getLen());

    // Write more data to flume file only
    newWriteLength = (int) (BLOCKSIZE * 2.5);
    buf = new byte[newWriteLength];
    random.nextBytes(buf);
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Create Snapshot S3
    final Path flumeS3Dir = SnapshotTestHelper.createSnapshot(
        fs, flumeSnapRootDir, flumeSnap3Name);
    final Path flumeS3Path = new Path(flumeS3Dir, flumeRelPathFromSnapDir);
    final Path hbaseS3Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap3Name);
    final Path hbaseS3Path = new Path(hbaseS3Dir, hbaseFileName);

    // Verify live files lengths are same as all data written till now
    final long flumeFileLengthAfterS3 = fs.getFileStatus(flumeFile).getLen();
    final long hbaseFileLengthAfterS3 = fs.getFileStatus(hbaseFile).getLen();
    Assert.assertEquals(flumeFileWrittenDataLength, flumeFileLengthAfterS3);
    Assert.assertEquals(hbaseFileWrittenDataLength, hbaseFileLengthAfterS3);

    // Verify if Snap S3 file lengths are same as the live ones
    Assert.assertEquals(flumeFileLengthAfterS3,
        fs.getFileStatus(flumeS3Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS3,
        fs.getFileStatus(hbaseS3Path).getLen());
    Assert.assertEquals(appAFileWrittenDataLength,
        fs.getFileStatus(appAFile).getLen());
    Assert.assertEquals(appBFileInitialLength,
        fs.getFileStatus(appBFile).getLen());

    // Verify old flume snapshots have point-in-time / frozen file lengths
    // even after the live file have moved forward.
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());
    Assert.assertEquals(flumeFileLengthAfterS3,
        fs.getFileStatus(flumeS3Path).getLen());

    // Verify old hbase snapshots have point-in-time / frozen file lengths
    // even after the live files have moved forward.
    Assert.assertEquals(hbaseFileLengthAfterS1,
        fs.getFileStatus(hbaseS1Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS2,
        fs.getFileStatus(hbaseS2Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS3,
        fs.getFileStatus(hbaseS3Path).getLen());

    flumeOutputStream.close();
    hbaseOutputStream.close();
    appAOutputStream.close();
    appBOutputStream.close();
  }

  /**
   * Test snapshot capturing open files and verify the same
   * across NameNode restarts.
   */
  @Test (timeout = 120000)
  public void testSnapshotsForOpenFilesWithNNRestart() throws Exception {
    // Construct the directory tree
    final Path level0A = new Path("/level_0_A");
    final Path flumeSnapRootDir = level0A;
    final String flumeFileName = "flume.log";
    final String flumeSnap1Name = "flume_snap_1";
    final String flumeSnap2Name = "flume_snap_2";

    // Create files and open a stream
    final Path flumeFile = new Path(level0A, flumeFileName);
    createFile(flumeFile);
    FSDataOutputStream flumeOutputStream = fs.append(flumeFile);

    // Create Snapshot S1
    final Path flumeS1Dir = SnapshotTestHelper.createSnapshot(
        fs, flumeSnapRootDir, flumeSnap1Name);
    final Path flumeS1Path = new Path(flumeS1Dir, flumeFileName);
    final long flumeFileLengthAfterS1 = fs.getFileStatus(flumeFile).getLen();

    // Verify if Snap S1 file length is same as the the live one
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());

    long flumeFileWrittenDataLength = flumeFileLengthAfterS1;
    int newWriteLength = (int) (BLOCKSIZE * 1.5);
    byte[] buf = new byte[newWriteLength];
    Random random = new Random();
    random.nextBytes(buf);

    // Write more data to flume file
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Create Snapshot S2
    final Path flumeS2Dir = SnapshotTestHelper.createSnapshot(
        fs, flumeSnapRootDir, flumeSnap2Name);
    final Path flumeS2Path = new Path(flumeS2Dir, flumeFileName);

    // Verify live files length is same as all data written till now
    final long flumeFileLengthAfterS2 = fs.getFileStatus(flumeFile).getLen();
    Assert.assertEquals(flumeFileWrittenDataLength, flumeFileLengthAfterS2);

    // Verify if Snap S2 file length is same as the live one
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());

    // Write more data to flume file
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Verify old flume snapshots have point-in-time / frozen file lengths
    // even after the live file have moved forward.
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());

    // Restart the NameNode
    restartNameNode();
    cluster.waitActive();

    // Verify live file length hasn't changed after NN restart
    Assert.assertEquals(flumeFileWrittenDataLength,
        fs.getFileStatus(flumeFile).getLen());

    // Verify old flume snapshots have point-in-time / frozen file lengths
    // after NN restart and live file moved forward.
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());

    flumeOutputStream.close();
  }

  /**
   * Test snapshot capturing open files when an open file with active lease
   * is deleted by the client.
   */
  @Test (timeout = 120000)
  public void testSnapshotsForOpenFilesAndDeletion() throws Exception {
    // Construct the directory tree
    final Path snapRootDir = new Path("/level_0_A");
    final String flumeFileName = "flume.log";
    final String hbaseFileName = "hbase.log";
    final String snap1Name = "snap_1";
    final String snap2Name = "snap_2";
    final String snap3Name = "snap_3";

    // Create files and open streams
    final Path flumeFile = new Path(snapRootDir, flumeFileName);
    createFile(flumeFile);
    final Path hbaseFile = new Path(snapRootDir, hbaseFileName);
    createFile(hbaseFile);
    FSDataOutputStream flumeOutputStream = fs.append(flumeFile);
    FSDataOutputStream hbaseOutputStream = fs.append(hbaseFile);

    // Create Snapshot S1
    final Path snap1Dir = SnapshotTestHelper.createSnapshot(
        fs, snapRootDir, snap1Name);
    final Path flumeS1Path = new Path(snap1Dir, flumeFileName);
    final long flumeFileLengthAfterS1 = fs.getFileStatus(flumeFile).getLen();
    final Path hbaseS1Path = new Path(snap1Dir, hbaseFileName);
    final long hbaseFileLengthAfterS1 = fs.getFileStatus(hbaseFile).getLen();

    // Verify if Snap S1 file length is same as the the current versions
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS1,
        fs.getFileStatus(hbaseS1Path).getLen());

    long flumeFileWrittenDataLength = flumeFileLengthAfterS1;
    long hbaseFileWrittenDataLength = hbaseFileLengthAfterS1;
    int newWriteLength = (int) (BLOCKSIZE * 1.5);
    byte[] buf = new byte[newWriteLength];
    Random random = new Random();
    random.nextBytes(buf);

    // Write more data to files
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);
    hbaseFileWrittenDataLength += writeToStream(hbaseOutputStream, buf);

    // Create Snapshot S2
    final Path snap2Dir = SnapshotTestHelper.createSnapshot(
        fs, snapRootDir, snap2Name);
    final Path flumeS2Path = new Path(snap2Dir, flumeFileName);
    final Path hbaseS2Path = new Path(snap2Dir, hbaseFileName);

    // Verify current files length are same as all data written till now
    final long flumeFileLengthAfterS2 = fs.getFileStatus(flumeFile).getLen();
    Assert.assertEquals(flumeFileWrittenDataLength, flumeFileLengthAfterS2);
    final long hbaseFileLengthAfterS2 = fs.getFileStatus(hbaseFile).getLen();
    Assert.assertEquals(hbaseFileWrittenDataLength, hbaseFileLengthAfterS2);

    // Verify if Snap S2 file length is same as the current versions
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS2,
        fs.getFileStatus(hbaseS2Path).getLen());

    // Write more data to open files
    writeToStream(flumeOutputStream, buf);
    hbaseFileWrittenDataLength += writeToStream(hbaseOutputStream, buf);

    // Verify old snapshots have point-in-time/frozen file
    // lengths even after the current versions have moved forward.
    Assert.assertEquals(flumeFileLengthAfterS1,
        fs.getFileStatus(flumeS1Path).getLen());
    Assert.assertEquals(flumeFileLengthAfterS2,
        fs.getFileStatus(flumeS2Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS1,
        fs.getFileStatus(hbaseS1Path).getLen());
    Assert.assertEquals(hbaseFileLengthAfterS2,
        fs.getFileStatus(hbaseS2Path).getLen());

    // Delete flume current file. Snapshots should
    // still have references to flume file.
    boolean flumeFileDeleted = fs.delete(flumeFile, true);
    Assert.assertTrue(flumeFileDeleted);
    Assert.assertFalse(fs.exists(flumeFile));
    Assert.assertTrue(fs.exists(flumeS1Path));
    Assert.assertTrue(fs.exists(flumeS2Path));

    SnapshotTestHelper.createSnapshot(fs, snapRootDir, "tmp_snap");
    fs.deleteSnapshot(snapRootDir, "tmp_snap");

    // Delete snap_2. snap_1 still has reference to
    // the flume file.
    fs.deleteSnapshot(snapRootDir, snap2Name);
    Assert.assertFalse(fs.exists(flumeS2Path));
    Assert.assertTrue(fs.exists(flumeS1Path));

    // Delete snap_1. Now all traces of flume file
    // is gone.
    fs.deleteSnapshot(snapRootDir, snap1Name);
    Assert.assertFalse(fs.exists(flumeS2Path));
    Assert.assertFalse(fs.exists(flumeS1Path));

    // Create Snapshot S3
    final Path snap3Dir = SnapshotTestHelper.createSnapshot(
        fs, snapRootDir, snap3Name);
    final Path hbaseS3Path = new Path(snap3Dir, hbaseFileName);

    // Verify live files length is same as all data written till now
    final long hbaseFileLengthAfterS3 = fs.getFileStatus(hbaseFile).getLen();
    Assert.assertEquals(hbaseFileWrittenDataLength, hbaseFileLengthAfterS3);

    // Write more data to open files
    hbaseFileWrittenDataLength += writeToStream(hbaseOutputStream, buf);

    // Verify old snapshots have point-in-time/frozen file
    // lengths even after the flume open file is deleted and
    // the hbase live file has moved forward.
    Assert.assertEquals(hbaseFileLengthAfterS3,
        fs.getFileStatus(hbaseS3Path).getLen());
    Assert.assertEquals(hbaseFileWrittenDataLength,
        fs.getFileStatus(hbaseFile).getLen());

    hbaseOutputStream.close();
  }

  /**
   * Test client writing to open files are not interrupted when snapshots
   * that captured open files get deleted.
   */
  @Test (timeout = 240000)
  public void testOpenFileWritingAcrossSnapDeletion() throws Exception {
    final Path snapRootDir = new Path("/level_0_A");
    final String flumeFileName = "flume.log";
    final String hbaseFileName = "hbase.log";
    final String snap1Name = "snap_1";
    final String snap2Name = "snap_2";
    final String snap3Name = "snap_3";

    // Create files and open streams
    final Path flumeFile = new Path(snapRootDir, flumeFileName);
    FSDataOutputStream flumeOut = fs.create(flumeFile, false,
        8000, (short)3, 1048576);
    flumeOut.close();
    final Path hbaseFile = new Path(snapRootDir, hbaseFileName);
    FSDataOutputStream hbaseOut = fs.create(hbaseFile, false,
        8000, (short)3, 1048576);
    hbaseOut.close();

    final AtomicBoolean writerError = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch deleteLatch = new CountDownLatch(1);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          FSDataOutputStream flumeOutputStream = fs.append(flumeFile, 8000);
          FSDataOutputStream hbaseOutputStream = fs.append(hbaseFile, 8000);
          byte[] bytes = new byte[(int) (1024 * 0.2)];
          Random r = new Random(Time.now());

          for (int i = 0; i < 200000; i++) {
            r.nextBytes(bytes);
            flumeOutputStream.write(bytes);
            if (hbaseOutputStream != null) {
              hbaseOutputStream.write(bytes);
            }
            if (i == 50000) {
              startLatch.countDown();
            } else if (i == 100000) {
              deleteLatch.countDown();
            } else if (i == 150000) {
              hbaseOutputStream.hsync();
              fs.delete(hbaseFile, true);
              try {
                hbaseOutputStream.close();
              } catch (Exception e) {
                // since the file is deleted before the open stream close,
                // it might throw FileNotFoundException. Ignore the
                // expected exception.
              }
              hbaseOutputStream = null;
            } else if (i % 5000 == 0) {
              LOG.info("Write pos: " + flumeOutputStream.getPos()
                  + ", size: " + fs.getFileStatus(flumeFile).getLen()
                  + ", loop: " + (i + 1));
            }
          }
        } catch (Exception e) {
          LOG.warn("Writer error: " + e);
          writerError.set(true);
        }
      }
    });
    t.start();

    startLatch.await();
    final Path snap1Dir = SnapshotTestHelper.createSnapshot(
        fs, snapRootDir, snap1Name);
    final Path flumeS1Path = new Path(snap1Dir, flumeFileName);
    LOG.info("Snap1 file status: " + fs.getFileStatus(flumeS1Path));
    LOG.info("Current file status: " + fs.getFileStatus(flumeFile));

    deleteLatch.await();
    LOG.info("Snap1 file status: " + fs.getFileStatus(flumeS1Path));
    LOG.info("Current file status: " + fs.getFileStatus(flumeFile));

    // Verify deletion of snapshot which had the under construction file
    // captured is not truncating the under construction file and the thread
    // writing to the same file not crashing on newer block allocations.
    LOG.info("Deleting " + snap1Name);
    fs.deleteSnapshot(snapRootDir, snap1Name);

    // Verify creation and deletion of snapshot newer than the oldest
    // snapshot is not crashing the thread writing to under construction file.
    SnapshotTestHelper.createSnapshot(fs, snapRootDir, snap2Name);
    SnapshotTestHelper.createSnapshot(fs, snapRootDir, snap3Name);
    fs.deleteSnapshot(snapRootDir, snap3Name);
    fs.deleteSnapshot(snapRootDir, snap2Name);
    SnapshotTestHelper.createSnapshot(fs, snapRootDir, "test");

    t.join();
    Assert.assertFalse("Client encountered writing error!", writerError.get());

    restartNameNode();
    cluster.waitActive();
  }

  /**
   * Verify snapshots with open files captured are safe even when the
   * 'current' version of the file is truncated and appended later.
   */
  @Test (timeout = 120000)
  public void testOpenFilesSnapChecksumWithTrunkAndAppend() throws Exception {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES,
        true);
    // Construct the directory tree
    final Path dir = new Path("/A/B/C");
    fs.mkdirs(dir);

    // String constants
    final Path hbaseSnapRootDir = dir;
    final String hbaseFileName = "hbase.wal";
    final String hbaseSnap1Name = "hbase_snap_s1";
    final String hbaseSnap2Name = "hbase_snap_s2";
    final String hbaseSnap3Name = "hbase_snap_s3";
    final String hbaseSnap4Name = "hbase_snap_s4";

    // Create files and open a stream
    final Path hbaseFile = new Path(dir, hbaseFileName);
    createFile(hbaseFile);
    final FileChecksum hbaseWALFileCksum0 =
        fs.getFileChecksum(hbaseFile);
    FSDataOutputStream hbaseOutputStream = fs.append(hbaseFile);

    // Create Snapshot S1
    final Path hbaseS1Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap1Name);
    final Path hbaseS1Path = new Path(hbaseS1Dir, hbaseFileName);
    final FileChecksum hbaseFileCksumS1 = fs.getFileChecksum(hbaseS1Path);

    // Verify if Snap S1 checksum is same as the current version one
    Assert.assertEquals("Live and snap1 file checksum doesn't match!",
        hbaseWALFileCksum0, fs.getFileChecksum(hbaseS1Path));

    int newWriteLength = (int) (BLOCKSIZE * 1.5);
    byte[] buf = new byte[newWriteLength];
    Random random = new Random();
    random.nextBytes(buf);
    writeToStream(hbaseOutputStream, buf);

    // Create Snapshot S2
    final Path hbaseS2Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap2Name);
    final Path hbaseS2Path = new Path(hbaseS2Dir, hbaseFileName);
    final FileChecksum hbaseFileCksumS2 = fs.getFileChecksum(hbaseS2Path);

    // Verify if the s1 checksum is still the same
    Assert.assertEquals("Snap file checksum has changed!",
        hbaseFileCksumS1, fs.getFileChecksum(hbaseS1Path));
    // Verify if the s2 checksum is different from the s1 checksum
    Assert.assertNotEquals("Snap1 and snap2 file checksum should differ!",
        hbaseFileCksumS1, hbaseFileCksumS2);

    newWriteLength = (int) (BLOCKSIZE * 2.5);
    buf = new byte[newWriteLength];
    random.nextBytes(buf);
    writeToStream(hbaseOutputStream, buf);

    // Create Snapshot S3
    final Path hbaseS3Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap3Name);
    final Path hbaseS3Path = new Path(hbaseS3Dir, hbaseFileName);
    FileChecksum hbaseFileCksumS3 = fs.getFileChecksum(hbaseS3Path);

    // Record the checksum for the before truncate current file
    hbaseOutputStream.close();
    final FileChecksum hbaseFileCksumBeforeTruncate =
        fs.getFileChecksum(hbaseFile);
    Assert.assertEquals("Snap3 and before truncate file checksum should match!",
        hbaseFileCksumBeforeTruncate, hbaseFileCksumS3);

    // Truncate the current file and record the after truncate checksum
    long currentFileLen = fs.getFileStatus(hbaseFile).getLen();
    boolean fileTruncated = fs.truncate(hbaseFile, currentFileLen / 2);
    Assert.assertTrue("File truncation failed!", fileTruncated);
    final FileChecksum hbaseFileCksumAfterTruncate =
        fs.getFileChecksum(hbaseFile);

    Assert.assertNotEquals("Snap3 and after truncate checksum shouldn't match!",
        hbaseFileCksumS3, hbaseFileCksumAfterTruncate);

    // Append more data to the current file
    hbaseOutputStream = fs.append(hbaseFile);
    newWriteLength = (int) (BLOCKSIZE * 5.5);
    buf = new byte[newWriteLength];
    random.nextBytes(buf);
    writeToStream(hbaseOutputStream, buf);

    // Create Snapshot S4
    final Path hbaseS4Dir = SnapshotTestHelper.createSnapshot(
        fs, hbaseSnapRootDir, hbaseSnap4Name);
    final Path hbaseS4Path = new Path(hbaseS4Dir, hbaseFileName);
    final FileChecksum hbaseFileCksumS4 = fs.getFileChecksum(hbaseS4Path);

    // Record the checksum for the current file after append
    hbaseOutputStream.close();
    final FileChecksum hbaseFileCksumAfterAppend =
        fs.getFileChecksum(hbaseFile);

    Assert.assertEquals("Snap4 and after append file checksum should match!",
        hbaseFileCksumAfterAppend, hbaseFileCksumS4);

    // Recompute checksum for S3 path and verify it has not changed
    hbaseFileCksumS3 = fs.getFileChecksum(hbaseS3Path);
    Assert.assertEquals("Snap3 and before truncate file checksum should match!",
        hbaseFileCksumBeforeTruncate, hbaseFileCksumS3);
  }

  private Path createSnapshot(Path snapRootDir, String snapName,
      String fileName) throws Exception {
    final Path snap1Dir = SnapshotTestHelper.createSnapshot(
        fs, snapRootDir, snapName);
    return new Path(snap1Dir, fileName);
  }

  private void verifyFileSize(long fileSize, Path... filePaths) throws
      IOException {
    for (Path filePath : filePaths) {
      Assert.assertEquals(fileSize, fs.getFileStatus(filePath).getLen());
    }
  }

  /**
   * Verify open files captured in the snapshots across config disable
   * and enable.
   */
  @Test
  public void testOpenFilesWithMixedConfig() throws Exception {
    final Path snapRootDir = new Path("/level_0_A");
    final String flumeFileName = "flume.log";
    final String snap1Name = "s1";
    final String snap2Name = "s2";
    final String snap3Name = "s3";
    final String snap4Name = "s4";
    final String snap5Name = "s5";

    // Create files and open streams
    final Path flumeFile = new Path(snapRootDir, flumeFileName);
    createFile(flumeFile);
    FSDataOutputStream flumeOutputStream = fs.append(flumeFile);

    // 1. Disable capture open files
    cluster.getNameNode().getNamesystem()
        .getSnapshotManager().setCaptureOpenFiles(false);

    // Create Snapshot S1
    final Path flumeS1Path = createSnapshot(snapRootDir,
        snap1Name, flumeFileName);

    // Verify if Snap S1 file length is same as the the current versions
    verifyFileSize(FILELEN, flumeS1Path);

    // Write more data to files
    long flumeFileWrittenDataLength = FILELEN;
    int newWriteLength = (int) (BLOCKSIZE * 1.5);
    byte[] buf = new byte[newWriteLength];
    Random random = new Random();
    random.nextBytes(buf);
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Create Snapshot S2
    final Path flumeS2Path = createSnapshot(snapRootDir,
        snap2Name, flumeFileName);

    // Since capture open files was disabled, all snapshots paths
    // and the current version should have same file lengths.
    verifyFileSize(flumeFileWrittenDataLength,
        flumeFile, flumeS2Path, flumeS1Path);

    // 2. Enable capture open files
    cluster.getNameNode().getNamesystem()
        .getSnapshotManager() .setCaptureOpenFiles(true);

    // Write more data to files
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);
    long flumeFileLengthAfterS3 = flumeFileWrittenDataLength;

    // Create Snapshot S3
    final Path flumeS3Path = createSnapshot(snapRootDir,
        snap3Name, flumeFileName);

    // Since open files captured in the previous snapshots were with config
    // disabled, their file lengths are now same as the current version.
    // With the config turned on, any new data written to the open files
    // will no more reflect in the current version or old snapshot paths.
    verifyFileSize(flumeFileWrittenDataLength, flumeFile, flumeS3Path,
        flumeS2Path, flumeS1Path);

    // Write more data to files
    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Create Snapshot S4
    final Path flumeS4Path = createSnapshot(snapRootDir,
        snap4Name, flumeFileName);

    // Verify S4 has the latest data
    verifyFileSize(flumeFileWrittenDataLength, flumeFile, flumeS4Path);

    // But, open files captured as of Snapshot S3 and before should
    // have their old file lengths intact.
    verifyFileSize(flumeFileLengthAfterS3, flumeS3Path,
        flumeS2Path, flumeS1Path);

    long flumeFileLengthAfterS4 =  flumeFileWrittenDataLength;

    // 3. Disable capture open files
    cluster.getNameNode().getNamesystem()
        .getSnapshotManager() .setCaptureOpenFiles(false);

    // Create Snapshot S5
    final Path flumeS5Path = createSnapshot(snapRootDir,
        snap5Name, flumeFileName);

    flumeFileWrittenDataLength += writeToStream(flumeOutputStream, buf);

    // Since capture open files was disabled, any snapshots taken after the
    // config change and the current version should have same file lengths
    // for the open files.
    verifyFileSize(flumeFileWrittenDataLength, flumeFile, flumeS5Path);

    // But, the old snapshots taken before the config disable should
    // continue to be consistent.
    verifyFileSize(flumeFileLengthAfterS4, flumeS4Path);
  }

  private void restartNameNode() throws Exception {
    cluster.triggerBlockReports();
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    cluster.restartNameNode(true);
  }
}