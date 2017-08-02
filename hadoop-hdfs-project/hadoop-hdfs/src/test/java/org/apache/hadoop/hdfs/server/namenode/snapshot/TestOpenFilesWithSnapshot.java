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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestOpenFilesWithSnapshot {
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

  private void restartNameNode() throws Exception {
    cluster.triggerBlockReports();
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    cluster.restartNameNode(true);
  }
}