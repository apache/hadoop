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
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOpenFilesWithSnapshot {
  private final Configuration conf = new Configuration();
  MiniDFSCluster cluster = null;
  DistributedFileSystem fs = null;

  @Before
  public void setup() throws IOException {
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
    cluster.restartNameNode();
  }

  @Test
  public void testParentDirWithUCFileDeleteWithSnapShot() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);

    // delete parent directory
    fs.delete(new Path("/test/test"), true);
    cluster.restartNameNode();
  }

  @Test
  public void testWithCheckpoint() throws Exception {
    Path path = new Path("/test");
    doWriteAndAbort(fs, path);
    fs.delete(new Path("/test/test"), true);
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    cluster.restartNameNode(true);
    
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
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    cluster.restartNameNode(true);
    
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
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    NameNodeAdapter.leaveSafeMode(nameNode);
    cluster.restartNameNode(true);
  }
}