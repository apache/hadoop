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
import java.util.EnumSet;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.junit.Test;

public class TestFSImage {

  @Test
  public void testPersist() throws IOException {
    Configuration conf = new Configuration();
    testPersistHelper(conf);
  }

  @Test
  public void testCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
        "org.apache.hadoop.io.compress.GzipCodec");
    testPersistHelper(conf);
  }

  private void testPersistHelper(Configuration conf) throws IOException {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      DistributedFileSystem fs = cluster.getFileSystem();

      final Path dir = new Path("/abc/def");
      final Path file1 = new Path(dir, "f1");
      final Path file2 = new Path(dir, "f2");

      // create an empty file f1
      fs.create(file1).close();

      // create an under-construction file f2
      FSDataOutputStream out = fs.create(file2);
      out.writeBytes("hello");
      ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.UPDATE_LENGTH));

      // checkpoint
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      cluster.restartNameNode();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      assertTrue(fs.isDirectory(dir));
      assertTrue(fs.exists(file1));
      assertTrue(fs.exists(file2));

      // check internals of file2
      INodeFile file2Node = fsn.dir.getINode4Write(file2.toString()).asFile();
      assertEquals("hello".length(), file2Node.computeFileSize());
      assertTrue(file2Node.isUnderConstruction());
      BlockInfo[] blks = file2Node.getBlocks();
      assertEquals(1, blks.length);
      assertEquals(BlockUCState.UNDER_CONSTRUCTION, blks[0].getBlockUCState());
      // check lease manager
      Lease lease = fsn.leaseManager.getLeaseByPath(file2.toString());
      Assert.assertNotNull(lease);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Ensure that the digest written by the saver equals to the digest of the
   * file.
   */
  @Test
  public void testDigest() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      File currentDir = FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0).get(
          0);
      File fsimage = FSImageTestUtil.findNewestImageFile(currentDir
          .getAbsolutePath());
      assertEquals(MD5FileUtils.readStoredMd5ForFile(fsimage),
          MD5FileUtils.computeMd5ForFile(fsimage));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Ensure mtime and atime can be loaded from fsimage.
   */
  @Test(timeout=60000)
  public void testLoadMtimeAtime() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      String userDir = hdfs.getHomeDirectory().toUri().getPath().toString();
      Path file = new Path(userDir, "file");
      Path dir = new Path(userDir, "/dir");
      Path link = new Path(userDir, "/link");
      hdfs.createNewFile(file);
      hdfs.mkdirs(dir);
      hdfs.createSymlink(file, link, false);

      long mtimeFile = hdfs.getFileStatus(file).getModificationTime();
      long atimeFile = hdfs.getFileStatus(file).getAccessTime();
      long mtimeDir = hdfs.getFileStatus(dir).getModificationTime();
      long mtimeLink = hdfs.getFileLinkStatus(link).getModificationTime();
      long atimeLink = hdfs.getFileLinkStatus(link).getAccessTime();

      // save namespace and restart cluster
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      hdfs.saveNamespace();
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).format(false)
          .numDataNodes(1).build();
      cluster.waitActive();
      hdfs = cluster.getFileSystem();
      
      assertEquals(mtimeFile, hdfs.getFileStatus(file).getModificationTime());
      assertEquals(atimeFile, hdfs.getFileStatus(file).getAccessTime());
      assertEquals(mtimeDir, hdfs.getFileStatus(dir).getModificationTime());
      assertEquals(mtimeLink, hdfs.getFileLinkStatus(link).getModificationTime());
      assertEquals(atimeLink, hdfs.getFileLinkStatus(link).getAccessTime());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
