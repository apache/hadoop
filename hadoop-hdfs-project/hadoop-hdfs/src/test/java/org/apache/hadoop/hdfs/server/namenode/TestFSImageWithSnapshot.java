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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.Canceler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test FSImage save/load when Snapshot is supported
 */
public class TestFSImageWithSnapshot {
  static final long seed = 0;
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1024;
  static final long txid = 1;

  private final Path dir = new Path("/TestSnapshot");
  private static String testDir =
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
    // make changes to the namesystem
    hdfs.mkdirs(dir);
    hdfs.allowSnapshot(dir.toString());
    hdfs.createSnapshot(dir, "s0");
    
    Path sub1 = new Path(dir, "sub1");
    Path sub1file1 = new Path(sub1, "sub1file1");
    Path sub1file2 = new Path(sub1, "sub1file2");
    DFSTestUtil.createFile(hdfs, sub1file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub1file2, BLOCKSIZE, REPLICATION, seed);
    
    hdfs.createSnapshot(dir, "s1");
    
    Path sub2 = new Path(dir, "sub2");
    Path sub2file1 = new Path(sub2, "sub2file1");
    Path sub2file2 = new Path(sub2, "sub2file2");
    DFSTestUtil.createFile(hdfs, sub2file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, sub2file2, BLOCKSIZE, REPLICATION, seed);
    hdfs.setReplication(sub1file1, (short) (REPLICATION - 1));
    hdfs.delete(sub1file2, true);
    
    hdfs.createSnapshot(dir, "s2");
    hdfs.setOwner(sub2, "dr.who", "unknown");
    hdfs.delete(sub2file2, true);
    
    // dump the fsdir tree
    StringBuffer fsnStrBefore = fsn.getFSDirectory().rootDir
        .dumpTreeRecursively();
    
    // save the namesystem to a temp file
    SaveNamespaceContext context = new SaveNamespaceContext(fsn, txid,
        new Canceler());
    FSImageFormat.Saver saver = new FSImageFormat.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    File dstFile = getStorageFile(testDir, txid);
    fsn.readLock();
    try {
      saver.save(dstFile, compression);
    } finally {
      fsn.readUnlock();
    }

    // restart the cluster, and format the cluster
    cluster.shutdown();
    cluster = new MiniDFSCluster.Builder(conf).format(true)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    
    // load the namesystem from the temp file
    FSImageFormat.Loader loader = new FSImageFormat.Loader(conf, fsn);
    fsn.writeLock();
    try {
      loader.load(dstFile);
    } finally {
      fsn.writeUnlock();
    }
    
    // dump the fsdir tree again
    StringBuffer fsnStrAfter = fsn.getFSDirectory().rootDir
        .dumpTreeRecursively();
    
    // compare two dumped tree
    System.out.println(fsnStrBefore.toString());
    System.out.println("\n" + fsnStrAfter.toString());
    assertEquals(fsnStrBefore.toString(), fsnStrAfter.toString());
  }
  
  /**
   * Create a temp fsimage file for testing.
   * @param dir The directory where the fsimage file resides
   * @param imageTxId The transaction id of the fsimage
   * @return The file of the image file
   */
  private File getStorageFile(String dir, long imageTxId) {
    return new File(dir, String.format("%s_%019d", NameNodeFile.IMAGE,
        imageTxId));
  }
}