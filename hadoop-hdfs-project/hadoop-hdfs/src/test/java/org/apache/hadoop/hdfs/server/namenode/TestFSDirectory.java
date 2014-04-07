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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link FSDirectory}, the in-memory namespace tree.
 */
public class TestFSDirectory {
  public static final Log LOG = LogFactory.getLog(TestFSDirectory.class);

  private static final long seed = 0;
  private static final short REPLICATION = 3;

  private final Path dir = new Path("/" + getClass().getSimpleName());
  
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  private final Path file2 = new Path(sub1, "file2");

  private final Path sub11 = new Path(sub1, "sub11");
  private final Path file3 = new Path(sub11, "file3");
  private final Path file5 = new Path(sub1, "z_file5");

  private final Path sub2 = new Path(dir, "sub2");
  private final Path file6 = new Path(sub2, "file6");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem fsn;
  private FSDirectory fsdir;

  private DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();
    
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    
    hdfs = cluster.getFileSystem();
    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, seed);

    DFSTestUtil.createFile(hdfs, file5, 1024, REPLICATION, seed);
    hdfs.mkdirs(sub2);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Dump the tree, make some changes, and then dump the tree again. */
  @Test
  public void testDumpTree() throws Exception {
    final INode root = fsdir.getINode("/");

    LOG.info("Original tree");
    final StringBuffer b1 = root.dumpTreeRecursively();
    System.out.println("b1=" + b1);

    final BufferedReader in = new BufferedReader(new StringReader(b1.toString()));
    
    String line = in.readLine();
    checkClassName(line);

    for(; (line = in.readLine()) != null; ) {
      line = line.trim();
      if (!line.isEmpty() && !line.contains("snapshot")) {
        Assert.assertTrue("line=" + line,
            line.startsWith(INodeDirectory.DUMPTREE_LAST_ITEM)
            || line.startsWith(INodeDirectory.DUMPTREE_EXCEPT_LAST_ITEM));
        checkClassName(line);
      }
    }
  }
  
  @Test
  public void testReset() throws Exception {
    fsdir.reset();
    Assert.assertFalse(fsdir.isReady());
    final INodeDirectory root = (INodeDirectory) fsdir.getINode("/");
    Assert.assertTrue(root.getChildrenList(Snapshot.CURRENT_STATE_ID).isEmpty());
    fsdir.imageLoadComplete();
    Assert.assertTrue(fsdir.isReady());
  }

  @Test
  public void testSkipQuotaCheck() throws Exception {
    try {
      // set quota. nsQuota of 1 means no files can be created
      //  under this directory.
      hdfs.setQuota(sub2, 1, Long.MAX_VALUE);

      // create a file
      try {
        // this should fail
        DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);
        throw new IOException("The create should have failed.");
      } catch (NSQuotaExceededException qe) {
        // ignored
      }
      // disable the quota check and retry. this should succeed.
      fsdir.disableQuotaChecks();
      DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);

      // trying again after re-enabling the check.
      hdfs.delete(file6, false); // cleanup
      fsdir.enableQuotaChecks();
      try {
        // this should fail
        DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);
        throw new IOException("The create should have failed.");
      } catch (NSQuotaExceededException qe) {
        // ignored
      }
    } finally {
      hdfs.delete(file6, false); // cleanup, in case the test failed in the middle.
      hdfs.setQuota(sub2, Long.MAX_VALUE, Long.MAX_VALUE);
    }
  }
  
  static void checkClassName(String line) {
    int i = line.lastIndexOf('(');
    int j = line.lastIndexOf('@');
    final String classname = line.substring(i+1, j);
    Assert.assertTrue(classname.startsWith(INodeFile.class.getSimpleName())
        || classname.startsWith(INodeDirectory.class.getSimpleName()));
  }
}
