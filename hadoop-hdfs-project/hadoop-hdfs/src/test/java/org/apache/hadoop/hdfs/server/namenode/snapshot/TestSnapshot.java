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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Test snapshot related operations. */
public class TestSnapshot {
  private static final long seed = 0;
  private static final short REPLICATION = 3;

  private final Path dir = new Path("/TestSnapshot");
  
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  private final Path file2 = new Path(sub1, "file2");

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
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Test allow-snapshot operation. */
  @Test
  public void testAllowSnapshot() throws Exception {
    final String path = sub1.toString();
    final INode before = fsdir.getINode(path);
    Assert.assertTrue(before instanceof INodeDirectory);
    Assert.assertFalse(before instanceof INodeDirectorySnapshottable);

    hdfs.allowSnapshot(path);
    final INode after = fsdir.getINode(path);
    Assert.assertTrue(after instanceof INodeDirectorySnapshottable);
  }
}
