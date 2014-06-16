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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;


import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ToolRunner;

public class TestSnapshotFileLength {

  private static final long SEED = 0;
  private static final short REPLICATION = 1;
  private static final int BLOCKSIZE = 1024;

  private static final Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem hdfs;

  private final Path dir = new Path("/TestSnapshotFileLength");
  private final Path sub = new Path(dir, "sub1");
  private final String file1Name = "file1";
  private final String snapshot1 = "snapshot1";

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
                                              .build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }


  /**
   * Test that we cannot read a file beyond its snapshot length
   * when accessing it via a snapshot path.
   *
   */
  @Test (timeout=300000)
  public void testSnapshotfileLength() throws Exception {
    hdfs.mkdirs(sub);

    int bytesRead;
    byte[] buffer = new byte[BLOCKSIZE * 8];
    FSDataInputStream fis = null;
    FileStatus fileStatus = null;

    // Create and write a file.
    Path file1 = new Path(sub, file1Name);
    DFSTestUtil.createFile(hdfs, file1, 0, REPLICATION, SEED);
    DFSTestUtil.appendFile(hdfs, file1, BLOCKSIZE);

    // Create a snapshot on the parent directory.
    hdfs.allowSnapshot(sub);
    hdfs.createSnapshot(sub, snapshot1);

    // Write more data to the file.
    DFSTestUtil.appendFile(hdfs, file1, BLOCKSIZE);

    // Make sure we can read the entire file via its non-snapshot path.
    fileStatus = hdfs.getFileStatus(file1);
    assertThat(fileStatus.getLen(), is((long) BLOCKSIZE * 2));
    fis = hdfs.open(file1);
    bytesRead = fis.read(0, buffer, 0, buffer.length);
    assertThat(bytesRead, is(BLOCKSIZE * 2));
    fis.close();

    // Try to open the file via its snapshot path.
    Path file1snap1 =
        SnapshotTestHelper.getSnapshotPath(sub, snapshot1, file1Name);
    fis = hdfs.open(file1snap1);
    fileStatus = hdfs.getFileStatus(file1snap1);
    assertThat(fileStatus.getLen(), is((long) BLOCKSIZE));

    // Make sure we can only read up to the snapshot length.
    bytesRead = fis.read(0, buffer, 0, buffer.length);
    assertThat(bytesRead, is(BLOCKSIZE));
    fis.close();
  }

  /**
   * Adding as part of jira HDFS-5343
   * Test for checking the cat command on snapshot path it
   *  cannot read a file beyond snapshot file length
   * @throws Exception
   */
  @Test (timeout = 600000)
  public void testSnapshotFileLengthWithCatCommand() throws Exception {

    FSDataInputStream fis = null;
    FileStatus fileStatus = null;

    int bytesRead;
    byte[] buffer = new byte[BLOCKSIZE * 8];

    hdfs.mkdirs(sub);
    Path file1 = new Path(sub, file1Name);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, SEED);

    hdfs.allowSnapshot(sub);
    hdfs.createSnapshot(sub, snapshot1);

    DFSTestUtil.appendFile(hdfs, file1, BLOCKSIZE);

    // Make sure we can read the entire file via its non-snapshot path.
    fileStatus = hdfs.getFileStatus(file1);
    assertEquals("Unexpected file length", BLOCKSIZE * 2, fileStatus.getLen());
    fis = hdfs.open(file1);
    bytesRead = fis.read(buffer, 0, buffer.length);
    assertEquals("Unexpected # bytes read", BLOCKSIZE * 2, bytesRead);
    fis.close();

    Path file1snap1 =
        SnapshotTestHelper.getSnapshotPath(sub, snapshot1, file1Name);
    fis = hdfs.open(file1snap1);
    fileStatus = hdfs.getFileStatus(file1snap1);
    assertEquals(fileStatus.getLen(), BLOCKSIZE);
    // Make sure we can only read up to the snapshot length.
    bytesRead = fis.read(buffer, 0, buffer.length);
    assertEquals("Unexpected # bytes read", BLOCKSIZE, bytesRead);
    fis.close();

    PrintStream outBackup = System.out;
    PrintStream errBackup = System.err;
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    System.setOut(new PrintStream(bao));
    System.setErr(new PrintStream(bao));
    // Make sure we can cat the file upto to snapshot length
    FsShell shell = new FsShell();
    try {
      ToolRunner.run(conf, shell, new String[] { "-cat",
      "/TestSnapshotFileLength/sub1/.snapshot/snapshot1/file1" });
      assertEquals("Unexpected # bytes from -cat", BLOCKSIZE, bao.size());
    } finally {
      System.setOut(outBackup);
      System.setErr(errBackup);
    }
  }
}
