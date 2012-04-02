package org.apache.hadoop.hdfs;


import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestFileAppendRestart {
  private static final int BLOCK_SIZE = 4096;

  private void writeAndAppend(FileSystem fs, Path p,
      int lengthForCreate, int lengthForAppend) throws IOException {
    // Creating a file with 4096 blockSize to write multiple blocks
    FSDataOutputStream stream = fs.create(
        p, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
    try {
      AppendTestUtil.write(stream, 0, lengthForCreate);
      stream.close();

      stream = fs.append(p);
      AppendTestUtil.write(stream, lengthForCreate, lengthForAppend);
      stream.close();
    } finally {
      IOUtils.closeStream(stream);
    }

    int totalLength = lengthForCreate + lengthForAppend; 
    assertEquals(totalLength, fs.getFileStatus(p).getLen());
  }

  /**
   * Regression test for HDFS-2991. Creates and appends to files
   * where blocks start/end on block boundaries.
   */
  @Test
  public void testAppendRestart() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();

      Path p1 = new Path("/block-boundaries");
      writeAndAppend(fs, p1, BLOCK_SIZE, BLOCK_SIZE);

      Path p2 = new Path("/not-block-boundaries");
      writeAndAppend(fs, p2, BLOCK_SIZE/2, BLOCK_SIZE);

      cluster.restartNameNode();

      AppendTestUtil.check(fs, p1, 2*BLOCK_SIZE);
      AppendTestUtil.check(fs, p2, 3*BLOCK_SIZE/2);
    } finally {
      IOUtils.closeStream(stream);
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
