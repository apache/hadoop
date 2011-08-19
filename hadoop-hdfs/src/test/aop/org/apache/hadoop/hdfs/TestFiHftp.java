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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class TestFiHftp {
  final Log LOG = FileSystem.LOG;
  {
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
  }

  static final short DATANODE_NUM = 1;
  static final Random ran = new Random();
  static final byte[] buffer = new byte[1 << 16];
  static final MessageDigest md5;
  static {
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] createFile(FileSystem fs, Path name, long length, 
      short replication, long blocksize) throws IOException {
    final FSDataOutputStream out = fs.create(name, false, 4096,
        replication, blocksize);
    try {
      for(long n = length; n > 0; ) {
        ran.nextBytes(buffer);
        final int w = n < buffer.length? (int)n: buffer.length;
        out.write(buffer, 0, w);
        md5.update(buffer, 0, w);
        n -= w;
      }
    } finally {
      IOUtils.closeStream(out);
    }
    return md5.digest();
  }

  @Test
  public void testHftpOpen() throws IOException {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
      cluster.waitActive();

      //test with a file
      //which is larger than the servlet response buffer size
      {
        final long blocksize = 1L << 20; //  
        final long filesize = 2*blocksize + 100;
        runTestHftpOpen(cluster, "/foo", blocksize, filesize);
      }

      //test with a small file
      //which is smaller than the servlet response buffer size
      { 
        final long blocksize = 1L << 10; //  
        final long filesize = 2*blocksize + 100;
        runTestHftpOpen(cluster, "/small", blocksize, filesize);
      }
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * A test with a 3GB file.
   * It may take ~6 minutes.
   */
  void largeFileTest(final MiniDFSCluster cluster) throws IOException {
    final long blocksize = 128L << 20;  
    final long filesize = 3L << 30;
    runTestHftpOpen(cluster, "/large", blocksize, filesize);
  }

  /**
   * @param blocksize
   * @param filesize must be > block size 
   */
  private void runTestHftpOpen(final MiniDFSCluster cluster, final String file,
      final long blocksize, final long filesize) throws IOException {
    //create a file
    final DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
    final Path filepath = new Path(file);
    final byte[] filemd5 = createFile(dfs, filepath, filesize, DATANODE_NUM,
        blocksize);
    DFSTestUtil.waitReplication(dfs, filepath, DATANODE_NUM);

    //test hftp open and read
    final HftpFileSystem hftpfs = cluster.getHftpFileSystem(0);
    {
      final FSDataInputStream in = hftpfs.open(filepath);
      long bytesRead = 0;
      try {
        for(int r; (r = in.read(buffer)) != -1; ) {
          bytesRead += r;
          md5.update(buffer, 0, r);
        }
      } finally {
        LOG.info("bytesRead=" + bytesRead);
        in.close();
      }
      Assert.assertEquals(filesize, bytesRead);
      Assert.assertArrayEquals(filemd5, md5.digest());
    }

    //delete the second block
    final DFSClient client = dfs.getClient();
    final LocatedBlocks locatedblocks = client.getNamenode().getBlockLocations(
        file, 0, filesize);
    Assert.assertEquals((filesize - 1)/blocksize + 1,
        locatedblocks.locatedBlockCount());
    final LocatedBlock lb = locatedblocks.get(1);
    final ExtendedBlock blk = lb.getBlock();
    Assert.assertEquals(blocksize, lb.getBlockSize());
    final DatanodeInfo[] datanodeinfos = lb.getLocations();
    Assert.assertEquals(DATANODE_NUM, datanodeinfos.length);
    final DataNode dn = cluster.getDataNode(datanodeinfos[0].getIpcPort());
    LOG.info("dn=" + dn + ", blk=" + blk + " (length=" + blk.getNumBytes() + ")");
    final FSDataset data = (FSDataset)dn.getFSDataset();
    final File blkfile = data.getBlockFile(blk);
    Assert.assertTrue(blkfile.delete());

    //read again by hftp, should get an exception 
    LOG.info("hftpfs.getUri() = " + hftpfs.getUri());
    final ContentSummary cs = hftpfs.getContentSummary(filepath);
    LOG.info("hftpfs.getContentSummary = " + cs);
    Assert.assertEquals(filesize, cs.getLength());

    final FSDataInputStream in = hftpfs.open(hftpfs.makeQualified(filepath));
    long bytesRead = 0;
    try {
      for(int r; (r = in.read(buffer)) != -1; ) {
        bytesRead += r;
      }
      Assert.fail();
    } catch(IOException ioe) {
      LOG.info("GOOD: get an exception", ioe);
    } finally {
      LOG.info("bytesRead=" + bytesRead);
      in.close();
    }
  }
}
