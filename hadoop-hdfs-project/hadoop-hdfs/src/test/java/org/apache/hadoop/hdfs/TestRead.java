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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil.ShortCircuitTestContext;
import org.junit.Test;

public class TestRead {
  final private int BLOCK_SIZE = 512;

  private void testEOF(MiniDFSCluster cluster, int fileLength) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("testEOF." + fileLength);
    DFSTestUtil.createFile(fs, path, fileLength, (short)1, 0xBEEFBEEF);
    FSDataInputStream fis = fs.open(path);
    ByteBuffer empty = ByteBuffer.allocate(0);
    // A read into an empty bytebuffer at the beginning of the file gives 0.
    Assert.assertEquals(0, fis.read(empty));
    fis.seek(fileLength);
    // A read into an empty bytebuffer at the end of the file gives -1.
    Assert.assertEquals(-1, fis.read(empty));
    if (fileLength > BLOCK_SIZE) {
      fis.seek(fileLength - BLOCK_SIZE + 1);
      ByteBuffer dbb = ByteBuffer.allocateDirect(BLOCK_SIZE);
      Assert.assertEquals(BLOCK_SIZE - 1, fis.read(dbb));
    }
    fis.close();
  }

  @Test(timeout=60000)
  public void testEOFWithBlockReaderLocal() throws Exception {
    ShortCircuitTestContext testContext = 
        new ShortCircuitTestContext("testEOFWithBlockReaderLocal");
    try {
      final Configuration conf = testContext.newConfiguration();
      conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD, BLOCK_SIZE);
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .format(true).build();
      testEOF(cluster, 1);
      testEOF(cluster, 14);
      testEOF(cluster, 10000);
      cluster.shutdown();
    } finally {
      testContext.close();
    }
  }

  @Test(timeout=60000)
  public void testEOFWithRemoteBlockReader() throws Exception {
    final Configuration conf = new Configuration();
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_CACHE_READAHEAD, BLOCK_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(true).build();
    testEOF(cluster, 1);
    testEOF(cluster, 14);
    testEOF(cluster, 10000);   
    cluster.shutdown();
  }

  /**
   * Regression test for HDFS-7045.
   * If deadlock happen, the test will time out.
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testReadReservedPath() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(1).format(true).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      fs.open(new Path("/.reserved/.inodes/file"));
      Assert.fail("Open a non existing file should fail.");
    } catch (FileNotFoundException e) {
      // Expected
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testInterruptReader() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
        DelayedSimulatedFSDataset.Factory.class.getName());

    final MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(1).build();
    final DistributedFileSystem fs = cluster.getFileSystem();
    try {
      cluster.waitActive();
      final Path file = new Path("/foo");
      DFSTestUtil.createFile(fs, file, 1024, (short) 1, 0L);

      final FSDataInputStream in = fs.open(file);
      AtomicBoolean readInterrupted = new AtomicBoolean(false);
      final Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            in.read(new byte[1024], 0, 1024);
          } catch (IOException e) {
            if (e instanceof ClosedByInterruptException ||
                e instanceof InterruptedIOException) {
              readInterrupted.set(true);
            }
          }
        }
      });

      reader.start();
      Thread.sleep(1000);
      reader.interrupt();
      reader.join();

      Assert.assertTrue(readInterrupted.get());
    } finally {
      cluster.shutdown();
    }
  }

  private static class DelayedSimulatedFSDataset extends SimulatedFSDataset {
    private volatile boolean isDelayed = true;

    DelayedSimulatedFSDataset(DataNode datanode, DataStorage storage,
        Configuration conf) {
      super(datanode, storage, conf);
    }

    @Override
    public synchronized InputStream getBlockInputStream(ExtendedBlock b,
        long seekOffset) throws IOException {
      while (isDelayed) {
        try {
          this.wait();
        } catch (InterruptedException ignored) {
        }
      }
      InputStream result = super.getBlockInputStream(b);
      IOUtils.skipFully(result, seekOffset);
      return result;
    }

    static class Factory extends FsDatasetSpi.Factory<DelayedSimulatedFSDataset> {
      @Override
      public DelayedSimulatedFSDataset newInstance(DataNode datanode,
          DataStorage storage, Configuration conf) throws IOException {
        return new DelayedSimulatedFSDataset(datanode, storage, conf);
      }

      @Override
      public boolean isSimulated() {
        return true;
      }
    }
  }
}
