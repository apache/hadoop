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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil.ShortCircuitTestContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class TestRead {
  static final private int BLOCK_SIZE = 512;
  static final long SEED = 0xDEADBEEFL;
  static final int FILE_SIZE = BLOCK_SIZE * 10;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRead.class.getName());
  private final GenericTestUtils.LogCapturer dfsClientLog =
      GenericTestUtils.LogCapturer.captureLogs(DFSClient.LOG);

  @BeforeClass
  public static void setLogLevel() {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.WARN);
  }

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

  /**
   * Test logging in readBuffer() when the number of IOExceptions can be recovered by retrying on
   * a different datanode or by refreshing data nodes and retrying each data node one more time.
   */
  @Test(timeout=120000)
  public void testReadBufferIOExceptionLogging() throws IOException {
    testReadBufferIOExceptionLogging(0, 0);
    testReadBufferIOExceptionLogging(1, 0);
    testReadBufferIOExceptionLogging(2, 0);
    testReadBufferIOExceptionLogging(3, 0);
    testReadBufferIOExceptionLogging(4, 1);
    testReadBufferIOExceptionLogging(5, 1);
    testReadBufferIOExceptionLogging(6, 1);
  }

  /**
   * @param ioExceptions number of IOExceptions to throw during a test.
   * @param maxBlockAcquireFailures number of refreshLocation we would perform once we mark
   *                                   all current data nodes as dead.
   */
  private void testReadBufferIOExceptionLogging(final int ioExceptions,
      int maxBlockAcquireFailures) throws IOException {
    dfsClientLog.clearOutput();
    Configuration conf = new Configuration();
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, maxBlockAcquireFailures);
    final int[] count = {0};
    // Set up the InjectionHandler
    DFSClientFaultInjector.set(Mockito.mock(DFSClientFaultInjector.class));
    DFSClientFaultInjector injector = DFSClientFaultInjector.get();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count[0] < ioExceptions) {
          LOG.info("-------------- throw IOException");
          count[0]++;
          throw new IOException("IOException test");
        }
        return null;
      }
    }).when(injector).fetchFromDatanodeException();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSClient dfsClient = fileSys.getClient();
    DFSInputStream input = null;
    Path file = new Path("/testfile.dat");

    try {
      DFSTestUtil.createFile(fileSys, file, FILE_SIZE, FILE_SIZE, BLOCK_SIZE, (short) 3, SEED);

      byte[] buffer = new byte[FILE_SIZE];
      input = dfsClient.open(file.toString());
      input.read(buffer, 0, FILE_SIZE);
      assertEquals(ioExceptions, StringUtils.countMatches(dfsClientLog.getOutput(),
          "Retry with the current or next available datanode."));
    } finally {
      Mockito.reset(injector);
      IOUtils.cleanupWithLogger(LOG, input);
      fileSys.close();
      cluster.shutdown();
      dfsClientLog.clearOutput();
    }
  }

  /**
   * Test the case where we always hit IOExceptions, causing the read request to fail.
   */
  @Test(timeout=60000)
  public void testReadBufferIOExceptionLoggingFailedRequest() throws IOException {
    testReadBufferIOExceptionLoggingFailedRequest(0);
    testReadBufferIOExceptionLoggingFailedRequest(1);
  }

  private void testReadBufferIOExceptionLoggingFailedRequest(int maxBlockAcquireFailures)
      throws IOException {
    dfsClientLog.clearOutput();
    Configuration conf = new Configuration();
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, maxBlockAcquireFailures);
    // Set up the InjectionHandler
    DFSClientFaultInjector.set(Mockito.mock(DFSClientFaultInjector.class));
    DFSClientFaultInjector injector = DFSClientFaultInjector.get();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("-------------- throw IOException");
        throw new IOException("IOException test");
      }
    }).when(injector).fetchFromDatanodeException();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSClient dfsClient = fileSys.getClient();
    DFSInputStream input = null;
    Path file = new Path("/testfile.dat");

    try {
      DFSTestUtil.createFile(fileSys, file, FILE_SIZE, FILE_SIZE, BLOCK_SIZE, (short) 3, SEED);

      byte[] buffer = new byte[FILE_SIZE];
      input = dfsClient.open(file.toString());
      input.read(buffer, 0, FILE_SIZE);
      fail();
    } catch (BlockMissingException e) {
      // Logging from readWithStrategy()
      assertEquals(1, StringUtils.countMatches(dfsClientLog.getOutput(),
          "Failed to read from all available datanodes for file"));
      assertEquals(1 + 3L * (maxBlockAcquireFailures + 1),
          StringUtils.countMatches(dfsClientLog.getOutput(),
              "Exception when fetching file /testfile.dat at position="));
      // Logging from actualGetFromOneDataNode
      assertEquals(1 + 3L * (maxBlockAcquireFailures + 1),
          StringUtils.countMatches(dfsClientLog.getOutput(),
              "Retry with the current or next available datanode."));
    } finally {
      Mockito.reset(injector);
      IOUtils.cleanupWithLogger(LOG, input);
      fileSys.close();
      cluster.shutdown();
      dfsClientLog.clearOutput();
    }
  }
}
