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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class tests the DFS positional read functionality in a single node
 * mini-cluster.
 */
public class TestPread {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int numBlocksPerFile = 12;
  static final int fileSize = numBlocksPerFile * blockSize;
  boolean simulatedStorage;
  boolean isHedgedRead;

  @Before
  public void setup() {
    simulatedStorage = false;
    isHedgedRead = false;
  }

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    int replication = 3;// We need > 1 blocks to test out the hedged reads.
    // test empty file open and read
    DFSTestUtil.createFile(fileSys, name, fileSize, 0,
      blockSize, (short)replication, seed);
    FSDataInputStream in = fileSys.open(name);
    byte[] buffer = new byte[fileSize];
    in.readFully(0, buffer, 0, 0);
    IOException res = null;
    try { // read beyond the end of the file
      in.readFully(0, buffer, 0, 1);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);
    in.close();
    if (!fileSys.delete(name, true))
      assertTrue("Cannot delete file", false);
    
    // now create the real file
    DFSTestUtil.createFile(fileSys, name, fileSize, fileSize,
        blockSize, (short) replication, seed);
  }
  
  private void checkAndEraseData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }
  
  private void doPread(FSDataInputStream stm, long position, byte[] buffer,
                       int offset, int length) throws IOException {
    int nread = 0;
    long totalRead = 0;
    DFSInputStream dfstm = null;

    if (stm.getWrappedStream() instanceof DFSInputStream) {
      dfstm = (DFSInputStream) (stm.getWrappedStream());
      totalRead = dfstm.getReadStatistics().getTotalBytesRead();
    }

    while (nread < length) {
      int nbytes =
          stm.read(position + nread, buffer, offset + nread, length - nread);
      assertTrue("Error in pread", nbytes > 0);
      nread += nbytes;
    }

    if (dfstm != null) {
      if (isHedgedRead) {
        assertTrue("Expected read statistic to be incremented", length <= dfstm
            .getReadStatistics().getTotalBytesRead() - totalRead);
      } else {
        assertEquals("Expected read statistic to be incremented", length, dfstm
            .getReadStatistics().getTotalBytesRead() - totalRead);
      }
    }
  }
  
  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[fileSize];
    if (simulatedStorage) {
      assert fileSys instanceof DistributedFileSystem;
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      LocatedBlocks lbs = dfs.getClient().getLocatedBlocks(name.toString(),
          0, fileSize);
      DFSTestUtil.fillExpectedBuf(lbs, expected);
    } else {
      Random rand = new Random(seed);
      rand.nextBytes(expected);
    }
    // do a sanity check. Read first 4K bytes
    byte[] actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    // now do a pread for the first 8K bytes
    actual = new byte[8192];
    doPread(stm, 0L, actual, 0, 8192);
    checkAndEraseData(actual, 0, expected, "Pread Test 1");
    // Now check to see if the normal read returns 4K-8K byte range
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 4096, expected, "Pread Test 2");
    // Now see if we can cross a single block boundary successfully
    // read 4K bytes from blockSize - 2K offset
    stm.readFully(blockSize - 2048, actual, 0, 4096);
    checkAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 3");
    // now see if we can cross two block boundaries successfully
    // read blockSize + 4K bytes from blockSize - 2K offset
    actual = new byte[blockSize + 4096];
    stm.readFully(blockSize - 2048, actual);
    checkAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 4");
    // now see if we can cross two block boundaries that are not cached
    // read blockSize + 4K bytes from 10*blockSize - 2K offset
    actual = new byte[blockSize + 4096];
    stm.readFully(10 * blockSize - 2048, actual);
    checkAndEraseData(actual, (10 * blockSize - 2048), expected, "Pread Test 5");
    // now check that even after all these preads, we can still read
    // bytes 8K-12K
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 8192, expected, "Pread Test 6");
    // done
    stm.close();
    // check block location caching
    stm = fileSys.open(name);
    stm.readFully(1, actual, 0, 4096);
    stm.readFully(4*blockSize, actual, 0, 4096);
    stm.readFully(7*blockSize, actual, 0, 4096);
    actual = new byte[3*4096];
    stm.readFully(0*blockSize, actual, 0, 3*4096);
    checkAndEraseData(actual, 0, expected, "Pread Test 7");
    actual = new byte[8*4096];
    stm.readFully(3*blockSize, actual, 0, 8*4096);
    checkAndEraseData(actual, 3*blockSize, expected, "Pread Test 8");
    // read the tail
    stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize/2);
    IOException res = null;
    try { // read beyond the end of the file
      stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);
    
    stm.close();
  }
    
  // test pread can survive datanode restarts
  private void datanodeRestartTest(MiniDFSCluster cluster, FileSystem fileSys,
      Path name) throws IOException {
    // skip this test if using simulated storage since simulated blocks
    // don't survive datanode restarts.
    if (simulatedStorage) {
      return;
    }
    int numBlocks = 1;
    assertTrue(numBlocks <= HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
    byte[] expected = new byte[numBlocks * blockSize];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    byte[] actual = new byte[numBlocks * blockSize];
    FSDataInputStream stm = fileSys.open(name);
    // read a block and get block locations cached as a result
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Pread Datanode Restart Setup");
    // restart all datanodes. it is expected that they will
    // restart on different ports, hence, cached block locations
    // will no longer work.
    assertTrue(cluster.restartDataNodes());
    cluster.waitActive();
    // verify the block can be read again using the same InputStream 
    // (via re-fetching of block locations from namenode). there is a 
    // 3 sec sleep in chooseDataNode(), which can be shortened for 
    // this test if configurable.
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Pread Datanode Restart Test");
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    assertTrue(fileSys.delete(name, true));
    assertTrue(!fileSys.exists(name));
  }

  private Callable<Void> getPReadFileCallable(final FileSystem fileSys,
      final Path file) {
    return new Callable<Void>() {
      public Void call() throws IOException {
        pReadFile(fileSys, file);
        return null;
      }
    };
  }

  /**
   * Tests positional read in DFS.
   */
  @Test
  public void testPreadDFS() throws IOException {
    Configuration conf = new Configuration();
    dfsPreadTest(conf, false, true); // normal pread
    dfsPreadTest(conf, true, true); // trigger read code path without
                                    // transferTo.
  }
  
  @Test
  public void testPreadDFSNoChecksum() throws IOException {
    Configuration conf = new Configuration();
    GenericTestUtils.setLogLevel(DataTransferProtocol.LOG, Level.ALL);
    dfsPreadTest(conf, false, false);
    dfsPreadTest(conf, true, false);
  }
  
  /**
   * Tests positional read in DFS, with hedged reads enabled.
   */
  @Test
  public void testHedgedPreadDFSBasic() throws IOException {
    isHedgedRead = true;
    Configuration conf = new Configuration();
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, 5);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 1);
    dfsPreadTest(conf, false, true); // normal pread
    dfsPreadTest(conf, true, true); // trigger read code path without
                                    // transferTo.
  }

  @Test
  public void testHedgedReadLoopTooManyTimes() throws IOException {
    Configuration conf = new Configuration();
    int numHedgedReadPoolThreads = 5;
    final int hedgedReadTimeoutMillis = 50;

    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        numHedgedReadPoolThreads);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        hedgedReadTimeoutMillis);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    // Set up the InjectionHandler
    DFSClientFaultInjector.set(Mockito.mock(DFSClientFaultInjector.class));
    DFSClientFaultInjector injector = DFSClientFaultInjector.get();
    final int sleepMs = 100;
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (true) {
          Thread.sleep(hedgedReadTimeoutMillis + sleepMs);
          if (DFSClientFaultInjector.exceptionNum.compareAndSet(0, 1)) {
            System.out.println("-------------- throw Checksum Exception");
            throw new ChecksumException("ChecksumException test", 100);
          }
        }
        return null;
      }
    }).when(injector).fetchFromDatanodeException();
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (true) {
          Thread.sleep(sleepMs * 2);
        }
        return null;
      }
    }).when(injector).readFromDatanodeDelay();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(true).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSClient dfsClient = fileSys.getClient();
    FSDataOutputStream output = null;
    DFSInputStream input = null;
    String filename = "/hedgedReadMaxOut.dat";
    try {
      
      Path file = new Path(filename);
      output = fileSys.create(file, (short) 2);
      byte[] data = new byte[64 * 1024];
      output.write(data);
      output.flush();
      output.write(data);
      output.flush();
      output.write(data);
      output.flush();
      output.close();
      byte[] buffer = new byte[64 * 1024];
      input = dfsClient.open(filename);
      input.read(0, buffer, 0, 1024);
      input.close();
      assertEquals(3, input.getHedgedReadOpsLoopNumForTesting());
    } catch (BlockMissingException e) {
      assertTrue(false);
    } finally {
      Mockito.reset(injector);
      IOUtils.cleanup(null, input);
      IOUtils.cleanup(null, output);
      fileSys.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testMaxOutHedgedReadPool() throws IOException,
      InterruptedException, ExecutionException {
    isHedgedRead = true;
    Configuration conf = new Configuration();
    int numHedgedReadPoolThreads = 5;
    final int initialHedgedReadTimeoutMillis = 50000;
    final int fixedSleepIntervalMillis = 50;
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        numHedgedReadPoolThreads);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        initialHedgedReadTimeoutMillis);

    // Set up the InjectionHandler
    DFSClientFaultInjector.set(Mockito.mock(DFSClientFaultInjector.class));
    DFSClientFaultInjector injector = DFSClientFaultInjector.get();
    // make preads sleep for 50ms
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(fixedSleepIntervalMillis);
        return null;
      }
    }).when(injector).startFetchFromDatanode();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .format(true).build();
    DistributedFileSystem fileSys = cluster.getFileSystem();
    DFSClient dfsClient = fileSys.getClient();
    DFSHedgedReadMetrics metrics = dfsClient.getHedgedReadMetrics();
    // Metrics instance is static, so we need to reset counts from prior tests.
    metrics.hedgedReadOps.set(0);
    metrics.hedgedReadOpsWin.set(0);
    metrics.hedgedReadOpsInCurThread.set(0);

    try {
      Path file1 = new Path("hedgedReadMaxOut.dat");
      writeFile(fileSys, file1);
      // Basic test. Reads complete within timeout. Assert that there were no
      // hedged reads.
      pReadFile(fileSys, file1);
      // assert that there were no hedged reads. 50ms + delta < 500ms
      assertTrue(metrics.getHedgedReadOps() == 0);
      assertTrue(metrics.getHedgedReadOpsInCurThread() == 0);
      /*
       * Reads take longer than timeout. But, only one thread reading. Assert
       * that there were hedged reads. But, none of the reads had to run in the
       * current thread.
       */
      {
        Configuration conf2 =  new Configuration(cluster.getConfiguration(0));
        conf2.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf2.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, 50);
        fileSys.close();
        fileSys = (DistributedFileSystem)FileSystem.get(cluster.getURI(0), conf2);
        metrics = fileSys.getClient().getHedgedReadMetrics();
      }
      pReadFile(fileSys, file1);
      // assert that there were hedged reads
      assertTrue(metrics.getHedgedReadOps() > 0);
      assertTrue(metrics.getHedgedReadOpsInCurThread() == 0);
      /*
       * Multiple threads reading. Reads take longer than timeout. Assert that
       * there were hedged reads. And that reads had to run in the current
       * thread.
       */
      int factor = 10;
      int numHedgedReads = numHedgedReadPoolThreads * factor;
      long initialReadOpsValue = metrics.getHedgedReadOps();
      ExecutorService executor = Executors.newFixedThreadPool(numHedgedReads);
      ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
      for (int i = 0; i < numHedgedReads; i++) {
        futures.add(executor.submit(getPReadFileCallable(fileSys, file1)));
      }
      for (int i = 0; i < numHedgedReads; i++) {
        futures.get(i).get();
      }
      assertTrue(metrics.getHedgedReadOps() > initialReadOpsValue);
      assertTrue(metrics.getHedgedReadOpsInCurThread() > 0);
      cleanupFile(fileSys, file1);
      executor.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
      Mockito.reset(injector);
    }
  }

  private void dfsPreadTest(Configuration conf, boolean disableTransferTo, boolean verifyChecksum)
      throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.setLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY, 4096);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 0);
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    if (disableTransferTo) {
      conf.setBoolean("dfs.datanode.transferTo.allowed", false);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    FileSystem fileSys = cluster.getFileSystem();
    fileSys.setVerifyChecksum(verifyChecksum);
    try {
      Path file1 = new Path("/preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      datanodeRestartTest(cluster, fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  @Test
  public void testPreadDFSSimulated() throws IOException {
    simulatedStorage = true;
    testPreadDFS();
  }
  
  /**
   * Tests positional read in LocalFS.
   */
  @Test
  public void testPreadLocalFS() throws IOException {
    Configuration conf = new HdfsConfiguration();
    FileSystem fileSys = FileSystem.getLocal(conf);
    try {
      Path file1 = new Path(GenericTestUtils.getTempPath("preadtest.dat"));
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestPread().testPreadDFS();
  }
}
