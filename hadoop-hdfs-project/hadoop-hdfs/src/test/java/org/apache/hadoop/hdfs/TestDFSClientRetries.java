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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.LeaseRenewer;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors.
 */
public class TestDFSClientRetries {
  private static final String ADDRESS = "0.0.0.0";
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;
  public static final Log LOG =
    LogFactory.getLog(TestDFSClientRetries.class.getName());
  static private Configuration conf = null;
 
 private static class TestServer extends Server {
    private boolean sleep;
    private Class<? extends Writable> responseClass;

    public TestServer(int handlerCount, boolean sleep) throws IOException {
      this(handlerCount, sleep, LongWritable.class, null);
    }

    public TestServer(int handlerCount, boolean sleep,
        Class<? extends Writable> paramClass,
        Class<? extends Writable> responseClass)
      throws IOException {
      super(ADDRESS, 0, paramClass, handlerCount, conf);
      this.sleep = sleep;
      this.responseClass = responseClass;
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        // sleep a bit
        try {
          Thread.sleep(PING_INTERVAL + MIN_SLEEP_TIME);
        } catch (InterruptedException e) {}
      }
      if (responseClass != null) {
        try {
          return responseClass.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        return param;                               // echo param as result
      }
    }
  }
 
  // writes 'len' bytes of data to out.
  private static void writeData(OutputStream out, int len) throws IOException {
    byte [] buf = new byte[4096*16];
    while(len > 0) {
      int toWrite = Math.min(len, buf.length);
      out.write(buf, 0, toWrite);
      len -= toWrite;
    }
  }
  
  @Before
  public void setupConf(){
    conf = new HdfsConfiguration();
  }
  
  /**
   * This makes sure that when DN closes clients socket after client had
   * successfully connected earlier, the data can still be fetched.
   */
  @Test
  public void testWriteTimeoutAtDataNode() throws IOException,
                                                  InterruptedException { 
    final int writeTimeout = 100; //milliseconds.
    // set a very short write timeout for datanode, so that tests runs fast.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, writeTimeout); 
    // set a smaller block size
    final int blockSize = 10*1024*1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
    // set a small buffer size
    final int bufferSize = 4096;
    conf.setInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, bufferSize);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
    
      Path filePath = new Path("/testWriteTimeoutAtDataNode");
      OutputStream out = fs.create(filePath, true, bufferSize);
    
      // write a 2 block file.
      writeData(out, 2*blockSize);
      out.close();
      
      byte[] buf = new byte[1024*1024]; // enough to empty TCP buffers.
      
      InputStream in = fs.open(filePath, bufferSize);
      
      //first read a few bytes
      IOUtils.readFully(in, buf, 0, bufferSize/2);
      //now read few more chunks of data by sleeping in between :
      for(int i=0; i<10; i++) {
        Thread.sleep(2*writeTimeout); // force write timeout at the datanode.
        // read enough to empty out socket buffers.
        IOUtils.readFully(in, buf, 0, buf.length); 
      }
      // successfully read with write timeout on datanodes.
      in.close();
    } finally {
      cluster.shutdown();
    }
  }
  
  // more tests related to different failure cases can be added here.

  /**
   * Verify that client will correctly give up after the specified number
   * of times trying to add a block
   */
  @SuppressWarnings({ "serial", "unchecked" })
  @Test
  public void testNotYetReplicatedErrors() throws IOException
  { 
    final String exceptionMsg = "Nope, not replicated yet...";
    final int maxRetries = 1; // Allow one retry (total of two calls)
    conf.setInt(DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY, maxRetries);
    
    NamenodeProtocols mockNN = mock(NamenodeProtocols.class);
    Answer<Object> answer = new ThrowsException(new IOException()) {
      int retryCount = 0;
      
      @Override
      public Object answer(InvocationOnMock invocation) 
                       throws Throwable {
        retryCount++;
        System.out.println("addBlock has been called "  + retryCount + " times");
        if(retryCount > maxRetries + 1) // First call was not a retry
          throw new IOException("Retried too many times: " + retryCount);
        else
          throw new RemoteException(NotReplicatedYetException.class.getName(),
                                    exceptionMsg);
      }
    };
    when(mockNN.addBlock(anyString(), 
                         anyString(),
                         any(ExtendedBlock.class),
                         any(DatanodeInfo[].class),
                         anyLong(), any(String[].class))).thenAnswer(answer);
    
    Mockito.doReturn(
            new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission(
                (short) 777), "owner", "group", new byte[0], new byte[0],
                1010, 0, null, (byte) 0)).when(mockNN).getFileInfo(anyString());
    
    Mockito.doReturn(
            new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission(
                (short) 777), "owner", "group", new byte[0], new byte[0],
                1010, 0, null, (byte) 0))
        .when(mockNN)
        .create(anyString(), (FsPermission) anyObject(), anyString(),
            (EnumSetWritable<CreateFlag>) anyObject(), anyBoolean(),
            anyShort(), anyLong(), (CryptoProtocolVersion[]) anyObject());

    final DFSClient client = new DFSClient(null, mockNN, conf, null);
    OutputStream os = client.create("testfile", true);
    os.write(20); // write one random byte
    
    try {
      os.close();
    } catch (Exception e) {
      assertTrue("Retries are not being stopped correctly: " + e.getMessage(),
           e.getMessage().equals(exceptionMsg));
    }
  }

  /**
   * This tests that DFSInputStream failures are counted for a given read
   * operation, and not over the lifetime of the stream. It is a regression
   * test for HDFS-127.
   */
  @Test
  public void testFailuresArePerOperation() throws Exception
  {
    long fileSize = 4096;
    Path file = new Path("/testFile");

    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    conf.setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, 2 * 1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      int maxBlockAcquires = client.getMaxBlockAcquireFailures();
      assertTrue(maxBlockAcquires > 0);


      DFSTestUtil.createFile(fs, file, fileSize, (short)1, 12345L /*seed*/);

      // If the client will retry maxBlockAcquires times, then if we fail
      // any more than that number of times, the operation should entirely
      // fail.
      doAnswer(new FailNTimesAnswer(preSpyNN, maxBlockAcquires + 1))
        .when(spyNN).getBlockLocations(anyString(), anyLong(), anyLong());
      try {
        IOUtils.copyBytes(client.open(file.toString()), new IOUtils.NullOutputStream(), conf,
                          true);
        fail("Didn't get exception");
      } catch (IOException ioe) {
        DFSClient.LOG.info("Got expected exception", ioe);
      }

      // If we fail exactly that many times, then it should succeed.
      doAnswer(new FailNTimesAnswer(preSpyNN, maxBlockAcquires))
        .when(spyNN).getBlockLocations(anyString(), anyLong(), anyLong());
      IOUtils.copyBytes(client.open(file.toString()), new IOUtils.NullOutputStream(), conf,
                        true);

      DFSClient.LOG.info("Starting test case for failure reset");

      // Now the tricky case - if we fail a few times on one read, then succeed,
      // then fail some more on another read, it shouldn't fail.
      doAnswer(new FailNTimesAnswer(preSpyNN, maxBlockAcquires))
        .when(spyNN).getBlockLocations(anyString(), anyLong(), anyLong());
      DFSInputStream is = client.open(file.toString());
      byte buf[] = new byte[10];
      IOUtils.readFully(is, buf, 0, buf.length);

      DFSClient.LOG.info("First read successful after some failures.");

      // Further reads at this point will succeed since it has the good block locations.
      // So, force the block locations on this stream to be refreshed from bad info.
      // When reading again, it should start from a fresh failure count, since
      // we're starting a new operation on the user level.
      doAnswer(new FailNTimesAnswer(preSpyNN, maxBlockAcquires))
        .when(spyNN).getBlockLocations(anyString(), anyLong(), anyLong());
      is.openInfo();
      // Seek to beginning forces a reopen of the BlockReader - otherwise it'll
      // just keep reading on the existing stream and the fact that we've poisoned
      // the block info won't do anything.
      is.seek(0);
      IOUtils.readFully(is, buf, 0, buf.length);

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test DFSClient can continue to function after renewLease RPC
   * receives SocketTimeoutException.
   */
  @Test
  public void testLeaseRenewSocketTimeout() throws Exception
  {
    String file1 = "/testFile1";
    String file2 = "/testFile2";
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    conf.setInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, 2 * 1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      NamenodeProtocols spyNN = spy(cluster.getNameNodeRpc());
      Mockito.doThrow(new SocketTimeoutException()).when(spyNN).renewLease(
          Mockito.anyString());
      DFSClient client = new DFSClient(null, spyNN, conf, null);
      // Get hold of the lease renewer instance used by the client
      LeaseRenewer leaseRenewer = client.getLeaseRenewer();
      leaseRenewer.setRenewalTime(100);
      OutputStream out1 = client.create(file1, false);

      Mockito.verify(spyNN, timeout(10000).times(1)).renewLease(
          Mockito.anyString());
      verifyEmptyLease(leaseRenewer);
      try {
        out1.write(new byte[256]);
        fail("existing output stream should be aborted");
      } catch (IOException e) {
      }

      // Verify DFSClient can do read operation after renewLease aborted.
      client.exists(file2);
      // Verify DFSClient can do write operation after renewLease no longer
      // throws SocketTimeoutException.
      Mockito.doNothing().when(spyNN).renewLease(
          Mockito.anyString());
      leaseRenewer = client.getLeaseRenewer();
      leaseRenewer.setRenewalTime(100);
      OutputStream out2 = client.create(file2, false);
      Mockito.verify(spyNN, timeout(10000).times(2)).renewLease(
          Mockito.anyString());
      out2.write(new byte[256]);
      out2.close();
      verifyEmptyLease(leaseRenewer);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test that getAdditionalBlock() and close() are idempotent. This allows
   * a client to safely retry a call and still produce a correct
   * file. See HDFS-3031.
   */
  @Test
  public void testIdempotentAllocateBlockAndClose() throws Exception {
    final String src = "/testIdempotentAllocateBlock";
    Path file = new Path(src);

    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NamenodeProtocols preSpyNN = cluster.getNameNodeRpc();
      NamenodeProtocols spyNN = spy(preSpyNN);
      DFSClient client = new DFSClient(null, spyNN, conf, null);

      
      // Make the call to addBlock() get called twice, as if it were retried
      // due to an IPC issue.
      doAnswer(new Answer<LocatedBlock>() {
        private int getBlockCount(LocatedBlock ret) throws IOException {
          LocatedBlocks lb = cluster.getNameNodeRpc().getBlockLocations(src, 0, Long.MAX_VALUE);
          assertEquals(lb.getLastLocatedBlock().getBlock(), ret.getBlock());
          return lb.getLocatedBlocks().size();
        }

        @Override
        public LocatedBlock answer(InvocationOnMock invocation) throws Throwable {
          LOG.info("Called addBlock: "
              + Arrays.toString(invocation.getArguments()));

          // call first time
          // warp NotReplicatedYetException with RemoteException as rpc does.
          final LocatedBlock ret;
          try {
            ret = (LocatedBlock) invocation.callRealMethod();
          } catch(NotReplicatedYetException e) {
            throw new RemoteException(e.getClass().getName(), e.getMessage());
          }
          final int blockCount = getBlockCount(ret);

          // Retrying should result in a new block at the end of the file.
          // (abandoning the old one)
          // It should not have NotReplicatedYetException.
          final LocatedBlock ret2;
          try {
            ret2 = (LocatedBlock) invocation.callRealMethod();
          } catch(NotReplicatedYetException e) {
            throw new AssertionError("Unexpected exception", e);
          }
          final int blockCount2 = getBlockCount(ret2);

          // We shouldn't have gained an extra block by the RPC.
          assertEquals(blockCount, blockCount2);
          return ret2;
        }
      }).when(spyNN).addBlock(Mockito.anyString(), Mockito.anyString(),
          Mockito.<ExtendedBlock> any(), Mockito.<DatanodeInfo[]> any(),
          Mockito.anyLong(), Mockito.<String[]> any());

      doAnswer(new Answer<Boolean>() {

        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          // complete() may return false a few times before it returns
          // true. We want to wait until it returns true, and then
          // make it retry one more time after that.
          LOG.info("Called complete:");
          if (!(Boolean)invocation.callRealMethod()) {
            LOG.info("Complete call returned false, not faking a retry RPC");
            return false;
          }
          // We got a successful close. Call it again to check idempotence.
          try {
            boolean ret = (Boolean) invocation.callRealMethod();
            LOG.info("Complete call returned true, faked second RPC. " +
                "Returned: " + ret);
            return ret;
          } catch (Throwable t) {
            LOG.error("Idempotent retry threw exception", t);
            throw t;
          }
        }
      }).when(spyNN).complete(Mockito.anyString(), Mockito.anyString(),
          Mockito.<ExtendedBlock>any(), anyLong());
      
      OutputStream stm = client.create(file.toString(), true);
      try {
        AppendTestUtil.write(stm, 0, 10000);
        stm.close();
        stm = null;
      } finally {
        IOUtils.cleanup(LOG, stm);
      }
      
      // Make sure the mock was actually properly injected.
      Mockito.verify(spyNN, Mockito.atLeastOnce()).addBlock(
          Mockito.anyString(), Mockito.anyString(),
          Mockito.<ExtendedBlock> any(), Mockito.<DatanodeInfo[]> any(),
          Mockito.anyLong(), Mockito.<String[]> any());
      Mockito.verify(spyNN, Mockito.atLeastOnce()).complete(
          Mockito.anyString(), Mockito.anyString(),
          Mockito.<ExtendedBlock>any(), anyLong());
      
      AppendTestUtil.check(fs, file, 10000);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Mock Answer implementation of NN.getBlockLocations that will return
   * a poisoned block list a certain number of times before returning
   * a proper one.
   */
  private static class FailNTimesAnswer implements Answer<LocatedBlocks> {
    private int failuresLeft;
    private final NamenodeProtocols realNN;

    public FailNTimesAnswer(NamenodeProtocols preSpyNN, int timesToFail) {
      failuresLeft = timesToFail;
      this.realNN = preSpyNN;
    }

    @Override
    public LocatedBlocks answer(InvocationOnMock invocation) throws IOException {
      Object args[] = invocation.getArguments();
      LocatedBlocks realAnswer = realNN.getBlockLocations(
        (String)args[0],
        (Long)args[1],
        (Long)args[2]);

      if (failuresLeft-- > 0) {
        NameNode.LOG.info("FailNTimesAnswer injecting failure.");
        return makeBadBlockList(realAnswer);
      }
      NameNode.LOG.info("FailNTimesAnswer no longer failing.");
      return realAnswer;
    }

    private LocatedBlocks makeBadBlockList(LocatedBlocks goodBlockList) {
      LocatedBlock goodLocatedBlock = goodBlockList.get(0);
      LocatedBlock badLocatedBlock = new LocatedBlock(
        goodLocatedBlock.getBlock(),
        new DatanodeInfo[] {
          DFSTestUtil.getDatanodeInfo("1.2.3.4", "bogus", 1234)
        },
        goodLocatedBlock.getStartOffset(),
        false);


      List<LocatedBlock> badBlocks = new ArrayList<LocatedBlock>();
      badBlocks.add(badLocatedBlock);
      return new LocatedBlocks(goodBlockList.getFileLength(), false,
                               badBlocks, null, true,
                               null);
    }
  }
  
  /**
   * Test that a DFSClient waits for random time before retry on busy blocks.
   */
  @Test
  public void testDFSClientRetriesOnBusyBlocks() throws IOException {
    
    System.out.println("Testing DFSClient random waiting on busy blocks.");
    
    //
    // Test settings: 
    // 
    //           xcievers    fileLen   #clients  timeWindow    #retries
    //           ========    =======   ========  ==========    ========
    // Test 1:          2       6 MB         50      300 ms           3
    // Test 2:          2       6 MB         50      300 ms          50
    // Test 3:          2       6 MB         50     1000 ms           3
    // Test 4:          2       6 MB         50     1000 ms          50
    // 
    //   Minimum xcievers is 2 since 1 thread is reserved for registry.
    //   Test 1 & 3 may fail since # retries is low. 
    //   Test 2 & 4 should never fail since (#threads)/(xcievers-1) is the upper
    //   bound for guarantee to not throw BlockMissingException.
    //
    int xcievers  = 2;
    int fileLen   = 6*1024*1024;
    int threads   = 50;
    int retries   = 3;
    int timeWin   = 300;
    
    //
    // Test 1: might fail
    // 
    long timestamp = Time.now();
    boolean pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    long timestamp2 = Time.now();
    if ( pass ) {
      LOG.info("Test 1 succeeded! Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    } else {
      LOG.warn("Test 1 failed, but relax. Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    }
    
    //
    // Test 2: should never fail
    // 
    retries = 50;
    timestamp = Time.now();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = Time.now();
    assertTrue("Something wrong! Test 2 got Exception with maxmum retries!", pass);
    LOG.info("Test 2 succeeded! Time spent: "  + (timestamp2-timestamp)/1000.0 + " sec.");
    
    //
    // Test 3: might fail
    // 
    retries = 3;
    timeWin = 1000;
    timestamp = Time.now();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = Time.now();
    if ( pass ) {
      LOG.info("Test 3 succeeded! Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    } else {
      LOG.warn("Test 3 failed, but relax. Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    }
    
    //
    // Test 4: should never fail
    //
    retries = 50;
    timeWin = 1000;
    timestamp = Time.now();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = Time.now();
    assertTrue("Something wrong! Test 4 got Exception with maxmum retries!", pass);
    LOG.info("Test 4 succeeded! Time spent: "  + (timestamp2-timestamp)/1000.0 + " sec.");
  }

  private boolean busyTest(int xcievers, int threads, int fileLen, int timeWin, int retries) 
    throws IOException {

    boolean ret = true;
    short replicationFactor = 1;
    long blockSize = 128*1024*1024; // DFS block size
    int bufferSize = 4096;
    
    conf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, xcievers);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 
                retries);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, timeWin);
    // Disable keepalive
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY, 0);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(replicationFactor).build();
    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("test_data.dat");
    file1 = file1.makeQualified(fs.getUri(), fs.getWorkingDirectory()); // make URI hdfs://
    
    try {
      
      FSDataOutputStream stm = fs.create(file1, true,
                                         bufferSize,
                                         replicationFactor,
                                         blockSize);
      
      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isFile());
      System.out.println("Path : \"" + file1 + "\"");
      LOG.info("Path : \"" + file1 + "\"");

      // write 1 block to file
      byte[] buffer = AppendTestUtil.randomBytes(Time.now(), fileLen);
      stm.write(buffer, 0, fileLen);
      stm.close();

      // verify that file size has changed to the full size
      long len = fs.getFileStatus(file1).getLen();
      
      assertTrue(file1 + " should be of size " + fileLen +
                 " but found to be of size " + len, 
                  len == fileLen);
      
      // read back and check data integrigy
      byte[] read_buf = new byte[fileLen];
      InputStream in = fs.open(file1, fileLen);
      IOUtils.readFully(in, read_buf, 0, fileLen);
      assert(Arrays.equals(buffer, read_buf));
      in.close();
      read_buf = null; // GC it if needed
      
      // compute digest of the content to reduce memory space
      MessageDigest m = MessageDigest.getInstance("SHA");
      m.update(buffer, 0, fileLen);
      byte[] hash_sha = m.digest();

      // spawn multiple threads and all trying to access the same block
      Thread[] readers = new Thread[threads];
      Counter counter = new Counter(0);
      for (int i = 0; i < threads; ++i ) {
        DFSClientReader reader = new DFSClientReader(file1, cluster, hash_sha, fileLen, counter);
        readers[i] = new Thread(reader);
        readers[i].start();
      }
      
      // wait for them to exit
      for (int i = 0; i < threads; ++i ) {
        readers[i].join();
      }
      if ( counter.get() == threads )
        ret = true;
      else
        ret = false;
      
    } catch (InterruptedException e) {
      System.out.println("Thread got InterruptedException.");
      e.printStackTrace();
      ret = false;
    } catch (Exception e) {
      e.printStackTrace();
      ret = false;
    } finally {
      fs.delete(file1, false);
      cluster.shutdown();
    }
    return ret;
  }

  private void verifyEmptyLease(LeaseRenewer leaseRenewer) throws Exception {
    int sleepCount = 0;
    while (!leaseRenewer.isEmpty() && sleepCount++ < 20) {
      Thread.sleep(500);
    }
    assertTrue("Lease should be empty.", leaseRenewer.isEmpty());
  }

  class DFSClientReader implements Runnable {
    
    DFSClient client;
    final Configuration conf;
    final byte[] expected_sha;
    FileSystem  fs;
    final Path filePath;
    final MiniDFSCluster cluster;
    final int len;
    final Counter counter;

    DFSClientReader(Path file, MiniDFSCluster cluster, byte[] hash_sha, int fileLen, Counter cnt) {
      filePath = file;
      this.cluster = cluster;
      counter = cnt;
      len = fileLen;
      conf = new HdfsConfiguration();
      expected_sha = hash_sha;
      try {
        cluster.waitActive();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public void run() {
      try {
        fs = cluster.getNewFileSystemInstance(0);
        
        int bufferSize = len;
        byte[] buf = new byte[bufferSize];

        InputStream in = fs.open(filePath, bufferSize);
        
        // read the whole file
        IOUtils.readFully(in, buf, 0, bufferSize);
        
        // compare with the expected input
        MessageDigest m = MessageDigest.getInstance("SHA");
        m.update(buf, 0, bufferSize);
        byte[] hash_sha = m.digest();
        
        buf = null; // GC if needed since there may be too many threads
        in.close();
        fs.close();

        assertTrue("hashed keys are not the same size",
                   hash_sha.length == expected_sha.length);

        assertTrue("hashed keys are not equal",
                   Arrays.equals(hash_sha, expected_sha));
        
        counter.inc(); // count this thread as successful
        
        LOG.info("Thread correctly read the block.");
        
      } catch (BlockMissingException e) {
        LOG.info("Bad - BlockMissingException is caught.");
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      } 
    }
  }

  class Counter {
    int counter;
    Counter(int n) { counter = n; }
    public synchronized void inc() { ++counter; }
    public int get() { return counter; }
  }

  @Test
  public void testGetFileChecksum() throws Exception {
    final String f = "/testGetFileChecksum";
    final Path p = new Path(f);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();

      //create a file
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, p, 1L << 20, (short)3, 20100402L);

      //get checksum
      final FileChecksum cs1 = fs.getFileChecksum(p);
      assertTrue(cs1 != null);

      //stop the first datanode
      final List<LocatedBlock> locatedblocks = DFSClient.callGetBlockLocations(
          cluster.getNameNodeRpc(), f, 0, Long.MAX_VALUE)
            .getLocatedBlocks();
      final DatanodeInfo first = locatedblocks.get(0).getLocations()[0];
      cluster.stopDataNode(first.getXferAddr());

      //get checksum again
      final FileChecksum cs2 = fs.getFileChecksum(p);
      assertEquals(cs1, cs2);
    } finally {
      cluster.shutdown();
    }
  }

  /** Test that timeout occurs when DN does not respond to RPC.
   * Start up a server and ask it to sleep for n seconds. Make an
   * RPC to the server and set rpcTimeout to less than n and ensure
   * that socketTimeoutException is obtained
   */
  @Test
  public void testClientDNProtocolTimeout() throws IOException {
    final Server server = new TestServer(1, true);
    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    DatanodeID fakeDnId = DFSTestUtil.getLocalDatanodeID(addr.getPort());
    
    ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
    LocatedBlock fakeBlock = new LocatedBlock(b, new DatanodeInfo[0]);

    ClientDatanodeProtocol proxy = null;

    try {
      proxy = DFSUtil.createClientDatanodeProtocolProxy(
          fakeDnId, conf, 500, false, fakeBlock);

      proxy.getReplicaVisibleLength(new ExtendedBlock("bpid", 1));
      fail ("Did not get expected exception: SocketTimeoutException");
    } catch (SocketTimeoutException e) {
      LOG.info("Got the expected Exception: SocketTimeoutException");
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      server.stop();
    }
  }

  /** Test client retry with namenode restarting. */
  @Test(timeout=300000)
  public void testNamenodeRestart() throws Exception {
    namenodeRestartTest(new Configuration(), false);
  }

  public static void namenodeRestartTest(final Configuration conf,
      final boolean isWebHDFS) throws Exception {
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);

    final List<Exception> exceptions = new ArrayList<Exception>();

    final Path dir = new Path("/testNamenodeRestart");

    if (isWebHDFS) {
      conf.setBoolean(DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    } else {
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    }
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setInt(MiniDFSCluster.DFS_NAMENODE_SAFEMODE_EXTENSION_TESTING_KEY, 5000);

    final short numDatanodes = 3;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FileSystem fs = isWebHDFS ? WebHdfsTestUtil.getWebHdfsFileSystem(
          conf, WebHdfsFileSystem.SCHEME) : dfs;
      final URI uri = dfs.getUri();
      assertTrue(HdfsUtils.isHealthy(uri));

      //create a file
      final long length = 1L << 20;
      final Path file1 = new Path(dir, "foo"); 
      DFSTestUtil.createFile(fs, file1, length, numDatanodes, 20120406L);

      //get file status
      final FileStatus s1 = fs.getFileStatus(file1);
      assertEquals(length, s1.getLen());

      //create file4, write some data but not close
      final Path file4 = new Path(dir, "file4"); 
      final FSDataOutputStream out4 = fs.create(file4, false, 4096,
          fs.getDefaultReplication(file4), 1024L, null);
      final byte[] bytes = new byte[1000];
      new Random().nextBytes(bytes);
      out4.write(bytes);
      out4.write(bytes);
      if (isWebHDFS) {
        // WebHDFS does not support hflush. To avoid DataNode communicating with
        // NN while we're shutting down NN, we call out4.close() to finish
        // writing the data
        out4.close();
      } else {
        out4.hflush();
      }

      //shutdown namenode
      assertTrue(HdfsUtils.isHealthy(uri));
      cluster.shutdownNameNode(0);
      assertFalse(HdfsUtils.isHealthy(uri));

      //namenode is down, continue writing file4 in a thread
      final Thread file4thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //write some more data and then close the file
            if (!isWebHDFS) {
              out4.write(bytes);
              out4.write(bytes);
              out4.write(bytes);
              out4.close();
            }
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      });
      file4thread.start();

      //namenode is down, read the file in a thread
      final Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //it should retry till namenode is up.
            final FileSystem fs = createFsWithDifferentUsername(conf, isWebHDFS);
            final FSDataInputStream in = fs.open(file1);
            int count = 0;
            for(; in.read() != -1; count++);
            in.close();
            assertEquals(s1.getLen(), count);
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      });
      reader.start();

      //namenode is down, create another file in a thread
      final Path file3 = new Path(dir, "file"); 
      final Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //it should retry till namenode is up.
            final FileSystem fs = createFsWithDifferentUsername(conf, isWebHDFS);
            DFSTestUtil.createFile(fs, file3, length, numDatanodes, 20120406L);
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      });
      thread.start();

      //restart namenode in a new thread
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //sleep, restart, and then wait active
            TimeUnit.SECONDS.sleep(30);
            assertFalse(HdfsUtils.isHealthy(uri));
            cluster.restartNameNode(0, false);
            cluster.waitActive();
            assertTrue(HdfsUtils.isHealthy(uri));
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      }).start();

      //namenode is down, it should retry until namenode is up again. 
      final FileStatus s2 = fs.getFileStatus(file1);
      assertEquals(s1, s2);

      //check file1 and file3
      thread.join();
      assertEmpty(exceptions);
      assertEquals(s1.getLen(), fs.getFileStatus(file3).getLen());
      assertEquals(fs.getFileChecksum(file1), fs.getFileChecksum(file3));

      reader.join();
      assertEmpty(exceptions);

      //check file4
      file4thread.join();
      assertEmpty(exceptions);
      {
        final FSDataInputStream in = fs.open(file4);
        int count = 0;
        for(int r; (r = in.read()) != -1; count++) {
          Assert.assertEquals(String.format("count=%d", count),
              bytes[count % bytes.length], (byte)r);
        }
        if (!isWebHDFS) {
          Assert.assertEquals(5 * bytes.length, count);
        } else {
          Assert.assertEquals(2 * bytes.length, count);
        }
        in.close();
      }

      //enter safe mode
      assertTrue(HdfsUtils.isHealthy(uri));
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      assertFalse(HdfsUtils.isHealthy(uri));
      
      //leave safe mode in a new thread
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //sleep and then leave safe mode
            TimeUnit.SECONDS.sleep(30);
            assertFalse(HdfsUtils.isHealthy(uri));
            dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
            assertTrue(HdfsUtils.isHealthy(uri));
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      }).start();

      //namenode is in safe mode, create should retry until it leaves safe mode.
      final Path file2 = new Path(dir, "bar");
      DFSTestUtil.createFile(fs, file2, length, numDatanodes, 20120406L);
      assertEquals(fs.getFileChecksum(file1), fs.getFileChecksum(file2));
      
      assertTrue(HdfsUtils.isHealthy(uri));

      //make sure it won't retry on exceptions like FileNotFoundException
      final Path nonExisting = new Path(dir, "nonExisting");
      LOG.info("setPermission: " + nonExisting);
      try {
        fs.setPermission(nonExisting, new FsPermission((short)0));
        fail();
      } catch(FileNotFoundException fnfe) {
        LOG.info("GOOD!", fnfe);
      }

      assertEmpty(exceptions);
    } finally {
      cluster.shutdown();
    }
  }

  static void assertEmpty(final List<Exception> exceptions) {
    if (!exceptions.isEmpty()) {
      final StringBuilder b = new StringBuilder("There are ")
        .append(exceptions.size())
        .append(" exception(s):");
      for(int i = 0; i < exceptions.size(); i++) {
        b.append("\n  Exception ")
         .append(i)
         .append(": ")
         .append(StringUtils.stringifyException(exceptions.get(i)));
      }
      fail(b.toString());
    }
  }

  private static FileSystem createFsWithDifferentUsername(
      final Configuration conf, final boolean isWebHDFS
      ) throws IOException, InterruptedException {
    final String username = UserGroupInformation.getCurrentUser(
        ).getShortUserName() + "_XXX";
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        username, new String[]{"supergroup"});

    return isWebHDFS? WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem.SCHEME)
        : DFSTestUtil.getFileSystemAs(ugi, conf);
  }

  @Test
  public void testMultipleLinearRandomRetry() {
    parseMultipleLinearRandomRetry(null, "");
    parseMultipleLinearRandomRetry(null, "11");
    parseMultipleLinearRandomRetry(null, "11,22,33");
    parseMultipleLinearRandomRetry(null, "11,22,33,44,55");
    parseMultipleLinearRandomRetry(null, "AA");
    parseMultipleLinearRandomRetry(null, "11,AA");
    parseMultipleLinearRandomRetry(null, "11,22,33,FF");
    parseMultipleLinearRandomRetry(null, "11,-22");
    parseMultipleLinearRandomRetry(null, "-11,22");

    parseMultipleLinearRandomRetry("[22x11ms]",
        "11,22");
    parseMultipleLinearRandomRetry("[22x11ms, 44x33ms]",
        "11,22,33,44");
    parseMultipleLinearRandomRetry("[22x11ms, 44x33ms, 66x55ms]",
        "11,22,33,44,55,66");
    parseMultipleLinearRandomRetry("[22x11ms, 44x33ms, 66x55ms]",
        "   11,   22, 33,  44, 55,  66   ");
  }
  
  static void parseMultipleLinearRandomRetry(String expected, String s) {
    final MultipleLinearRandomRetry r = MultipleLinearRandomRetry.parseCommaSeparatedString(s);
    LOG.info("input=" + s + ", parsed=" + r + ", expected=" + expected);
    if (r == null) {
      assertEquals(expected, null);
    } else {
      assertEquals("MultipleLinearRandomRetry" + expected, r.toString());
    }
  }

  /**
   * Test that checksum failures are recovered from by the next read on the same
   * DFSInputStream. Corruption information is not persisted from read call to
   * read call, so the client should expect consecutive calls to behave the same
   * way. See HDFS-3067.
   */
  @Test
  public void testRetryOnChecksumFailure() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster =
      new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

    try {
      final short REPL_FACTOR = 1;
      final long FILE_LENGTH = 512L;
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      Path path = new Path("/corrupted");

      DFSTestUtil.createFile(fs, path, FILE_LENGTH, REPL_FACTOR, 12345L);
      DFSTestUtil.waitReplication(fs, path, REPL_FACTOR);

      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, path);
      int blockFilesCorrupted = cluster.corruptBlockOnDataNodes(block);
      assertEquals("All replicas not corrupted", REPL_FACTOR,
          blockFilesCorrupted);

      InetSocketAddress nnAddr =
        new InetSocketAddress("localhost", cluster.getNameNodePort());
      DFSClient client = new DFSClient(nnAddr, conf);
      DFSInputStream dis = client.open(path.toString());
      byte[] arr = new byte[(int)FILE_LENGTH];
      for (int i = 0; i < 2; ++i) {
        try {
          dis.read(arr, 0, (int)FILE_LENGTH);
          fail("Expected ChecksumException not thrown");
        } catch (Exception ex) {
          GenericTestUtils.assertExceptionContains(
              "Checksum error", ex);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }
}
