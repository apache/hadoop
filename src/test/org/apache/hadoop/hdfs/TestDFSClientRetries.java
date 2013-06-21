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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors.
 */
public class TestDFSClientRetries extends TestCase {
  private static final String ADDRESS = "0.0.0.0";
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;
  public static final Log LOG =
    LogFactory.getLog(TestDFSClientRetries.class.getName());
  final static private Configuration conf = new Configuration();
 
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
    public Writable call(Class<?> protocol, Writable param, long receiveTime)
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
  
  /**
   * This makes sure that when DN closes clients socket after client had
   * successfully connected earlier, the data can still be fetched.
   */
  public void testWriteTimeoutAtDataNode() throws IOException,
                                                  InterruptedException { 
    
    final int writeTimeout = 100; //milliseconds.
    // set a very short write timeout for datanode, so that tests runs fast.
    conf.setInt("dfs.datanode.socket.write.timeout", writeTimeout); 
    // set a smaller block size
    final int blockSize = 10*1024*1024;
    conf.setInt("dfs.block.size", blockSize);
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    // set a small buffer size
    final int bufferSize = 4096;
    conf.setInt("io.file.buffer.size", bufferSize);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    
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
  
  class TestNameNode implements ClientProtocol
  {
    int num_calls = 0;
    
    // The total number of calls that can be made to addBlock
    // before an exception is thrown
    int num_calls_allowed; 
    public final String ADD_BLOCK_EXCEPTION = "Testing exception thrown from"
                                             + "TestDFSClientRetries::"
                                             + "TestNameNode::addBlock";
    public final String RETRY_CONFIG
          = "dfs.client.block.write.locateFollowingBlock.retries";
          
    public TestNameNode(Configuration conf) throws IOException
    {
      // +1 because the configuration value is the number of retries and
      // the first call is not a retry (e.g., 2 retries == 3 total
      // calls allowed)
      this.num_calls_allowed = conf.getInt(RETRY_CONFIG, 5) + 1;
    }

    public long getProtocolVersion(String protocol, 
                                     long clientVersion)
    throws IOException
    {
      return versionID;
    }

    public boolean isFileClosed(String src) throws IOException {
      return true;
    }

    public LocatedBlock addBlock(String src, String clientName)
    throws IOException
    {
      return addBlock(src, clientName, null);
    }


    public LocatedBlock addBlock(String src, String clientName,
                                 DatanodeInfo[] excludedNode)
      throws IOException {
      num_calls++;
      if (num_calls > num_calls_allowed) { 
        throw new IOException("addBlock called more times than "
                              + RETRY_CONFIG
                              + " allows.");
      } else {
          throw new RemoteException(NotReplicatedYetException.class.getName(),
                                    ADD_BLOCK_EXCEPTION);
      }
    }
    
    
    // The following methods are stub methods that are not needed by this mock class
    public LocatedBlocks  getBlockLocations(String src, long offset, long length) throws IOException { return null; }

    @Deprecated
    public void create(String src, FsPermission masked, String clientName, boolean overwrite, short replication, long blockSize) throws IOException {}

    public void create(String src, FsPermission masked, String clientName, boolean overwrite, boolean createparent, short replication, long blockSize) throws IOException {}

    public LocatedBlock append(String src, String clientName) throws IOException { return null; }

    public boolean setReplication(String src, short replication) throws IOException { return false; }

    public void setPermission(String src, FsPermission permission) throws IOException {}

    public void setOwner(String src, String username, String groupname) throws IOException {}

    public void abandonBlock(Block b, String src, String holder) throws IOException {}

    public boolean complete(String src, String clientName) throws IOException { return false; }

    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {}

    public boolean rename(String src, String dst) throws IOException { return false; }
    
    public void concat(String trg, String[] srcs)  throws IOException {}

    public boolean delete(String src) throws IOException { return false; }

    public boolean delete(String src, boolean recursive) throws IOException { return false; }

    public boolean mkdirs(String src, FsPermission masked) throws IOException { return false; }

    public HdfsFileStatus[] getListing(String src) throws IOException { return null; }

    public DirectoryListing getListing(String src, byte[] startName) throws IOException { return null; }

    public void renewLease(String clientName) throws IOException {}

    public long[] getStats() throws IOException { return null; }

    public DatanodeInfo[] getDatanodeReport(FSConstants.DatanodeReportType type) throws IOException { return null; }

    public long getPreferredBlockSize(String filename) throws IOException { return 0; }

    public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException { return false; }

    public void saveNamespace() throws IOException {}

    public boolean restoreFailedStorage(String arg) throws AccessControlException { return false; }

    public void refreshNodes() throws IOException {}

    public void finalizeUpgrade() throws IOException {}

    public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException { return null; }

    public void metaSave(String filename) throws IOException {}

    public void setBalancerBandwidth(long bandwidth) throws IOException {}

    public HdfsFileStatus getFileInfo(String src) throws IOException { return null; }

    public ContentSummary getContentSummary(String path) throws IOException { return null; }

    public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException {}

    public void fsync(String src, String client) throws IOException {}

    public void setTimes(String src, long mtime, long atime) throws IOException {}

    public boolean recoverLease(String src, String clientName) throws IOException {return true;}

    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
        throws IOException {
      return null;
    }

    public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
        throws InvalidToken, IOException {
      return 0;
    }

    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
        throws IOException {
    }
  }
  
  public void testNotYetReplicatedErrors() throws IOException
  {   
    // allow 1 retry (2 total calls)
    conf.setInt("dfs.client.block.write.locateFollowingBlock.retries", 1);
        
    TestNameNode tnn = new TestNameNode(conf);
    final DFSClient client = new DFSClient(null, tnn, conf, null);
    OutputStream os = client.create("testfile", true);
    os.write(20); // write one random byte
    
    try {
      os.close();
    } catch (Exception e) {
      assertTrue("Retries are not being stopped correctly",
           e.getMessage().equals(tnn.ADD_BLOCK_EXCEPTION));
    }
  }

  /**
   * This tests that DFSInputStream failures are counted for a given read
   * operation, and not over the lifetime of the stream. It is a regression
   * test for HDFS-127.
   */
  public void testFailuresArePerOperation() throws Exception
  {
    long fileSize = 4096;
    Path file = new Path("/testFile");

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);

    int maxBlockAcquires = DFSClient.getMaxBlockAcquireFailures(conf);
    assertTrue(maxBlockAcquires > 0);

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode preSpyNN = cluster.getNameNode();
      NameNode spyNN = spy(preSpyNN);
      DFSClient client = new DFSClient(null, spyNN, conf, null);

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
   * Mock Answer implementation of NN.getBlockLocations that will return
   * a poisoned block list a certain number of times before returning
   * a proper one.
   */
  private static class FailNTimesAnswer implements Answer<LocatedBlocks> {
    private int failuresLeft;
    private NameNode realNN;

    public FailNTimesAnswer(NameNode realNN, int timesToFail) {
      failuresLeft = timesToFail;
      this.realNN = realNN;
    }

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
          new DatanodeInfo(new DatanodeID("255.255.255.255:234"))
        },
        goodLocatedBlock.getStartOffset(),
        false);


      List<LocatedBlock> badBlocks = new ArrayList<LocatedBlock>();
      badBlocks.add(badLocatedBlock);
      return new LocatedBlocks(goodBlockList.getFileLength(), badBlocks, false);
    }
  }
  
  /** Test that timeout occurs when DN does not respond to RPC.
   * Start up a server and ask it to sleep for n seconds. Make an
   * RPC to the server and set rpcTimeout to less than n and ensure
   * that socketTimeoutException is obtained
   */
  public void testClientDNProtocolTimeout() throws IOException {
    final Server server = new TestServer(1, true);
    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    DatanodeID fakeDnId = new DatanodeID(
        "localhost:" + addr.getPort(), "fake-storage", 0, addr.getPort());
    DatanodeInfo dnInfo = new DatanodeInfo(fakeDnId);

    LocatedBlock fakeBlock = new LocatedBlock(new Block(12345L), new DatanodeInfo[0]);

    ClientDatanodeProtocol proxy = null;

    try {
      proxy = DFSClient.createClientDatanodeProtocolProxy(dnInfo, conf,
          fakeBlock.getBlock(), fakeBlock.getBlockToken(), 500, false);

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

  public void testGetFileChecksum() throws Exception {
    final String f = "/testGetFileChecksum";
    final Path p = new Path(f);

    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
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
          cluster.getNameNode(), f, 0, Long.MAX_VALUE).getLocatedBlocks();
      final DatanodeInfo first = locatedblocks.get(0).getLocations()[0];
      cluster.stopDataNode(first.getName());

      //get checksum again
      final FileChecksum cs2 = fs.getFileChecksum(p);
      assertEquals(cs1, cs2);
    } finally {
      cluster.shutdown();
    }
  }

  /** Test client retry with namenode restarting. */
  public void testNamenodeRestart() throws Exception {
    namenodeRestartTest(new Configuration(), false);
  }

  public static void namenodeRestartTest(final Configuration conf,
      final boolean isWebHDFS) throws Exception {
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);

    final List<Exception> exceptions = new ArrayList<Exception>();

    final Path dir = new Path("/testNamenodeRestart");

    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setInt("dfs.safemode.extension.testing", 5000);

    final short numDatanodes = 3;
    final MiniDFSCluster cluster = new MiniDFSCluster(
        conf, numDatanodes, true, null);
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      final FileSystem fs = isWebHDFS?
          WebHdfsTestUtil.getWebHdfsFileSystem(conf): dfs;
      final URI uri = dfs.getUri();
      assertTrue(DistributedFileSystem.isHealthy(uri));

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
      out4.sync();

      //shutdown namenode
      assertTrue(DistributedFileSystem.isHealthy(uri));
      cluster.shutdownNameNode();
      assertFalse(DistributedFileSystem.isHealthy(uri));

      //namenode is down, continue writing file4 in a thread
      final Thread file4thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //write some more data and then close the file
            out4.write(bytes);
            out4.write(bytes);
            out4.write(bytes);
            out4.close();
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
            assertFalse(DistributedFileSystem.isHealthy(uri));
            cluster.restartNameNode(false, false);
            cluster.waitActive();
            assertTrue(DistributedFileSystem.isHealthy(uri));
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
        Assert.assertEquals(5 * bytes.length, count);
        in.close();
      }

      //enter safe mode
      assertTrue(DistributedFileSystem.isHealthy(uri));
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      assertFalse(DistributedFileSystem.isHealthy(uri));
      
      //leave safe mode in a new thread
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            //sleep and then leave safe mode
            TimeUnit.SECONDS.sleep(30);
            assertFalse(DistributedFileSystem.isHealthy(uri));
            dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
            assertTrue(DistributedFileSystem.isHealthy(uri));
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      }).start();

      //namenode is in safe mode, create should retry until it leaves safe mode.
      final Path file2 = new Path(dir, "bar");
      DFSTestUtil.createFile(fs, file2, length, numDatanodes, 20120406L);
      assertEquals(fs.getFileChecksum(file1), fs.getFileChecksum(file2));

      assertTrue(DistributedFileSystem.isHealthy(uri));
      
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
    String username = UserGroupInformation.getCurrentUser().getShortUserName()+"_XXX";
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting(username, new String[]{"supergroup"});
    
    return isWebHDFS? WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, conf)
        : DFSTestUtil.getFileSystemAs(ugi, conf);
  }

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
      Assert.assertEquals(expected, null);
    } else {
      Assert.assertEquals("MultipleLinearRandomRetry" + expected, r.toString());
    }
  }
}
