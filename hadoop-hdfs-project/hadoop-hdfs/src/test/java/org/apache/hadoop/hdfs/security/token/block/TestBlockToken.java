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

package org.apache.hadoop.hdfs.security.token.block;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ClientDatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.io.TestWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/** Unit tests for block tokens */
public class TestBlockToken {
  public static final Log LOG = LogFactory.getLog(TestBlockToken.class);
  private static final String ADDRESS = "0.0.0.0";

  static {
    ((Log4JLogger) Client.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) SaslInputStream.LOG).getLogger().setLevel(Level.ALL);
  }

  /** Directory where we can count our open file descriptors under Linux */
  static final File FD_DIR = new File("/proc/self/fd/");

  final long blockKeyUpdateInterval = 10 * 60 * 1000; // 10 mins
  final long blockTokenLifetime = 2 * 60 * 1000; // 2 mins
  final ExtendedBlock block1 = new ExtendedBlock("0", 0L);
  final ExtendedBlock block2 = new ExtendedBlock("10", 10L);
  final ExtendedBlock block3 = new ExtendedBlock("-10", -108L);
  
  @Before
  public void disableKerberos() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
    UserGroupInformation.setConfiguration(conf);
  }

  private static class GetLengthAnswer implements
      Answer<GetReplicaVisibleLengthResponseProto> {
    final BlockTokenSecretManager sm;
    final BlockTokenIdentifier ident;

    public GetLengthAnswer(BlockTokenSecretManager sm,
        BlockTokenIdentifier ident) {
      this.sm = sm;
      this.ident = ident;
    }

    @Override
    public GetReplicaVisibleLengthResponseProto answer(
        InvocationOnMock invocation) throws IOException {
      Object args[] = invocation.getArguments();
      assertEquals(2, args.length);
      GetReplicaVisibleLengthRequestProto req = 
          (GetReplicaVisibleLengthRequestProto) args[1];
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      assertEquals("Only one BlockTokenIdentifier expected", 1, tokenIds.size());
      long result = 0;
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        LOG.info("Got: " + id.toString());
        assertTrue("Received BlockTokenIdentifier is wrong", ident.equals(id));
        sm.checkAccess(id, null, PBHelper.convert(req.getBlock()),
            BlockTokenSecretManager.AccessMode.WRITE);
        result = id.getBlockId();
      }
      return GetReplicaVisibleLengthResponseProto.newBuilder()
          .setLength(result).build();
    }
  }

  private BlockTokenIdentifier generateTokenId(BlockTokenSecretManager sm,
      ExtendedBlock block,
      EnumSet<BlockTokenSecretManager.AccessMode> accessModes)
      throws IOException {
    Token<BlockTokenIdentifier> token = sm.generateToken(block, accessModes);
    BlockTokenIdentifier id = sm.createIdentifier();
    id.readFields(new DataInputStream(new ByteArrayInputStream(token
        .getIdentifier())));
    return id;
  }

  @Test
  public void testWritable() throws Exception {
    TestWritable.testWritable(new BlockTokenIdentifier());
    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    TestWritable.testWritable(generateTokenId(sm, block1,
        EnumSet.allOf(BlockTokenSecretManager.AccessMode.class)));
    TestWritable.testWritable(generateTokenId(sm, block2,
        EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
    TestWritable.testWritable(generateTokenId(sm, block3,
        EnumSet.noneOf(BlockTokenSecretManager.AccessMode.class)));
  }

  private void tokenGenerationAndVerification(BlockTokenSecretManager master,
      BlockTokenSecretManager slave) throws Exception {
    // single-mode tokens
    for (BlockTokenSecretManager.AccessMode mode : BlockTokenSecretManager.AccessMode
        .values()) {
      // generated by master
      Token<BlockTokenIdentifier> token1 = master.generateToken(block1,
          EnumSet.of(mode));
      master.checkAccess(token1, null, block1, mode);
      slave.checkAccess(token1, null, block1, mode);
      // generated by slave
      Token<BlockTokenIdentifier> token2 = slave.generateToken(block2,
          EnumSet.of(mode));
      master.checkAccess(token2, null, block2, mode);
      slave.checkAccess(token2, null, block2, mode);
    }
    // multi-mode tokens
    Token<BlockTokenIdentifier> mtoken = master.generateToken(block3,
        EnumSet.allOf(BlockTokenSecretManager.AccessMode.class));
    for (BlockTokenSecretManager.AccessMode mode : BlockTokenSecretManager.AccessMode
        .values()) {
      master.checkAccess(mtoken, null, block3, mode);
      slave.checkAccess(mtoken, null, block3, mode);
    }
  }

  /** test block key and token handling */
  @Test
  public void testBlockTokenSecretManager() throws Exception {
    BlockTokenSecretManager masterHandler = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    BlockTokenSecretManager slaveHandler = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, "fake-pool", null);
    ExportedBlockKeys keys = masterHandler.exportKeys();
    slaveHandler.addKeys(keys);
    tokenGenerationAndVerification(masterHandler, slaveHandler);
    // key updating
    masterHandler.updateKeys();
    tokenGenerationAndVerification(masterHandler, slaveHandler);
    keys = masterHandler.exportKeys();
    slaveHandler.addKeys(keys);
    tokenGenerationAndVerification(masterHandler, slaveHandler);
  }

  private static Server createMockDatanode(BlockTokenSecretManager sm,
      Token<BlockTokenIdentifier> token, Configuration conf)
      throws IOException, ServiceException {
    ClientDatanodeProtocolPB mockDN = mock(ClientDatanodeProtocolPB.class);

    BlockTokenIdentifier id = sm.createIdentifier();
    id.readFields(new DataInputStream(new ByteArrayInputStream(token
        .getIdentifier())));
    
    doAnswer(new GetLengthAnswer(sm, id)).when(mockDN)
        .getReplicaVisibleLength(any(RpcController.class),
            any(GetReplicaVisibleLengthRequestProto.class));

    RPC.setProtocolEngine(conf, ClientDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    BlockingService service = ClientDatanodeProtocolService
        .newReflectiveBlockingService(mockDN);
    return new RPC.Builder(conf).setProtocol(ClientDatanodeProtocolPB.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(0)
        .setNumHandlers(5).setVerbose(true).setSecretManager(sm).build();
  }

  @Test
  public void testBlockTokenRpc() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    
    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    Token<BlockTokenIdentifier> token = sm.generateToken(block3,
        EnumSet.allOf(BlockTokenSecretManager.AccessMode.class));

    final Server server = createMockDatanode(sm, token, conf);

    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    final UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(block3.toString());
    ticket.addToken(token);

    ClientDatanodeProtocol proxy = null;
    try {
      proxy = DFSUtil.createClientDatanodeProtocolProxy(addr, ticket, conf,
          NetUtils.getDefaultSocketFactory(conf));
      assertEquals(block3.getBlockId(), proxy.getReplicaVisibleLength(block3));
    } finally {
      server.stop();
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  /**
   * Test that fast repeated invocations of createClientDatanodeProtocolProxy
   * will not end up using up thousands of sockets. This is a regression test
   * for HDFS-1965.
   */
  @Test
  public void testBlockTokenRpcLeak() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    
    Assume.assumeTrue(FD_DIR.exists());
    BlockTokenSecretManager sm = new BlockTokenSecretManager(
        blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
    Token<BlockTokenIdentifier> token = sm.generateToken(block3,
        EnumSet.allOf(BlockTokenSecretManager.AccessMode.class));

    final Server server = createMockDatanode(sm, token, conf);
    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    DatanodeID fakeDnId = DFSTestUtil.getLocalDatanodeID(addr.getPort());

    ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
    LocatedBlock fakeBlock = new LocatedBlock(b, new DatanodeInfo[0]);
    fakeBlock.setBlockToken(token);

    // Create another RPC proxy with the same configuration - this will never
    // attempt to connect anywhere -- but it causes the refcount on the
    // RPC "Client" object to stay above 0 such that RPC.stopProxy doesn't
    // actually close the TCP connections to the real target DN.
    ClientDatanodeProtocol proxyToNoWhere = RPC.getProxy(
        ClientDatanodeProtocol.class, ClientDatanodeProtocol.versionID,
        new InetSocketAddress("1.1.1.1", 1),
        UserGroupInformation.createRemoteUser("junk"), conf,
        NetUtils.getDefaultSocketFactory(conf));

    ClientDatanodeProtocol proxy = null;

    int fdsAtStart = countOpenFileDescriptors();
    try {
      long endTime = Time.now() + 3000;
      while (Time.now() < endTime) {
        proxy = DFSUtil.createClientDatanodeProtocolProxy(fakeDnId, conf, 1000,
            false, fakeBlock);
        assertEquals(block3.getBlockId(), proxy.getReplicaVisibleLength(block3));
        if (proxy != null) {
          RPC.stopProxy(proxy);
        }
        LOG.info("Num open fds:" + countOpenFileDescriptors());
      }

      int fdsAtEnd = countOpenFileDescriptors();

      if (fdsAtEnd - fdsAtStart > 50) {
        fail("Leaked " + (fdsAtEnd - fdsAtStart) + " fds!");
      }
    } finally {
      server.stop();
    }

    RPC.stopProxy(proxyToNoWhere);
  }

  /**
   * @return the current number of file descriptors open by this process.
   */
  private static int countOpenFileDescriptors() {
    return FD_DIR.list().length;
  }

  /**
   * Test {@link BlockPoolTokenSecretManager}
   */
  @Test
  public void testBlockPoolTokenSecretManager() throws Exception {
    BlockPoolTokenSecretManager bpMgr = new BlockPoolTokenSecretManager();

    // Test BlockPoolSecretManager with upto 10 block pools
    for (int i = 0; i < 10; i++) {
      String bpid = Integer.toString(i);
      BlockTokenSecretManager masterHandler = new BlockTokenSecretManager(
          blockKeyUpdateInterval, blockTokenLifetime, 0, "fake-pool", null);
      BlockTokenSecretManager slaveHandler = new BlockTokenSecretManager(
          blockKeyUpdateInterval, blockTokenLifetime, "fake-pool", null);
      bpMgr.addBlockPool(bpid, slaveHandler);

      ExportedBlockKeys keys = masterHandler.exportKeys();
      bpMgr.addKeys(bpid, keys);
      tokenGenerationAndVerification(masterHandler, bpMgr.get(bpid));

      // Test key updating
      masterHandler.updateKeys();
      tokenGenerationAndVerification(masterHandler, bpMgr.get(bpid));
      keys = masterHandler.exportKeys();
      bpMgr.addKeys(bpid, keys);
      tokenGenerationAndVerification(masterHandler, bpMgr.get(bpid));
    }
  }

  /**
   * This test writes a file and gets the block locations without closing the
   * file, and tests the block token in the last block. Block token is verified
   * by ensuring it is of correct kind.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBlockTokenInLastLocatedBlock() throws IOException,
      InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    try {
      FileSystem fs = cluster.getFileSystem();
      String fileName = "/testBlockTokenInLastLocatedBlock";
      Path filePath = new Path(fileName);
      FSDataOutputStream out = fs.create(filePath, (short) 1);
      out.write(new byte[1000]);
      // ensure that the first block is written out (see FSOutputSummer#flush)
      out.flush();
      LocatedBlocks locatedBlocks = cluster.getNameNodeRpc().getBlockLocations(
          fileName, 0, 1000);
      while (locatedBlocks.getLastLocatedBlock() == null) {
        Thread.sleep(100);
        locatedBlocks = cluster.getNameNodeRpc().getBlockLocations(fileName, 0,
            1000);
      }
      Token<BlockTokenIdentifier> token = locatedBlocks.getLastLocatedBlock()
          .getBlockToken();
      Assert.assertEquals(BlockTokenIdentifier.KIND_NAME, token.getKind());
      out.close();
    } finally {
      cluster.shutdown();
    }
  }
}
