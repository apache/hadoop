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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StandardSocketFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks that used sockets have TCP_NODELAY set when configured.
 */
public class TestDataNodeTcpNoDelay {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeTcpNoDelay.class);
  private static Configuration baseConf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    baseConf = new HdfsConfiguration();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  @Test
  public void testTcpNoDelayEnabled() throws Exception {
    Configuration testConf = new Configuration(baseConf);
    // here we do not have to config TCP_NDELAY settings, since they should be
    // active by default
    testConf.set(HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        SocketFactoryWrapper.class.getName());

    SocketFactory defaultFactory = NetUtils.getDefaultSocketFactory(testConf);
    LOG.info("Socket factory is " + defaultFactory.getClass().getName());
    MiniDFSCluster dfsCluster =
        new MiniDFSCluster.Builder(testConf).numDataNodes(3).build();
    dfsCluster.waitActive();

    DistributedFileSystem dfs = dfsCluster.getFileSystem();

    try {
      createData(dfs);
      transferBlock(dfs);

      // check that TCP_NODELAY has been set on all sockets
      assertTrue(SocketFactoryWrapper.wasTcpNoDelayActive());
    } finally {
      SocketFactoryWrapper.reset();
      dfsCluster.shutdown();
    }
  }

  @Test
  public void testTcpNoDelayDisabled() throws Exception {
    Configuration testConf = new Configuration(baseConf);
    // disable TCP_NODELAY in settings
    setTcpNoDelay(testConf, false);
    testConf.set(HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        SocketFactoryWrapper.class.getName());

    SocketFactory defaultFactory = NetUtils.getDefaultSocketFactory(testConf);
    LOG.info("Socket factory is " + defaultFactory.getClass().getName());
    MiniDFSCluster dfsCluster =
        new MiniDFSCluster.Builder(testConf).numDataNodes(3).build();
    dfsCluster.waitActive();

    DistributedFileSystem dfs = dfsCluster.getFileSystem();

    try {
      createData(dfs);
      transferBlock(dfs);

      // we can only check that TCP_NODELAY was disabled on some sockets,
      // since part of the client write path always enables TCP_NODELAY
      // by necessity
      assertFalse(SocketFactoryWrapper.wasTcpNoDelayActive());
    } finally {
      SocketFactoryWrapper.reset();
      dfsCluster.shutdown();
    }
  }


  private void createData(DistributedFileSystem dfs) throws Exception {
    Path dir = new Path("test-dir");
    for (int i = 0; i < 3; i++) {
      Path f = new Path(dir, "file" + i);
      DFSTestUtil.createFile(dfs, f, 10240, (short) 3, 0);
    }
  }

  /**
   * Tests the {@code DataNode#transferBlocks()} path by re-replicating an
   * existing block.
   */
  private void transferBlock(DistributedFileSystem dfs) throws Exception {
    Path dir = new Path("test-block-transfer");
    Path f = new Path(dir, "testfile");
    DFSTestUtil.createFile(dfs, f, 10240, (short) 1, 0);

    // force a block transfer to another DN
    dfs.setReplication(f, (short) 2);
    DFSTestUtil.waitForReplication(dfs, f, (short) 2, 20000);
  }

  /**
   * Sets known TCP_NODELAY configs to the given value.
   */
  private void setTcpNoDelay(Configuration conf, boolean value) {
    conf.setBoolean(
        HdfsClientConfigKeys.DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_KEY, value);
    conf.setBoolean(
        DFSConfigKeys.DFS_DATA_TRANSFER_SERVER_TCPNODELAY, value);
    conf.setBoolean(
        CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY, value);
    conf.setBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY, value);
  }

  public static class SocketFactoryWrapper extends StandardSocketFactory {
    private static List<SocketWrapper> sockets = new ArrayList<SocketWrapper>();

    public static boolean wasTcpNoDelayActive() {
      LOG.info("Checking " + sockets.size() + " sockets for TCP_NODELAY");
      for (SocketWrapper sw : sockets) {
        if (!sw.getLastTcpNoDelay()) {
          return false;
        }
      }
      return true;
    }

    public static void reset() {
      sockets = new ArrayList<>();
    }

    @Override
    public Socket createSocket() throws IOException {
      LOG.info("Creating new socket");
      SocketWrapper wrapper = new SocketWrapper(super.createSocket());
      sockets.add(wrapper);
      return wrapper;
    }

    @Override
    public Socket createSocket(String host, int port)
        throws IOException, UnknownHostException {
      LOG.info("Creating socket for " + host);
      SocketWrapper wrapper =
          new SocketWrapper(super.createSocket(host, port));
      sockets.add(wrapper);
      return wrapper;
    }

    @Override
    public Socket createSocket(String host, int port,
                               InetAddress localHostAddr, int localPort)
        throws IOException, UnknownHostException {
      LOG.info("Creating socket for " + host);
      SocketWrapper wrapper = new SocketWrapper(
          super.createSocket(host, port, localHostAddr, localPort));
      sockets.add(wrapper);
      return wrapper;
    }

    @Override
    public Socket createSocket(InetAddress addr, int port) throws IOException {
      LOG.info("Creating socket for " + addr);
      SocketWrapper wrapper =
          new SocketWrapper(super.createSocket(addr, port));
      sockets.add(wrapper);
      return wrapper;
    }

    @Override
    public Socket createSocket(InetAddress addr, int port,
                               InetAddress localHostAddr, int localPort)
        throws IOException {
      LOG.info("Creating socket for " + addr);
      SocketWrapper wrapper = new SocketWrapper(
          super.createSocket(addr, port, localHostAddr, localPort));
      sockets.add(wrapper);
      return wrapper;
    }
  }

  public static class SocketWrapper extends Socket {
    private final Socket wrapped;
    private boolean tcpNoDelay;

    public SocketWrapper(Socket socket) {
      this.wrapped = socket;
    }

    // Override methods, check whether tcpnodelay has been set for each socket
    // created. This isn't perfect, as we could still send before tcpnodelay
    // is set, but should at least trigger when tcpnodelay is never set at all.

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
      wrapped.connect(endpoint);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout)
        throws IOException {
      wrapped.connect(endpoint, timeout);
    }

    @Override
    public void bind(SocketAddress bindpoint) throws IOException {
      wrapped.bind(bindpoint);
    }

    @Override
    public InetAddress getInetAddress() {
      return wrapped.getInetAddress();
    }

    @Override
    public InetAddress getLocalAddress() {
      return wrapped.getLocalAddress();
    }

    @Override
    public int getPort() {
      return wrapped.getPort();
    }

    @Override
    public int getLocalPort() {
      return wrapped.getLocalPort();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
      return wrapped.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
      return wrapped.getLocalSocketAddress();
    }

    @Override
    public SocketChannel getChannel() {
      return wrapped.getChannel();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return wrapped.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return wrapped.getOutputStream();
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
      wrapped.setTcpNoDelay(on);
      this.tcpNoDelay = on;
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
      return wrapped.getTcpNoDelay();
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
      wrapped.setSoLinger(on, linger);
    }

    @Override
    public int getSoLinger() throws SocketException {
      return wrapped.getSoLinger();
    }

    @Override
    public void sendUrgentData(int data) throws IOException {
      wrapped.sendUrgentData(data);
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
      wrapped.setOOBInline(on);
    }

    @Override
    public boolean getOOBInline() throws SocketException {
      return wrapped.getOOBInline();
    }

    @Override
    public synchronized void setSoTimeout(int timeout) throws SocketException {
      wrapped.setSoTimeout(timeout);
    }

    @Override
    public synchronized int getSoTimeout() throws SocketException {
      return wrapped.getSoTimeout();
    }

    @Override
    public synchronized void setSendBufferSize(int size)
        throws SocketException {
      wrapped.setSendBufferSize(size);
    }

    @Override
    public synchronized int getSendBufferSize() throws SocketException {
      return wrapped.getSendBufferSize();
    }

    @Override
    public synchronized void setReceiveBufferSize(int size)
        throws SocketException {
      wrapped.setReceiveBufferSize(size);
    }

    @Override
    public synchronized int getReceiveBufferSize() throws SocketException {
      return wrapped.getReceiveBufferSize();
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
      wrapped.setKeepAlive(on);
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
      return wrapped.getKeepAlive();
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
      wrapped.setTrafficClass(tc);
    }

    @Override
    public int getTrafficClass() throws SocketException {
      return wrapped.getTrafficClass();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
      wrapped.setReuseAddress(on);
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
      return wrapped.getReuseAddress();
    }

    @Override
    public synchronized void close() throws IOException {
      wrapped.close();
    }

    @Override
    public void shutdownInput() throws IOException {
      wrapped.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
      wrapped.shutdownOutput();
    }

    @Override
    public String toString() {
      return wrapped.toString();
    }

    @Override
    public boolean isConnected() {
      return wrapped.isConnected();
    }

    @Override
    public boolean isBound() {
      return wrapped.isBound();
    }

    @Override
    public boolean isClosed() {
      return wrapped.isClosed();
    }

    @Override
    public boolean isInputShutdown() {
      return wrapped.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown() {
      return wrapped.isOutputShutdown();
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency,
                                          int bandwidth) {
      wrapped.setPerformancePreferences(connectionTime, latency, bandwidth);
    }

    public boolean getLastTcpNoDelay() {
      return tcpNoDelay;
    }
  }
}
