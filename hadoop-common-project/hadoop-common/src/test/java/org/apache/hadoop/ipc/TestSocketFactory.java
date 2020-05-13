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
package org.apache.hadoop.ipc;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocksSocketFactory;
import org.apache.hadoop.net.StandardSocketFactory;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * test StandardSocketFactory and SocksSocketFactory NetUtils
 *
 */
public class TestSocketFactory {

  private static final int START_STOP_TIMEOUT_SEC = 30;

  private ServerRunnable serverRunnable;
  private Thread serverThread;
  private int port;

  private void startTestServer() throws Exception {
    // start simple tcp server.
    serverRunnable = new ServerRunnable();
    serverThread = new Thread(serverRunnable);
    serverThread.start();
    final long timeout = System.currentTimeMillis() + START_STOP_TIMEOUT_SEC * 1000;
    while (!serverRunnable.isReady()) {
      assertNull(serverRunnable.getThrowable());
      Thread.sleep(10);
      if (System.currentTimeMillis() > timeout) {
        fail("Server thread did not start properly in allowed time of "
            + START_STOP_TIMEOUT_SEC + " sec.");
      }
    }
    port = serverRunnable.getPort();
  }

  @After
  public void stopTestServer() throws InterruptedException {
    final Thread t = serverThread;
    if (t != null) {
      serverThread = null;
      port = -1;
      // stop server
      serverRunnable.stop();
      t.join(START_STOP_TIMEOUT_SEC * 1000);
      assertFalse(t.isAlive());
      assertNull(serverRunnable.getThrowable());
    }
  }

  @Test
  public void testSocketFactoryAsKeyInMap() {
    Map<SocketFactory, Integer> dummyCache = new HashMap<SocketFactory, Integer>();
    int toBeCached1 = 1;
    int toBeCached2 = 2;
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        "org.apache.hadoop.ipc.TestSocketFactory$DummySocketFactory");
    final SocketFactory dummySocketFactory = NetUtils
        .getDefaultSocketFactory(conf);
    dummyCache.put(dummySocketFactory, toBeCached1);

    conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        "org.apache.hadoop.net.StandardSocketFactory");
    final SocketFactory defaultSocketFactory = NetUtils
        .getDefaultSocketFactory(conf);
    dummyCache.put(defaultSocketFactory, toBeCached2);

    assertThat(dummyCache.size())
        .withFailMessage("The cache contains two elements")
        .isEqualTo(2);
    assertThat(defaultSocketFactory)
        .withFailMessage("Equals of both socket factory shouldn't be same")
        .isNotEqualTo(dummySocketFactory);

    assertSame(toBeCached2, dummyCache.remove(defaultSocketFactory));
    dummyCache.put(defaultSocketFactory, toBeCached2);
    assertSame(toBeCached1, dummyCache.remove(dummySocketFactory));

  }

  /**
   * A dummy socket factory class that extends the StandardSocketFactory.
   */
  static class DummySocketFactory extends StandardSocketFactory {

  }

  /**
   * Test SocksSocketFactory.
   */
  @Test (timeout=5000)
  public void testSocksSocketFactory() throws Exception {
    startTestServer();
    testSocketFactory(new SocksSocketFactory());
  }

  /**
   * Test StandardSocketFactory.
   */
  @Test (timeout=5000)
  public void testStandardSocketFactory() throws Exception {
    startTestServer();
    testSocketFactory(new StandardSocketFactory());
  }

  /*
   * Common test implementation.
   */
  private void testSocketFactory(SocketFactory socketFactory) throws Exception {
    assertNull(serverRunnable.getThrowable());

    InetAddress address = InetAddress.getLocalHost();
    Socket socket = socketFactory.createSocket(address, port);
    checkSocket(socket);
    socket.close();

    socket = socketFactory.createSocket(address, port,
        InetAddress.getLocalHost(), 0);
    checkSocket(socket);
    socket.close();

    socket = socketFactory.createSocket("localhost", port);
    checkSocket(socket);
    socket.close();

    socket = socketFactory.createSocket("localhost", port,
        InetAddress.getLocalHost(), 0);
    checkSocket(socket);
    socket.close();

  }

  /**
   * test proxy methods
   */
  @Test (timeout=5000)
  public void testProxy() throws Exception {
    SocksSocketFactory templateWithoutProxy = new SocksSocketFactory();
    Proxy proxy = new Proxy(Type.SOCKS, InetSocketAddress.createUnresolved(
        "localhost", 0));

    SocksSocketFactory templateWithProxy = new SocksSocketFactory(proxy);
    assertThat(templateWithoutProxy).isNotEqualTo(templateWithProxy);

    Configuration configuration = new Configuration();
    configuration.set("hadoop.socks.server", "localhost:0");

    templateWithoutProxy.setConf(configuration);
    assertThat(templateWithoutProxy).isEqualTo(templateWithProxy);
  }

  private void checkSocket(Socket socket) throws Exception {
    BufferedReader input = new BufferedReader(new InputStreamReader(
        socket.getInputStream()));
    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    out.writeBytes("test\n");
    String answer = input.readLine();
    assertThat(answer).isEqualTo("TEST");
  }

  /**
   * Simple tcp server. Server gets a string, transforms it to upper case and returns it.
   */
  private static class ServerRunnable implements Runnable {

    private volatile boolean works = true;
    private ServerSocket testSocket;
    private volatile boolean ready = false;
    private volatile Throwable throwable;
    private int port0;

    @Override
    public void run() {
      try {
        testSocket = new ServerSocket(0);
        port0 = testSocket.getLocalPort();
        ready = true;
        while (works) {
          try {
            Socket connectionSocket = testSocket.accept();
            BufferedReader input = new BufferedReader(new InputStreamReader(
                connectionSocket.getInputStream()));
            DataOutputStream out = new DataOutputStream(
                connectionSocket.getOutputStream());
            String inData = input.readLine();

            String outData = inData.toUpperCase() + "\n";
            out.writeBytes(outData);
          } catch (SocketException ignored) {

          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
        throwable = ioe;
      }
    }

    public void stop() {
      works = false;
      try {
        testSocket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public boolean isReady() {
      return ready;
    }

    public int getPort() {
      return port0;
    }
    
    public Throwable getThrowable() {
      return throwable;
    }
  }

}
