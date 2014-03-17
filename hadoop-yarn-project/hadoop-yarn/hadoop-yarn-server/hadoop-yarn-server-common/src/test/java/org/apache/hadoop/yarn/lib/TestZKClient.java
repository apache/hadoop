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

package org.apache.hadoop.yarn.lib;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Assert;

import org.apache.hadoop.yarn.lib.ZKClient;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZKClient  {

  public static int CONNECTION_TIMEOUT = 30000;
  static final File BASETEST =
    new File(System.getProperty("build.test.dir", "target/zookeeper-build"));

  protected String hostPort = "127.0.0.1:2000";
  protected int maxCnxns = 0;
  protected NIOServerCnxnFactory factory = null;
  protected ZooKeeperServer zks;
  protected File tmpDir = null;

  public static String send4LetterWord(String host, int port, String cmd)
  throws IOException
  {
    Socket sock = new Socket(host, port);
    BufferedReader reader = null;
    try {
      OutputStream outstream = sock.getOutputStream();
      outstream.write(cmd.getBytes());
      outstream.flush();
      // this replicates NC - close the output stream before reading
      sock.shutdownOutput();

      reader =
        new BufferedReader(
            new InputStreamReader(sock.getInputStream()));
      StringBuilder sb = new StringBuilder();
      String line;
      while((line = reader.readLine()) != null) {
        sb.append(line + "\n");
      }
      return sb.toString();
    } finally {
      sock.close();
      if (reader != null) {
        reader.close();
      }
    }
  }

  public static boolean waitForServerDown(String hp, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        String host = hp.split(":")[0];
        int port = Integer.parseInt(hp.split(":")[1]);
        send4LetterWord(host, port, "stat");
      } catch (IOException e) {
        return true;
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }


  public static boolean waitForServerUp(String hp, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        String host = hp.split(":")[0];
        int port = Integer.parseInt(hp.split(":")[1]);
        // if there are multiple hostports, just take the first one
        String result = send4LetterWord(host, port, "stat");
        if (result.startsWith("Zookeeper version:")) {
          return true;
        }
      } catch (IOException e) {
      }
      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  public static File createTmpDir(File parentDir) throws IOException {
    File tmpFile = File.createTempFile("test", ".junit", parentDir);
    // don't delete tmpFile - this ensures we don't attempt to create
    // a tmpDir with a duplicate name
    File tmpDir = new File(tmpFile + ".dir");
    Assert.assertFalse(tmpDir.exists()); 
    Assert.assertTrue(tmpDir.mkdirs());
    return tmpDir;
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    System.setProperty("zookeeper.preAllocSize", "100");
    FileTxnLog.setPreallocSize(100 * 1024);
    if (!BASETEST.exists()) {
      BASETEST.mkdirs();
    }
    File dataDir = createTmpDir(BASETEST);
    zks = new ZooKeeperServer(dataDir, dataDir, 3000);
    final int PORT = Integer.parseInt(hostPort.split(":")[1]);
    if (factory == null) {
      factory = new NIOServerCnxnFactory();
      factory.configure(new InetSocketAddress(PORT), maxCnxns);
    }
    factory.startup(zks);
    Assert.assertTrue("waiting for server up",
        waitForServerUp("127.0.0.1:" + PORT,
            CONNECTION_TIMEOUT));
    
  }
  
  @After
  public void tearDown() throws IOException, InterruptedException {
    if (zks != null) {
      ZKDatabase zkDb = zks.getZKDatabase();
      factory.shutdown();
      try {
        zkDb.close();
      } catch (IOException ie) {
      }
      final int PORT = Integer.parseInt(hostPort.split(":")[1]);

      Assert.assertTrue("waiting for server down",
          waitForServerDown("127.0.0.1:" + PORT,
              CONNECTION_TIMEOUT));
    }

  }
  @Test
  public void testzkClient() throws Exception {
    test("/some/test");
  }

  private void test(String testClient) throws Exception { 
    ZKClient client = new ZKClient(hostPort);
    client.registerService("/nodemanager", "hostPort");
    client.unregisterService("/nodemanager");
  }

}
