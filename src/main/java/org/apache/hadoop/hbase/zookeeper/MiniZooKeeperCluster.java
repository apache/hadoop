/*
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;

/**
 * TODO: Most of the code in this class is ripped from ZooKeeper tests. Instead
 * of redoing it, we should contribute updates to their code which let us more
 * easily access testing helper objects.
 */
public class MiniZooKeeperCluster {
  private static final Log LOG = LogFactory.getLog(MiniZooKeeperCluster.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  private boolean started;

  private int defaultClientPort = 21818; // use non-standard port
  private int clientPort = defaultClientPort; 
  private int zooKeeperCandidateNum = 0;
  
  private NIOServerCnxn.Factory standaloneServerFactory;
  private int currentZooKeeper;
  private List<ZooKeeperServer> zooKeeperServers;
  private int tickTime = 0;

  /** Create mini ZooKeeper cluster. */
  public MiniZooKeeperCluster() {
    this.started = false;
    currentZooKeeper = -1;
    zooKeeperServers = new ArrayList();
  }

  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }

  public int getClientPort() {
    return clientPort;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }
  
  public int getZooKeeperCandidateNum() {
    return zooKeeperCandidateNum;
  }

  // / XXX: From o.a.zk.t.ClientBase
  private static void setupTestEnv() {
    // during the tests we run with 100K prealloc in the logs.
    // on windows systems prealloc of 64M was seen to take ~15seconds
    // resulting in test failure (client timeout on first session).
    // set env and directly in order to handle static init/gc issues
    System.setProperty("zookeeper.preAllocSize", "100");
    FileTxnLog.setPreallocSize(100);
  }
  
  public int startup(File baseDir) throws IOException,
  InterruptedException {
    return startup(baseDir,1);
  }

  /**
   * @param baseDir
   * @param numZooKeeperServers
   * @return ClientPort server bound to.
   * @throws IOException
   * @throws InterruptedException
   */
  public int startup(File baseDir, int numZooKeeperServers) throws IOException,
      InterruptedException {
    if (numZooKeeperServers <= 0)
      return -1;

    setupTestEnv();

    shutdown();
    
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_"+i).getAbsoluteFile();
      recreateDir(dir);
  
      int tickTimeToUse;
      if (this.tickTime > 0) {
        tickTimeToUse = this.tickTime;
      } else {
        tickTimeToUse = TICK_TIME;
      }
      ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTimeToUse);
      zooKeeperServers.add(server);
    }
    while (true) {
      try {
        standaloneServerFactory =
          new NIOServerCnxn.Factory(new InetSocketAddress(clientPort));
      } catch (BindException e) {
        LOG.info("Failed binding ZK Server to client port: " + clientPort);
        //this port is already in use. try to use another
        clientPort++;
        continue;
      }
      break;
    }
    currentZooKeeper = 0;
    standaloneServerFactory.startup(zooKeeperServers.get(currentZooKeeper));   
    if (!waitForServerUp(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }

    started = true;
    zooKeeperCandidateNum = numZooKeeperServers-1;
    LOG.info("Started MiniZK Server on client port: " + clientPort);
    return clientPort;
  }

  private void recreateDir(File dir) throws IOException {
    if (dir.exists()) {
      FileUtil.fullyDelete(dir);
    }
    try {
      dir.mkdirs();
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  /**
   * @throws IOException
   */
  public void shutdown() throws IOException {
    if (!started) {
      return;
    }

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    started = false;
    zooKeeperCandidateNum = 0;
    LOG.info("Shutdown MiniZK Server on client port: " + clientPort);
  }
  
  /**@return clientPort return clientPort if there is another ZooKeeper Candidate can run; return
   *         -1, if there is no candidates.
   * @throws IOException
   * @throws InterruptedException 
   */
  public int killCurrentZooKeeper() throws IOException, 
                                        InterruptedException {
    if (!started) {
      return -1;
    }
    // Shutdown the current one
    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }
    LOG.info("Kill the current MiniZK Server on client port: " 
        + clientPort);
    
    if (zooKeeperCandidateNum == 0) {
      return -1;
    }
    // Start another ZooKeeper Server
    clientPort = defaultClientPort;
    while (true) {
      try {
        standaloneServerFactory =
          new NIOServerCnxn.Factory(new InetSocketAddress(clientPort));
      } catch (BindException e) {
        LOG.info("Failed binding ZK Server to client port: " + clientPort);
        //this port is already in use. try to use another
        clientPort++;
        continue;
      }
      break;
    }
    currentZooKeeper = (++currentZooKeeper) % zooKeeperServers.size() ;
    standaloneServerFactory.startup(zooKeeperServers.get(currentZooKeeper));

    if (!waitForServerUp(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }
    
    started = true;
    zooKeeperCandidateNum--;
    LOG.info("Started another candidate MiniZK Server on client port: " + clientPort);
    return clientPort;
  }

  // XXX: From o.a.zk.t.ClientBase
  private static boolean waitForServerDown(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();
        } finally {
          sock.close();
        }
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

  // XXX: From o.a.zk.t.ClientBase
  private static boolean waitForServerUp(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket("localhost", port);
        BufferedReader reader = null;
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write("stat".getBytes());
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream());
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          sock.close();
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("server localhost:" + port + " not up " + e);
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
}
