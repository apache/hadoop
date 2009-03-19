/**
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
package org.apache.hadoop.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * TODO: Most of the code in this class is ripped from ZooKeeper tests. Instead
 * of redoing it, we should contribute updates to their code which let us more
 * easily access testing helper objects.
 */
public class MiniZooKeeperCluster {
  private static final Log LOG = LogFactory.getLog(MiniZooKeeperCluster.class);

  // TODO: make this more configurable?
  private static final int CLIENT_PORT_START = 21810; // use non-standard port
  private static final int LEADER_PORT_START = 31810; // use non-standard port
  private static final int TICK_TIME = 2000;
  private static final int INIT_LIMIT = 3;
  private static final int SYNC_LIMIT = 3;
  private static final int CONNECTION_TIMEOUT = 30000;

  private boolean started;
  private int numPeers;
  private File baseDir;

  // for distributed mode.
  private QuorumPeer[] quorumPeers;
  // for standalone mode.
  private NIOServerCnxn.Factory standaloneServerFactory;

  /**
   * @throws IOException
   */
  public MiniZooKeeperCluster() throws IOException {
    this.started = false;
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

  /**
   * @param numPeers
   * @param baseDir
   * @throws IOException
   * @throws InterruptedException
   */
  public void startup(int numPeers, File baseDir) throws IOException,
      InterruptedException {
    setupTestEnv();

    shutdown();

    if (numPeers < 1) {
      return;
    }

    this.numPeers = numPeers;
    this.baseDir = baseDir.getAbsoluteFile();
    if (isDistributed()) {
      startupDistributed();
    } else {
      startupStandalone();
    }

    started = true;
  }

  private void startupStandalone() throws IOException, InterruptedException {
    File dir = new File(baseDir, "zookeeper-standalone");
    recreateDir(dir);

    ZooKeeperServer server = new ZooKeeperServer(dir, dir, TICK_TIME);
    standaloneServerFactory = new NIOServerCnxn.Factory(CLIENT_PORT_START);
    standaloneServerFactory.startup(server);

    ZooKeeperWrapper.setQuorumServers("localhost:" + CLIENT_PORT_START);

    if (!waitForServerUp(CLIENT_PORT_START, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }
  }

  // XXX: From o.a.zk.t.QuorumTest.startServers
  private void startupDistributed() throws IOException {
    // Create map of peers
    HashMap<Long, QuorumServer> peers = new HashMap<Long, QuorumServer>();
    for (int id = 1; id <= numPeers; ++id) {
      int port = LEADER_PORT_START + id;
      InetSocketAddress addr = new InetSocketAddress("localhost", port);
      QuorumServer server = new QuorumServer(id, addr);
      peers.put(Long.valueOf(id), server);
    }

    StringBuffer serversBuffer = new StringBuffer();

    // Initialize each quorum peer.
    quorumPeers = new QuorumPeer[numPeers];
    for (int id = 1; id <= numPeers; ++id) {
      File dir = new File(baseDir, "zookeeper-peer-" + id);
      recreateDir(dir);

      int port = CLIENT_PORT_START + id;
      quorumPeers[id - 1] = new QuorumPeer(peers, dir, dir, port, 0, id,
          TICK_TIME, INIT_LIMIT, SYNC_LIMIT);

      if (id > 1) {
        serversBuffer.append(",");
      }
      serversBuffer.append("localhost:" + port);
    }

    String servers = serversBuffer.toString();
    ZooKeeperWrapper.setQuorumServers(servers);

    // Start quorum peer threads.
    for (QuorumPeer qp : quorumPeers) {
      qp.start();
    }

    // Wait for quorum peers to be up before going on.
    for (int id = 1; id <= numPeers; ++id) {
      int port = CLIENT_PORT_START + id;
      if (!waitForServerUp(port, CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for startup of peer " + id);
      }
    }
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
   * @throws InterruptedException
   */
  public void shutdown() throws IOException, InterruptedException {
    if (!started) {
      return;
    }

    if (isDistributed()) {
      shutdownDistributed();
    } else {
      shutdownStandalone();
    }

    started = false;
  }

  private boolean isDistributed() {
    return numPeers > 1;
  }

  private void shutdownDistributed() throws IOException, InterruptedException {
    for (QuorumPeer qp : quorumPeers) {
      qp.shutdown();
      qp.join(CONNECTION_TIMEOUT);
      if (qp.isAlive()) {
        throw new IOException("QuorumPeer " + qp.getId()
            + " failed to shutdown");
      }
    }

    for (int id = 1; id <= quorumPeers.length; ++id) {
      int port = CLIENT_PORT_START + id;
      if (!waitForServerDown(port, CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for shutdown of peer " + id);
      }
    }
  }

  private void shutdownStandalone() throws IOException {
    standaloneServerFactory.shutdown();
    if (!waitForServerDown(CLIENT_PORT_START, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }
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
