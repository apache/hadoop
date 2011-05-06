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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
  
  private List<NIOServerCnxn.Factory> standaloneServerFactoryList;
  private List<ZooKeeperServer> zooKeeperServers;
  private List<Integer> clientPortList;
  
  private int activeZKServerIndex;
  private int tickTime = 0;

  private Configuration configuration;

  /** Create mini ZooKeeper cluster. */
  public MiniZooKeeperCluster() {
    this(HBaseConfiguration.create());
  }

  /** Create mini ZooKeeper cluster. */
  public MiniZooKeeperCluster(Configuration configuration) {
    this.started = false;
    this.configuration = configuration;
    activeZKServerIndex = -1;
    zooKeeperServers = new ArrayList<ZooKeeperServer>();
    clientPortList = new ArrayList<Integer>();
    standaloneServerFactoryList = new ArrayList<NIOServerCnxn.Factory>();
  }

  public void setDefaultClientPort(int clientPort) {
    this.defaultClientPort = clientPort;
  }

  public int getDefaultClientPort() {
    return defaultClientPort;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }
  
  public int getBackupZooKeeperServerNum() {
    return zooKeeperServers.size()-1;
  }
  
  public int getZooKeeperServerNum() {
    return zooKeeperServers.size();
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
    
    // running all the ZK servers
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_"+i).getAbsoluteFile();
      recreateDir(dir);
      clientPort = defaultClientPort;
      int tickTimeToUse;
      if (this.tickTime > 0) {
        tickTimeToUse = this.tickTime;
      } else {
        tickTimeToUse = TICK_TIME;
      }
      int numberOfConnections = this.configuration.getInt("hbase.zookeeper.property.maxClientCnxns",5000);
      ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTimeToUse);    
      NIOServerCnxn.Factory standaloneServerFactory;
      while (true) {
        try {
          standaloneServerFactory =
            new NIOServerCnxn.Factory(new InetSocketAddress(clientPort), numberOfConnections);
        } catch (BindException e) {
          LOG.info("Failed binding ZK Server to client port: " + clientPort);
          //this port is already in use. try to use another
          clientPort++;
          continue;
        }
        break;
      }
      
      // Start up this ZK server
      standaloneServerFactory.startup(server);  
      if (!waitForServerUp(clientPort, CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for startup of standalone server");
      }
      
      clientPortList.add(clientPort);
      standaloneServerFactoryList.add(standaloneServerFactory);
      zooKeeperServers.add(server);
    }
    
    // set the first one to be active ZK; Others are backups
    activeZKServerIndex = 0;
    started = true;
    clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Started MiniZK Cluster and connect 1 ZK server " +
    		"on client port: " + clientPort);
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
    // shut down all the zk servers
    for (int i = 0; i < standaloneServerFactoryList.size(); i++) {
      NIOServerCnxn.Factory standaloneServerFactory = 
        standaloneServerFactoryList.get(i);      
      int clientPort = clientPortList.get(i);
      
      standaloneServerFactory.shutdown();
      if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for shutdown of standalone server");
      }
    }

    // clear everything
    started = false;
    activeZKServerIndex = 0;
    standaloneServerFactoryList.clear();
    clientPortList.clear();
    zooKeeperServers.clear();
    
    LOG.info("Shutdown MiniZK cluster with all ZK servers");
  }
  
  /**@return clientPort return clientPort if there is another ZK backup can run
   *         when killing the current active; return -1, if there is no backups.
   * @throws IOException
   * @throws InterruptedException 
   */
  public int killCurrentActiveZooKeeperServer() throws IOException, 
                                        InterruptedException {
    if (!started || activeZKServerIndex < 0 ) {
      return -1;
    }
    
    // Shutdown the current active one
    NIOServerCnxn.Factory standaloneServerFactory = 
      standaloneServerFactoryList.get(activeZKServerIndex);
    int clientPort = clientPortList.get(activeZKServerIndex);
    
    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }
    
    // remove the current active zk server
    standaloneServerFactoryList.remove(activeZKServerIndex);
    clientPortList.remove(activeZKServerIndex);
    zooKeeperServers.remove(activeZKServerIndex);    
    LOG.info("Kill the current active ZK servers in the cluster " +
        "on client port: " + clientPort);
    
    if (standaloneServerFactoryList.size() == 0) {
      // there is no backup servers;
      return -1;
    }
    clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Activate a backup zk server in the cluster " +
        "on client port: " + clientPort);
    // return the next back zk server's port
    return clientPort;
  }
  
  /**
   * Kill one back up ZK servers
   * @throws IOException
   * @throws InterruptedException 
   */
  public void killOneBackupZooKeeperServer() throws IOException, 
                                        InterruptedException {
    if (!started || activeZKServerIndex < 0 || 
        standaloneServerFactoryList.size() <= 1) {
      return ;
    }
    
    int backupZKServerIndex = activeZKServerIndex+1;
    // Shutdown the current active one
    NIOServerCnxn.Factory standaloneServerFactory = 
      standaloneServerFactoryList.get(backupZKServerIndex);
    int clientPort = clientPortList.get(backupZKServerIndex);
    
    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }
    
    // remove this backup zk server
    standaloneServerFactoryList.remove(backupZKServerIndex);
    clientPortList.remove(backupZKServerIndex);
    zooKeeperServers.remove(backupZKServerIndex);    
    LOG.info("Kill one backup ZK servers in the cluster " +
        "on client port: " + clientPort);
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
