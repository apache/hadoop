/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

/**
 * This class creates a single process HBase cluster. One thread is created for
 * a master and one per region server.
 * 
 * Call {@link #startup()} to start the cluster running and {@link #shutdown()}
 * to close it all down. {@link #join} the cluster is you want to wait on
 * shutdown completion.
 * 
 * <p>Runs master on port 60000 by default.  Because we can't just kill the
 * process -- not till HADOOP-1700 gets fixed and even then.... -- we need to
 * be able to find the master with a remote client to run shutdown.  To use a
 * port other than 60000, set the hbase.master to a value of 'local:PORT':
 * that is 'local', not 'localhost', and the port number the master should use
 * instead of 60000.
 * 
 * <p>To make 'local' mode more responsive, make values such as
 * <code>hbase.regionserver.msginterval</code>,
 * <code>hbase.master.meta.thread.rescanfrequency</code>, and
 * <code>hbase.server.thread.wakefrequency</code> a second or less.
 */
public class LocalHBaseCluster implements HConstants {
  static final Log LOG = LogFactory.getLog(LocalHBaseCluster.class);
  private final HMaster master;
  private final List<RegionServerThread> regionThreads;
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
  private final HBaseConfiguration conf;
  private final Class<? extends HRegionServer> regionServerClass;

  /**
   * Constructor.
   * @param conf
   * @throws IOException
   */
  public LocalHBaseCluster(final HBaseConfiguration conf)
  throws IOException {
    this(conf, DEFAULT_NO);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noRegionServers Count of regionservers to start.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public LocalHBaseCluster(final HBaseConfiguration conf,
    final int noRegionServers)
  throws IOException {
    super();
    this.conf = conf;
    doLocal(conf);
    // Create the master
    this.master = new HMaster(conf);
    // Set the master's port for the HRegionServers
    conf.set(MASTER_ADDRESS, this.master.getMasterAddress().toString());
    // Start the HRegionServers.  Always have region servers come up on
    // port '0' so there won't be clashes over default port as unit tests
    // start/stop ports at different times during the life of the test.
    conf.set(REGIONSERVER_ADDRESS, DEFAULT_HOST + ":0");
    this.regionThreads = new ArrayList<RegionServerThread>();
    regionServerClass = (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);
    for (int i = 0; i < noRegionServers; i++) {
      addRegionServer();
    }
  }

  /**
   * Creates a region server.
   * Call 'start' on the returned thread to make it run.
   *
   * @throws IOException
   * @return Region server added.
   */
  public RegionServerThread addRegionServer() throws IOException {
    synchronized (regionThreads) {
      HRegionServer server; 
      try {
        server = regionServerClass.getConstructor(HBaseConfiguration.class).
          newInstance(conf);
      } catch (Exception e) {
        IOException ioe = new IOException();
        ioe.initCause(e);
        throw ioe;
      }
      RegionServerThread t = new RegionServerThread(server,
          this.regionThreads.size());
      this.regionThreads.add(t);
      return t;
    }
  }

  /**
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    synchronized (regionThreads) {
      return regionThreads.get(serverNumber).getRegionServer();
    }
  }

  /** runs region servers */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;
    
    RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RegionServer:" + index);
      this.regionServer = r;
    }

    /** @return the region server */
    public HRegionServer getRegionServer() {
      return this.regionServer;
    }
    
    /**
     * Block until the region server has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      while (!regionServer.isOnline()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue waiting
        }
      }
    }
  }

  /**
   * @return the HMaster thread
   */
  public HMaster getMaster() {
    return this.master;
  }

  /**
   * @return Read-only list of region server threads.
   */
  public List<RegionServerThread> getRegionServers() {
    return Collections.unmodifiableList(this.regionThreads);
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(int serverNumber) {
    RegionServerThread regionServerThread;
    synchronized (regionThreads) {
      regionServerThread = this.regionThreads.remove(serverNumber);
    }
    while (regionServerThread.isAlive()) {
      try {
        LOG.info("Waiting on " +
          regionServerThread.getRegionServer().getServerInfo().toString());
        regionServerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return regionServerThread.getName();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   * Presumes you've already called {@link #shutdown()}.
   */
  public void join() {
    if (this.regionThreads != null) {
      synchronized(this.regionThreads) {
        for(Thread t: this.regionThreads) {
          if (t.isAlive()) {
            try {
              t.join();
            } catch (InterruptedException e) {
              // continue
            }
          }
        }
      }
    }
    if (this.master != null && this.master.isAlive()) {
      try {
        this.master.join();
      } catch(InterruptedException e) {
        // continue
      }
    }
  }
  
  /**
   * Start the cluster.
   * @return Address to use contacting master.
   */
  public String startup() {
    this.master.start();
    synchronized (regionThreads) {
      for (RegionServerThread t: this.regionThreads) {
        t.start();
      }
    }
    return this.master.getMasterAddress().toString();
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    LOG.debug("Shutting down HBase Cluster");
    // Be careful how the hdfs shutdown thread runs in context where more than
    // one regionserver in the mix.
    Thread shutdownThread = null;
    synchronized (this.regionThreads) {
      for (RegionServerThread t: this.regionThreads) {
        Thread tt = t.getRegionServer().setHDFSShutdownThreadOnExit(null);
        if (shutdownThread == null && tt != null) {
          shutdownThread = tt;
        }
      }
    }
    if(this.master != null) {
      this.master.shutdown();
    }
    // regionServerThreads can never be null because they are initialized when
    // the class is constructed.
    synchronized(this.regionThreads) {
      for(Thread t: this.regionThreads) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    if (this.master != null) {
      while (this.master.isAlive()) {
        try {
          // The below has been replaced to debug sometime hangs on end of
          // tests.
          // this.master.join():
          threadDumpingJoin(this.master);
        } catch(InterruptedException e) {
          // continue
        }
      }
    }
    Threads.shutdown(shutdownThread);
    LOG.info("Shutdown " +
      ((this.regionThreads != null)? this.master.getName(): "0 masters") +
      " " + this.regionThreads.size() + " region server(s)");
  }

  /**
   * @param t
   * @throws InterruptedException
   */
  public void threadDumpingJoin(final Thread t) throws InterruptedException {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }

  /**
   * Changes <code>hbase.master</code> from 'local' to 'localhost:PORT' in
   * passed Configuration instance.
   * @param c
   * @return The passed <code>c</code> configuration modified if hbase.master
   * value was 'local' otherwise, unaltered.
   */
  public static HBaseConfiguration doLocal(final HBaseConfiguration c) {
    if (!isLocal(c)) {
      return c;
    }
    // Need to rewrite address in Configuration if not done already.
    String address = c.get(MASTER_ADDRESS);
    if (address == null) {
      throw new NullPointerException("Address is null for " + MASTER_ADDRESS);
    }
    String port = address.startsWith(LOCAL_COLON)?
      address.substring(LOCAL_COLON.length()):
      Integer.toString(DEFAULT_MASTER_PORT);
    c.set(MASTER_ADDRESS, "localhost:" + port);
    return c;
  }
  
  /**
   * @param c Configuration to check.
   * @return True if a 'local' address in hbase.master value.
   */
  public static boolean isLocal(final Configuration c) {
    String address = c.get(MASTER_ADDRESS);
    return address == null || address.equals(LOCAL) ||
      address.startsWith(LOCAL_COLON);
  }
  
  /**
   * Test things basically work.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    HBaseConfiguration conf = new HBaseConfiguration();
    LocalHBaseCluster cluster = new LocalHBaseCluster(conf);
    cluster.startup();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor htd =
      new HTableDescriptor(Bytes.toBytes(cluster.getClass().getName()));
    admin.createTable(htd);
    cluster.shutdown();
  }
}
