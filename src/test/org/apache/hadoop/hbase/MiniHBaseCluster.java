/**
 * Copyright 2008 The Apache Software Foundation
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
import java.net.BindException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * This class creates a single process HBase cluster. One thread is run for
 * each server started.  Pass how many instances of a RegionServer you want
 * running in your cluster-in-a-single-jvm.  Its modeled on MiniDFSCluster.
 * Uses {@link LocalHBaseCluster}.  Will run on top of whatever the currently
 * configured FileSystem.
 */
public class MiniHBaseCluster implements HConstants {
  static final Log LOG = LogFactory.getLog(MiniHBaseCluster.class.getName());
  private Configuration conf;
  public LocalHBaseCluster hbaseCluster;

  /**
   * Start a MiniHBaseCluster. 
   * @param conf HBaseConfiguration to be used for cluster
   * @param numRegionServers initial number of region servers to start.
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int numRegionServers) 
  throws IOException {
    this.conf = conf;
    init(numRegionServers);
  }

  private void init(final int nRegionNodes) throws IOException {
    try {
      // start up a LocalHBaseCluster
      while (true) {
        try {
          hbaseCluster = new LocalHBaseCluster(conf, nRegionNodes);
          hbaseCluster.startup();
        } catch (BindException e) {
          //this port is already in use. try to use another (for multiple testing)
          int port = conf.getInt(MASTER_PORT, DEFAULT_MASTER_PORT);
          LOG.info("Failed binding Master to port: " + port, e);
          port++;
          conf.setInt(MASTER_PORT, port);
          continue;
        }
        break;
      }
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Starts a region server thread running
   *
   * @throws IOException
   * @return Name of regionserver started.
   */
  public String startRegionServer() throws IOException {
    LocalHBaseCluster.RegionServerThread t =
      this.hbaseCluster.addRegionServer();
    t.start();
    t.waitForServerOnline();
    return t.getName();
  }

  /**
   * @return Returns the rpc address actually used by the master server, because
   * the supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return this.hbaseCluster.getMaster().getMasterAddress();
  }

  /**
   * @return the HMaster
   */
  public HMaster getMaster() {
    return this.hbaseCluster.getMaster();
  }

  /**
   * Cause a region server to exit without cleaning up
   *
   * @param serverNumber  Used as index into a list.
   */
  public void abortRegionServer(int serverNumber) {
    HRegionServer server = getRegionServer(serverNumber);
    try {
      LOG.info("Aborting " + server.getHServerInfo().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    server.abort();
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @return the region server that was stopped
   */
  public LocalHBaseCluster.RegionServerThread stopRegionServer(int serverNumber) {
    return stopRegionServer(serverNumber, true);
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @param shutdownFS True is we are to shutdown the filesystem as part of this
   * regionserver's shutdown.  Usually we do but you do not want to do this if
   * you are running multiple regionservers in a test and you shut down one
   * before end of the test.
   * @return the region server that was stopped
   */
  public LocalHBaseCluster.RegionServerThread stopRegionServer(int serverNumber,
      final boolean shutdownFS) {
    LocalHBaseCluster.RegionServerThread server =
      hbaseCluster.getRegionServers().get(serverNumber);
    LOG.info("Stopping " + server.toString());
    if (!shutdownFS) {
      // Stop the running of the hdfs shutdown thread in tests.
      server.getRegionServer().setShutdownHDFS(false);
    }
    server.getRegionServer().stop();
    return server;
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(final int serverNumber) {
    return this.hbaseCluster.waitOnRegionServer(serverNumber);
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    this.hbaseCluster.join();
  }

  /**
   * Shut down the mini HBase cluster
   * @throws IOException 
   */
  public void shutdown() throws IOException {
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
    }
    HConnectionManager.deleteAllConnections(false);
  }

  /**
   * Call flushCache on all regions on all participating regionservers.
   * @throws IOException
   */
  public void flushcache() throws IOException {
    for (LocalHBaseCluster.RegionServerThread t:
        this.hbaseCluster.getRegionServers()) {
      for(HRegion r: t.getRegionServer().getOnlineRegions()) {
        r.flushcache();
      }
    }
  }

  /**
   * @return List of region server threads.
   */
  public List<LocalHBaseCluster.RegionServerThread> getRegionThreads() {
    return this.hbaseCluster.getRegionServers();
  }

  /**
   * Grab a numbered region server of your choice.
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return hbaseCluster.getRegionServer(serverNumber);
  }
}