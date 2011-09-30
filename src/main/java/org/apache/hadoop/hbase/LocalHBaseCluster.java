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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

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
 */
public class LocalHBaseCluster {
  static final Log LOG = LogFactory.getLog(LocalHBaseCluster.class);
  private final List<JVMClusterUtil.MasterThread> masterThreads =
    new CopyOnWriteArrayList<JVMClusterUtil.MasterThread>();
  private final List<JVMClusterUtil.RegionServerThread> regionThreads =
    new CopyOnWriteArrayList<JVMClusterUtil.RegionServerThread>();
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
  private final Configuration conf;
  private final Class<? extends HMaster> masterClass;
  private final Class<? extends HRegionServer> regionServerClass;

  /**
   * Constructor.
   * @param conf
   * @throws IOException
   */
  public LocalHBaseCluster(final Configuration conf)
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
  public LocalHBaseCluster(final Configuration conf, final int noRegionServers)
  throws IOException {
    this(conf, 1, noRegionServers, getMasterImplementation(conf),
        getRegionServerImplementation(conf));
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the active master
   * address.
   * @param noMasters Count of masters to start.
   * @param noRegionServers Count of regionservers to start.
   * @throws IOException
   */
  public LocalHBaseCluster(final Configuration conf, final int noMasters,
      final int noRegionServers)
  throws IOException {
    this(conf, noMasters, noRegionServers, getMasterImplementation(conf),
        getRegionServerImplementation(conf));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
    return (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       HRegionServer.class);
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HMaster> getMasterImplementation(final Configuration conf) {
    return (Class<? extends HMaster>)conf.getClass(HConstants.MASTER_IMPL,
       HMaster.class);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noMasters Count of masters to start.
   * @param noRegionServers Count of regionservers to start.
   * @param masterClass
   * @param regionServerClass
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public LocalHBaseCluster(final Configuration conf, final int noMasters,
    final int noRegionServers, final Class<? extends HMaster> masterClass,
    final Class<? extends HRegionServer> regionServerClass)
  throws IOException {
    this.conf = conf;
    // Always have masters and regionservers come up on port '0' so we don't
    // clash over default ports.
    conf.set(HConstants.MASTER_PORT, "0");
    conf.set(HConstants.REGIONSERVER_PORT, "0");
    // Start the HMasters.
    this.masterClass =
      (Class<? extends HMaster>)conf.getClass(HConstants.MASTER_IMPL,
          masterClass);
    for (int i = 0; i < noMasters; i++) {
      addMaster(new Configuration(conf), i);
    }
    // Start the HRegionServers.
    this.regionServerClass =
      (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       regionServerClass);

    for (int i = 0; i < noRegionServers; i++) {
      addRegionServer(new Configuration(conf), i);
    }
  }

  public JVMClusterUtil.RegionServerThread addRegionServer()
      throws IOException {
    return addRegionServer(new Configuration(conf), this.regionThreads.size());
  }

  public JVMClusterUtil.RegionServerThread addRegionServer(
      Configuration config, final int index)
  throws IOException {
    // Create each regionserver with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager.
    JVMClusterUtil.RegionServerThread rst =
      JVMClusterUtil.createRegionServerThread(config,
          this.regionServerClass, index);
    this.regionThreads.add(rst);
    return rst;
  }

  public JVMClusterUtil.RegionServerThread addRegionServer(
      final Configuration config, final int index, User user)
  throws IOException, InterruptedException {
    return user.runAs(
        new PrivilegedExceptionAction<JVMClusterUtil.RegionServerThread>() {
          public JVMClusterUtil.RegionServerThread run() throws Exception {
            return addRegionServer(config, index);
          }
        });
  }

  public JVMClusterUtil.MasterThread addMaster() throws IOException {
    return addMaster(new Configuration(conf), this.masterThreads.size());
  }

  public JVMClusterUtil.MasterThread addMaster(Configuration c, final int index)
  throws IOException {
    // Create each master with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager.
    JVMClusterUtil.MasterThread mt =
      JVMClusterUtil.createMasterThread(c,
        this.masterClass, index);
    this.masterThreads.add(mt);
    return mt;
  }

  public JVMClusterUtil.MasterThread addMaster(
      final Configuration c, final int index, User user)
  throws IOException, InterruptedException {
    return user.runAs(
        new PrivilegedExceptionAction<JVMClusterUtil.MasterThread>() {
          public JVMClusterUtil.MasterThread run() throws Exception {
            return addMaster(c, index);
          }
        });
  }

  /**
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return regionThreads.get(serverNumber).getRegionServer();
  }

  /**
   * @return Read-only list of region server threads.
   */
  public List<JVMClusterUtil.RegionServerThread> getRegionServers() {
    return Collections.unmodifiableList(this.regionThreads);
  }

  /**
   * @return List of running servers (Some servers may have been killed or
   * aborted during lifetime of cluster; these servers are not included in this
   * list).
   */
  public List<JVMClusterUtil.RegionServerThread> getLiveRegionServers() {
    List<JVMClusterUtil.RegionServerThread> liveServers =
      new ArrayList<JVMClusterUtil.RegionServerThread>();
    List<RegionServerThread> list = getRegionServers();
    for (JVMClusterUtil.RegionServerThread rst: list) {
      if (rst.isAlive()) liveServers.add(rst);
    }
    return liveServers;
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(int serverNumber) {
    JVMClusterUtil.RegionServerThread regionServerThread =
      this.regionThreads.remove(serverNumber);
    while (regionServerThread.isAlive()) {
      try {
        LOG.info("Waiting on " +
          regionServerThread.getRegionServer().toString());
        regionServerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return regionServerThread.getName();
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param rst
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(JVMClusterUtil.RegionServerThread rst) {
    while (rst.isAlive()) {
      try {
        LOG.info("Waiting on " +
          rst.getRegionServer().toString());
        rst.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (int i=0;i<regionThreads.size();i++) {
      if (regionThreads.get(i) == rst) {
        regionThreads.remove(i);
        break;
      }
    }
    return rst.getName();
  }

  /**
   * @param serverNumber
   * @return the HMaster thread
   */
  public HMaster getMaster(int serverNumber) {
    return masterThreads.get(serverNumber).getMaster();
  }

  /**
   * Gets the current active master, if available.  If no active master, returns
   * null.
   * @return the HMaster for the active master
   */
  public HMaster getActiveMaster() {
    for (JVMClusterUtil.MasterThread mt : masterThreads) {
      if (mt.getMaster().isActiveMaster()) {
        return mt.getMaster();
      }
    }
    return null;
  }

  /**
   * @return Read-only list of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasters() {
    return Collections.unmodifiableList(this.masterThreads);
  }

  /**
   * @return List of running master servers (Some servers may have been killed
   * or aborted during lifetime of cluster; these servers are not included in
   * this list).
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasters() {
    List<JVMClusterUtil.MasterThread> liveServers =
      new ArrayList<JVMClusterUtil.MasterThread>();
    List<JVMClusterUtil.MasterThread> list = getMasters();
    for (JVMClusterUtil.MasterThread mt: list) {
      if (mt.isAlive()) {
        liveServers.add(mt);
      }
    }
    return liveServers;
  }

  /**
   * Wait for the specified master to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of master that just went down.
   */
  public String waitOnMaster(int serverNumber) {
    JVMClusterUtil.MasterThread masterThread =
      this.masterThreads.remove(serverNumber);
    while (masterThread.isAlive()) {
      try {
        LOG.info("Waiting on " +
          masterThread.getMaster().getServerName().toString());
        masterThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return masterThread.getName();
  }

  /**
   * Wait for the specified master to stop
   * Removes this thread from list of running threads.
   * @param masterThread
   * @return Name of master that just went down.
   */
  public String waitOnMaster(JVMClusterUtil.MasterThread masterThread) {
    while (masterThread.isAlive()) {
      try {
        LOG.info("Waiting on " +
          masterThread.getMaster().getServerName().toString());
        masterThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (int i=0;i<masterThreads.size();i++) {
      if (masterThreads.get(i) == masterThread) {
        masterThreads.remove(i);
        break;
      }
    }
    return masterThread.getName();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   * Presumes you've already called {@link #shutdown()}.
   */
  public void join() {
    if (this.regionThreads != null) {
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
    if (this.masterThreads != null) {
      for (Thread t : this.masterThreads) {
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

  /**
   * Start the cluster.
   */
  public void startup() throws IOException {
    JVMClusterUtil.startup(this.masterThreads, this.regionThreads);
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    JVMClusterUtil.shutdown(this.masterThreads, this.regionThreads);
  }

  /**
   * @param c Configuration to check.
   * @return True if a 'local' address in hbase.master value.
   */
  public static boolean isLocal(final Configuration c) {
    final String mode = c.get(HConstants.CLUSTER_DISTRIBUTED);
    return mode == null || mode.equals(HConstants.CLUSTER_IS_LOCAL);
  }

  /**
   * Test things basically work.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    LocalHBaseCluster cluster = new LocalHBaseCluster(conf);
    cluster.startup();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor htd =
      new HTableDescriptor(Bytes.toBytes(cluster.getClass().getName()));
    admin.createTable(htd);
    cluster.shutdown();
  }
}