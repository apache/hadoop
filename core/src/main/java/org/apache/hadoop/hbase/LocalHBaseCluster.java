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
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
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
 * <p>To make 'local' mode more responsive, make values such as
 * <code>hbase.regionserver.msginterval</code>,
 * <code>hbase.master.meta.thread.rescanfrequency</code>, and
 * <code>hbase.server.thread.wakefrequency</code> a second or less.
 */
public class LocalHBaseCluster implements HConstants {
  static final Log LOG = LogFactory.getLog(LocalHBaseCluster.class);
  private final HMaster master;
  private final List<JVMClusterUtil.RegionServerThread> regionThreads;
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
  private final Configuration conf;
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
    this(conf, noRegionServers, HMaster.class, getRegionServerImplementation(conf));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
    return (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       HRegionServer.class);
  }

  /**
   * Constructor.
   * @param conf Configuration to use.  Post construction has the master's
   * address.
   * @param noRegionServers Count of regionservers to start.
   * @param masterClass
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public LocalHBaseCluster(final Configuration conf,
    final int noRegionServers, final Class<? extends HMaster> masterClass,
    final Class<? extends HRegionServer> regionServerClass)
  throws IOException {
    this.conf = conf;
    // Create the master
    this.master = HMaster.constructMaster(masterClass, conf);
    // Start the HRegionServers.  Always have region servers come up on
    // port '0' so there won't be clashes over default port as unit tests
    // start/stop ports at different times during the life of the test.
    conf.set(REGIONSERVER_PORT, "0");
    this.regionThreads =
      new CopyOnWriteArrayList<JVMClusterUtil.RegionServerThread>();
    this.regionServerClass =
      (Class<? extends HRegionServer>)conf.getClass(HConstants.REGION_SERVER_IMPL,
       regionServerClass);
    for (int i = 0; i < noRegionServers; i++) {
      addRegionServer(i);
    }
  }

  public JVMClusterUtil.RegionServerThread addRegionServer() throws IOException {
    return addRegionServer(this.regionThreads.size());
  }

  public JVMClusterUtil.RegionServerThread addRegionServer(final int index) throws IOException {
    JVMClusterUtil.RegionServerThread rst = JVMClusterUtil.createRegionServerThread(this.conf,
        this.regionServerClass, index);
    this.regionThreads.add(rst);
    return rst;
  }

  /**
   * @param serverNumber
   * @return region server
   */
  public HRegionServer getRegionServer(int serverNumber) {
    return regionThreads.get(serverNumber).getRegionServer();
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
  public List<JVMClusterUtil.RegionServerThread> getRegionServers() {
    return Collections.unmodifiableList(this.regionThreads);
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
          regionServerThread.getRegionServer().getHServerInfo().toString());
        regionServerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IOException e) {
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
   */
  public void startup() {
    JVMClusterUtil.startup(this.master, this.regionThreads);
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    JVMClusterUtil.shutdown(this.master, this.regionThreads);
  }

  /**
   * @param c Configuration to check.
   * @return True if a 'local' address in hbase.master value.
   */
  public static boolean isLocal(final Configuration c) {
    String mode = c.get(CLUSTER_DISTRIBUTED);
    return mode == null || mode.equals(CLUSTER_IS_LOCAL);
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
