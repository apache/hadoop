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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Abstract base class for HBase cluster junit tests.  Spins up an hbase
 * cluster in setup and tears it down again in tearDown.
 * @deprecated Use junit4 and {@link HBaseTestingUtility}
 */
public abstract class HBaseClusterTestCase extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(HBaseClusterTestCase.class);
  public MiniHBaseCluster cluster;
  protected MiniDFSCluster dfsCluster;
  protected MiniZooKeeperCluster zooKeeperCluster;
  protected int regionServers;
  protected boolean startDfs;
  private boolean openMetaTable = true;

  /** default constructor */
  public HBaseClusterTestCase() {
    this(1);
  }

  /**
   * Start a MiniHBaseCluster with regionServers region servers in-process to
   * start with. Also, start a MiniDfsCluster before starting the hbase cluster.
   * The configuration used will be edited so that this works correctly.
   * @param regionServers number of region servers to start.
   */
  public HBaseClusterTestCase(int regionServers) {
    this(regionServers, true);
  }

  /**  in-process to
   * start with. Optionally, startDfs indicates if a MiniDFSCluster should be
   * started. If startDfs is false, the assumption is that an external DFS is
   * configured in hbase-site.xml and is already started, or you have started a
   * MiniDFSCluster on your own and edited the configuration in memory. (You
   * can modify the config used by overriding the preHBaseClusterSetup method.)
   * @param regionServers number of region servers to start.
   * @param startDfs set to true if MiniDFS should be started
   */
  public HBaseClusterTestCase(int regionServers, boolean startDfs) {
    super();
    this.startDfs = startDfs;
    this.regionServers = regionServers;
  }

  protected void setOpenMetaTable(boolean val) {
    openMetaTable = val;
  }

  /**
   * Subclass hook.
   *
   * Run after dfs is ready but before hbase cluster is started up.
   */
  protected void preHBaseClusterSetup() throws Exception {
    // continue
  }

  /**
   * Actually start the MiniHBase instance.
   */
  protected void hBaseClusterSetup() throws Exception {
    File testDir = new File(getUnitTestdir(getName()).toString());

    // Note that this is done before we create the MiniHBaseCluster because we
    // need to edit the config to add the ZooKeeper servers.
    this.zooKeeperCluster = new MiniZooKeeperCluster();
    int clientPort = this.zooKeeperCluster.startup(testDir);
    conf.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));
    Configuration c = new Configuration(this.conf);
    // start the mini cluster
    this.cluster = new MiniHBaseCluster(c, regionServers);

    if (openMetaTable) {
      // opening the META table ensures that cluster is running
      new HTable(c, HConstants.META_TABLE_NAME);
    }
  }

  /**
   * Run after hbase cluster is started up.
   */
  protected void postHBaseClusterSetup() throws Exception {
    // continue
  }

  @Override
  protected void setUp() throws Exception {
    try {
      if (this.startDfs) {
        // This spews a bunch of warnings about missing scheme. TODO: fix.
        this.dfsCluster = new MiniDFSCluster(0, this.conf, 2, true, true, true,
          null, null, null, null);

        // mangle the conf so that the fs parameter points to the minidfs we
        // just started up
        FileSystem filesystem = dfsCluster.getFileSystem();
        conf.set("fs.defaultFS", filesystem.getUri().toString());
        Path parentdir = filesystem.getHomeDirectory();
        conf.set(HConstants.HBASE_DIR, parentdir.toString());
        filesystem.mkdirs(parentdir);
        FSUtils.setVersion(filesystem, parentdir);
      }

      // do the super setup now. if we had done it first, then we would have
      // gotten our conf all mangled and a local fs started up.
      super.setUp();

      // run the pre-cluster setup
      preHBaseClusterSetup();

      // start the instance
      hBaseClusterSetup();

      // run post-cluster setup
      postHBaseClusterSetup();
    } catch (Exception e) {
      LOG.error("Exception in setup!", e);
      if (cluster != null) {
        cluster.shutdown();
      }
      if (zooKeeperCluster != null) {
        zooKeeperCluster.shutdown();
      }
      if (dfsCluster != null) {
        shutdownDfs(dfsCluster);
      }
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (!openMetaTable) {
      // open the META table now to ensure cluster is running before shutdown.
      new HTable(conf, HConstants.META_TABLE_NAME);
    }
    super.tearDown();
    try {
      HConnectionManager.deleteConnection(conf, true);
      if (this.cluster != null) {
        try {
          this.cluster.shutdown();
        } catch (Exception e) {
          LOG.warn("Closing mini dfs", e);
        }
        try {
          this.zooKeeperCluster.shutdown();
        } catch (IOException e) {
          LOG.warn("Shutting down ZooKeeper cluster", e);
        }
      }
      if (startDfs) {
        shutdownDfs(dfsCluster);
      }
    } catch (Exception e) {
      LOG.error(e);
    }
    // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
    //  "Temporary end-of-test thread dump debugging HADOOP-2040: " + getName());
  }


  /**
   * Use this utility method debugging why cluster won't go down.  On a
   * period it throws a thread dump.  Method ends when all cluster
   * regionservers and master threads are no long alive.
   */
  public void threadDumpingJoin() {
    if (this.cluster.getRegionServerThreads() != null) {
      for(Thread t: this.cluster.getRegionServerThreads()) {
        threadDumpingJoin(t);
      }
    }
    threadDumpingJoin(this.cluster.getMaster());
  }

  protected void threadDumpingJoin(final Thread t) {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Continuing...", e);
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }
}
