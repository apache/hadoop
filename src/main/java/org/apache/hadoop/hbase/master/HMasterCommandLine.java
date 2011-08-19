/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ServerCommandLine;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;

public class HMasterCommandLine extends ServerCommandLine {
  private static final Log LOG = LogFactory.getLog(HMasterCommandLine.class);

  private static final String USAGE =
    "Usage: Master [opts] start|stop\n" +
    " start  Start Master. If local mode, start Master and RegionServer in same JVM\n" +
    " stop   Start cluster shutdown; Master signals RegionServer shutdown\n" +
    " where [opts] are:\n" +
    "   --minServers=<servers>    Minimum RegionServers needed to host user tables.\n" +
    "   --backup                  Master should start in backup mode";

  private final Class<? extends HMaster> masterClass;

  public HMasterCommandLine(Class<? extends HMaster> masterClass) {
    this.masterClass = masterClass;
  }

  protected String getUsage() {
    return USAGE;
  }


  public int run(String args[]) throws Exception {
    Options opt = new Options();
    opt.addOption("minServers", true, "Minimum RegionServers needed to host user tables");
    opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");


    CommandLine cmd;
    try {
      cmd = new GnuParser().parse(opt, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      usage(null);
      return -1;
    }


    if (cmd.hasOption("minServers")) {
      String val = cmd.getOptionValue("minServers");
      getConf().setInt("hbase.regions.server.count.min",
                  Integer.valueOf(val));
      LOG.debug("minServers set to " + val);
    }

    // check if we are the backup master - override the conf if so
    if (cmd.hasOption("backup")) {
      getConf().setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
    }

    List<String> remainingArgs = cmd.getArgList();
    if (remainingArgs.size() != 1) {
      usage(null);
      return -1;
    }

    String command = remainingArgs.get(0);

    if ("start".equals(command)) {
      return startMaster();
    } else if ("stop".equals(command)) {
      return stopMaster();
    } else {
      usage("Invalid command: " + command);
      return -1;
    }
  }

  private int startMaster() {
    Configuration conf = getConf();
    try {
      // If 'local', defer to LocalHBaseCluster instance.  Starts master
      // and regionserver both in the one JVM.
      if (LocalHBaseCluster.isLocal(conf)) {
        final MiniZooKeeperCluster zooKeeperCluster =
          new MiniZooKeeperCluster();
        File zkDataPath = new File(conf.get("hbase.zookeeper.property.dataDir"));
        int zkClientPort = conf.getInt("hbase.zookeeper.property.clientPort", 0);
        if (zkClientPort == 0) {
          throw new IOException("No config value for hbase.zookeeper.property.clientPort");
        }
        zooKeeperCluster.setDefaultClientPort(zkClientPort);
        int clientPort = zooKeeperCluster.startup(zkDataPath);
        if (clientPort != zkClientPort) {
          String errorMsg = "Couldnt start ZK at requested address of " +
            zkClientPort + ", instead got: " + clientPort + ". Aborting. Why? " +
            "Because clients (eg shell) wont be able to find this ZK quorum";
          System.err.println(errorMsg);
          throw new IOException(errorMsg);
        }
        conf.set("hbase.zookeeper.property.clientPort",
                 Integer.toString(clientPort));
        // Need to have the zk cluster shutdown when master is shutdown.
        // Run a subclass that does the zk cluster shutdown on its way out.
        LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1, 1,
                                                          LocalHMaster.class, HRegionServer.class);
        ((LocalHMaster)cluster.getMaster(0)).setZKCluster(zooKeeperCluster);
        cluster.startup();
        waitOnMasterThreads(cluster);
      } else {
        HMaster master = HMaster.constructMaster(masterClass, conf);
        if (master.isStopped()) {
          LOG.info("Won't bring the Master up as a shutdown is requested");
          return -1;
        }
        master.start();
        master.join();
        if(master.isAborted())
          throw new RuntimeException("HMaster Aborted");
      }
    } catch (Throwable t) {
      LOG.error("Failed to start master", t);
      return -1;
    }
    return 0;
  }

  private int stopMaster() {
    HBaseAdmin adm = null;
    try {
      Configuration conf = getConf();
      // Don't try more than once
      conf.setInt("hbase.client.retries.number", 1);
      adm = new HBaseAdmin(getConf());
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running");
      return -1;
    } catch (ZooKeeperConnectionException e) {
      LOG.error("ZooKeeper not available");
      return -1;
    }
    try {
      adm.shutdown();
    } catch (Throwable t) {
      LOG.error("Failed to stop master", t);
      return -1;
    }
    return 0;
  }

  private void waitOnMasterThreads(LocalHBaseCluster cluster) throws InterruptedException{
    List<JVMClusterUtil.MasterThread> masters = cluster.getMasters();
    List<JVMClusterUtil.RegionServerThread> regionservers = cluster.getRegionServers();
	  
    if (masters != null) { 
      for (JVMClusterUtil.MasterThread t : masters) {
        t.join();
        if(t.getMaster().isAborted()) {
          closeAllRegionServerThreads(regionservers);
          throw new RuntimeException("HMaster Aborted");
        }
      }
    }
  }

  private static void closeAllRegionServerThreads(List<JVMClusterUtil.RegionServerThread> regionservers) {
    for(JVMClusterUtil.RegionServerThread t : regionservers){
      t.getRegionServer().stop("HMaster Aborted; Bringing down regions servers");
    }
  }
  
  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  public static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf)
    throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }
}
