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
package org.apache.hadoop.hdfs.qjournal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;

import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MiniQJMHACluster {
  private MiniDFSCluster cluster;
  private MiniJournalCluster journalCluster;
  private final Configuration conf;
  private static final Log LOG = LogFactory.getLog(MiniQJMHACluster.class);

  public static final String NAMESERVICE = "ns1";
  private static final Random RANDOM = new Random();

  public static class Builder {
    private final Configuration conf;
    private StartupOption startOpt = null;
    private int numNNs = 2;
    private final MiniDFSCluster.Builder dfsBuilder;

    public Builder(Configuration conf) {
      this.conf = conf;
      // most QJMHACluster tests don't need DataNodes, so we'll make
      // this the default
      this.dfsBuilder = new MiniDFSCluster.Builder(conf).numDataNodes(0);
    }

    public MiniDFSCluster.Builder getDfsBuilder() {
      return dfsBuilder;
    }

    public MiniQJMHACluster build() throws IOException {
      return new MiniQJMHACluster(this);
    }

    public void startupOption(StartupOption startOpt) {
      this.startOpt = startOpt;
    }

    public Builder setNumNameNodes(int nns) {
      this.numNNs = nns;
      return this;
    }
  }

  public static MiniDFSNNTopology createDefaultTopology(int nns, int startingPort) {
    MiniDFSNNTopology.NSConf nameservice = new MiniDFSNNTopology.NSConf(NAMESERVICE);
    for (int i = 0; i < nns; i++) {
      nameservice.addNN(new MiniDFSNNTopology.NNConf("nn" + i)
          .setIpcPort(startingPort++)
          .setServicePort(startingPort++)
          .setHttpPort(startingPort++));
    }

    return new MiniDFSNNTopology().addNameservice(nameservice);
  }

  public static MiniDFSNNTopology createDefaultTopology(int basePort) {
    return createDefaultTopology(2, basePort);
  }

  private MiniQJMHACluster(Builder builder) throws IOException {
    this.conf = builder.conf;
    int retryCount = 0;
    int basePort = 10000;

    while (true) {
      try {
        basePort = 10000 + RANDOM.nextInt(1000) * 4;
        LOG.info("Set MiniQJMHACluster basePort to " + basePort);
        // start 3 journal nodes
        journalCluster = new MiniJournalCluster.Builder(conf).format(true)
            .build();
        journalCluster.waitActive();
        journalCluster.setNamenodeSharedEditsConf(NAMESERVICE);
        URI journalURI = journalCluster.getQuorumJournalURI(NAMESERVICE);

        // start cluster with specified NameNodes
        MiniDFSNNTopology topology = createDefaultTopology(builder.numNNs, basePort);

        initHAConf(journalURI, builder.conf, builder.numNNs, basePort);

        // First start up the NNs just to format the namespace. The MinIDFSCluster
        // has no way to just format the NameNodes without also starting them.
        cluster = builder.dfsBuilder.nnTopology(topology)
            .manageNameDfsSharedDirs(false).build();
        cluster.waitActive();
        cluster.shutdownNameNodes();

        // initialize the journal nodes
        Configuration confNN0 = cluster.getConfiguration(0);
        NameNode.initializeSharedEdits(confNN0, true);

        for (MiniDFSCluster.NameNodeInfo nn : cluster.getNameNodeInfos()) {
          nn.setStartOpt(builder.startOpt);
        }

        // restart the cluster
        cluster.restartNameNodes();
        break;
      } catch (BindException e) {
        if (cluster != null) {
          cluster.shutdown(true);
          cluster = null;
        }
        ++retryCount;
        LOG.info("MiniQJMHACluster port conflicts, retried " +
            retryCount + " times");
      }
    }
  }

  private Configuration initHAConf(URI journalURI, Configuration conf,
      int numNNs, int basePort) {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
        journalURI.toString());

    List<String> nns = new ArrayList<String>(numNNs);
    int port = basePort;
    for (int i = 0; i < numNNs; i++) {
      nns.add("127.0.0.1:" + port);
      // increment by 3 each time to account for the http and the service port
      // in the config setting
      port += 3;
    }

    // use standard failover configurations
    HATestUtil.setFailoverConfigurations(conf, NAMESERVICE, nns);
    return conf;
  }

  public MiniDFSCluster getDfsCluster() {
    return cluster;
  }

  public MiniJournalCluster getJournalCluster() {
    return journalCluster;
  }

  public void shutdown() throws IOException {
    cluster.shutdown();
    journalCluster.shutdown();
  }
}
