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
package org.apache.hadoop.hdfs;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This class is used to specify the setup of namenodes when instantiating
 * a MiniDFSCluster. It consists of a set of nameservices, each of which
 * may have one or more namenodes (in the case of HA)
 */
@InterfaceAudience.LimitedPrivate({"HBase", "HDFS", "Hive", "MapReduce", "Pig"})
@InterfaceStability.Unstable
public class MiniDFSNNTopology {
  private final List<NSConf> nameservices = Lists.newArrayList();
  private boolean federation;

  public MiniDFSNNTopology() {
  }

  /**
   * Set up a simple non-federated non-HA NN.
   */
  public static MiniDFSNNTopology simpleSingleNN(
      int nameNodePort, int nameNodeHttpPort) {
    return new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf(null)
        .addNN(new MiniDFSNNTopology.NNConf(null)
          .setHttpPort(nameNodeHttpPort)
          .setIpcPort(nameNodePort)));
  }
  

  /**
   * Set up an HA topology with a single HA nameservice.
   */
  public static MiniDFSNNTopology simpleHATopology() {
    return simpleHATopology(2);
  }

  /**
   * Set up an HA topology with a single HA nameservice.
   * @param nnCount of namenodes to use with the nameservice
   */
  public static MiniDFSNNTopology simpleHATopology(int nnCount) {
    MiniDFSNNTopology.NSConf nameservice = new MiniDFSNNTopology.NSConf("minidfs-ns");
    for (int i = 1; i <= nnCount; i++) {
      nameservice.addNN(new MiniDFSNNTopology.NNConf("nn" + i));
    }
    MiniDFSNNTopology topology = new MiniDFSNNTopology().addNameservice(nameservice);
    return topology;
  }

  /**
   * Set up an HA topology with a single HA nameservice.
   * @param nnCount of namenodes to use with the nameservice
   * @param basePort for IPC and Http ports of namenodes.
   */
  public static MiniDFSNNTopology simpleHATopology(int nnCount, int basePort) {
    MiniDFSNNTopology.NSConf ns = new MiniDFSNNTopology.NSConf("minidfs-ns");
    for (int i = 0; i < nnCount; i++) {
      ns.addNN(new MiniDFSNNTopology.NNConf("nn" + i)
          .setIpcPort(basePort++)
          .setHttpPort(basePort++));
    }
    MiniDFSNNTopology topology = new MiniDFSNNTopology()
        .addNameservice(ns);
    return topology;
  }

  /**
   * Set up federated cluster with the given number of nameservices, each
   * of which has only a single NameNode.
   */
  public static MiniDFSNNTopology simpleFederatedTopology(
      int numNameservices) {
    MiniDFSNNTopology topology = new MiniDFSNNTopology();
    for (int i = 1; i <= numNameservices; i++) {
      topology.addNameservice(new MiniDFSNNTopology.NSConf("ns" + i)
        .addNN(new MiniDFSNNTopology.NNConf(null)));
    }
    topology.setFederation(true);
    return topology;
  }

  /**
   * Set up federated cluster with the given nameservices, each
   * of which has only a single NameNode.
   */
  public static MiniDFSNNTopology simpleFederatedTopology(String nameservicesIds) {
    MiniDFSNNTopology topology = new MiniDFSNNTopology();
    String nsIds[] = nameservicesIds.split(",");
    for (String nsId : nsIds) {
      topology.addNameservice(new MiniDFSNNTopology.NSConf(nsId)
        .addNN(new MiniDFSNNTopology.NNConf(null)));
    }
    topology.setFederation(true);
    return topology;
  }

  /**
   * Set up federated cluster with the given number of nameservices, each
   * of which has two NameNodes.
   */
  public static MiniDFSNNTopology simpleHAFederatedTopology(
      int numNameservices) {
    MiniDFSNNTopology topology = new MiniDFSNNTopology();
    for (int i = 0; i < numNameservices; i++) {
      topology.addNameservice(new MiniDFSNNTopology.NSConf("ns" + i)
        .addNN(new MiniDFSNNTopology.NNConf("nn0"))
        .addNN(new MiniDFSNNTopology.NNConf("nn1")));
    }
    topology.setFederation(true);
    return topology;
  }

  public MiniDFSNNTopology setFederation(boolean federation) {
    this.federation = federation;
    return this;
  }

  public MiniDFSNNTopology addNameservice(NSConf nameservice) {
    Preconditions.checkArgument(!nameservice.getNNs().isEmpty(),
        "Must have at least one NN in a nameservice");
    this.nameservices.add(nameservice);
    return this;
  }

  public int countNameNodes() {
    int count = 0;
    for (NSConf ns : nameservices) {
      count += ns.nns.size();
    }
    return count;
  }
  
  public NNConf getOnlyNameNode() {
    Preconditions.checkState(countNameNodes() == 1,
        "must have exactly one NN!");
    return nameservices.get(0).getNNs().get(0);
  }

  public boolean isFederated() {
    return nameservices.size() > 1 || federation;
  }
  
  /**
   * @return true if at least one of the nameservices
   * in the topology has HA enabled.
   */
  public boolean isHA() {
    for (NSConf ns : nameservices) {
      if (ns.getNNs().size() > 1) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if all of the NNs in the cluster have their HTTP
   * port specified to be non-ephemeral.
   */
  public boolean allHttpPortsSpecified() {
    for (NSConf ns : nameservices) {
      for (NNConf nn : ns.getNNs()) {
        if (nn.getHttpPort() == 0) {
          return false;
        }
      }
    }
    return true;
  }
  
  /**
   * @return true if all of the NNs in the cluster have their IPC
   * port specified to be non-ephemeral.
   */
  public boolean allIpcPortsSpecified() {
    for (NSConf ns : nameservices) {
      for (NNConf nn : ns.getNNs()) {
        if (nn.getIpcPort() == 0) {
          return false;
        }
      }
    }
    return true;
  }

  public List<NSConf> getNameservices() {
    return nameservices;
  }
  
  public static class NSConf {
    private final String id;
    private final List<NNConf> nns = Lists.newArrayList();
    
    public NSConf(String id) {
      this.id = id;
    }
    
    public NSConf addNN(NNConf nn) {
      this.nns.add(nn);
      return this;
    }

    public String getId() {
      return id;
    }

    public List<NNConf> getNNs() {
      return nns;
    }
  }
  
  public static class NNConf {
    private final String nnId;
    private int httpPort;
    private int ipcPort;
    private String clusterId;
    
    public NNConf(String nnId) {
      this.nnId = nnId;
    }

    String getNnId() {
      return nnId;
    }

    int getIpcPort() {
      return ipcPort;
    }
    
    int getHttpPort() {
      return httpPort;
    }

    String getClusterId() {
      return clusterId;
    }

    public NNConf setHttpPort(int httpPort) {
      this.httpPort = httpPort;
      return this;
    }

    public NNConf setIpcPort(int ipcPort) {
      this.ipcPort = ipcPort;
      return this;
    }

    public NNConf setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }
  }

}
