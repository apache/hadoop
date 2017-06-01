/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.ratis;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.CheckedRunnable;
import org.apache.ratis.util.CheckedSupplier;
import org.apache.ratis.util.LifeCycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Implementation of {@link RatisManager}.
 */
public class RatisManagerImpl implements RatisManager {
  static final RaftPeer[] EMPTY_RARTPEER_ARRAY = {};

  static final class RatisCluster {
    private final String clusterId;
    private final LifeCycle state;
    private List<DatanodeID> datanodes;

    private RatisCluster(String clusterId, List<DatanodeID> datanodes) {
      this.clusterId = clusterId;
      this.state = new LifeCycle(toString());
      this.datanodes = Collections.unmodifiableList(new ArrayList<>(datanodes));
    }

    synchronized List<DatanodeID> getDatanodes() {
      return datanodes;
    }

    synchronized void setDatanodes(
        CheckedSupplier<List<DatanodeID>, IOException> update)
        throws IOException {
      state.assertCurrentState(LifeCycle.State.RUNNING);
      datanodes = Collections.unmodifiableList(update.get());
    }

    synchronized void init(CheckedRunnable<IOException> init)
        throws IOException {
      state.startAndTransition(() -> init.run());
    }

    synchronized void close(CheckedRunnable<IOException> close)
        throws IOException {
      state.checkStateAndClose(() -> close.run());
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + clusterId;
    }
  }

  static final class RatisInfo {
    private final RaftPeer peer;

    private RatisInfo(DatanodeID datanode) {
      this.peer = RatisHelper.toRaftPeer(datanode);
    }

    RaftPeer getPeer() {
      return peer;
    }
  }

  private final RpcType rpcType;
  private final Map<String, RatisCluster> clusters = new ConcurrentHashMap<>();
  private final Map<DatanodeID, RatisInfo> infos = new ConcurrentHashMap<>();

  RatisManagerImpl(String rpc) {
    rpcType = SupportedRpcType.valueOfIgnoreCase(rpc);
  }

  private RaftPeer getRaftPeer(DatanodeID datanode) {
    return infos.computeIfAbsent(datanode, RatisInfo::new).getPeer();
  }

  @Override
  public void createRatisCluster(String clusterId, List<DatanodeID> datanodes)
      throws IOException {
    final RatisCluster cluster = new RatisCluster(clusterId, datanodes);
    final RatisCluster returned = clusters.putIfAbsent(clusterId, cluster);
    if (returned != null) {
      throw new IOException("Cluster " + clusterId + " already exists.");
    }

    final RaftPeer[] newPeers = datanodes.stream().map(this::getRaftPeer)
        .toArray(RaftPeer[]::new);
    cluster.init(() -> reinitialize(datanodes, newPeers));
  }

  private void reinitialize(List<DatanodeID> datanodes, RaftPeer[] newPeers)
      throws IOException {
    if (datanodes.isEmpty()) {
      return;
    }

    IOException exception = null;
    for (DatanodeID d : datanodes) {
      try {
        reinitialize(d, newPeers);
      } catch (IOException ioe) {
        if (exception == null) {
          exception = new IOException(
              "Failed to reinitialize some of the RaftPeer(s)", ioe);
        } else {
          exception.addSuppressed(ioe);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  private void reinitialize(DatanodeID datanode, RaftPeer[] newPeers)
      throws IOException {
    final RaftPeer p = getRaftPeer(datanode);
    try (RaftClient client = RatisHelper.newRaftClient(rpcType, p)) {
      client.reinitialize(newPeers, p.getId());
    } catch (IOException ioe) {
      throw new IOException("Failed to reinitialize RaftPeer " + p
          + "(datanode=" + datanode + ")", ioe);
    }
  }

  @Override
  public void closeRatisCluster(String clusterId) throws IOException {
    final RatisCluster c = clusters.get(clusterId);
    if (c == null) {
      throw new IOException("Cluster " + clusterId + " not found.");
    }
    c.close(() -> reinitialize(c.getDatanodes(), EMPTY_RARTPEER_ARRAY));
  }

  @Override
  public List<DatanodeID> getDatanodes(String clusterId) throws IOException {
    return clusters.get(clusterId).getDatanodes();
  }

  @Override
  public void updateDatanodes(String clusterId, List<DatanodeID> newDNs)
      throws IOException {
    final RatisCluster c = clusters.get(clusterId);
    c.setDatanodes(() -> {
      final List<DatanodeID> oldDNs = c.getDatanodes();
      final RaftPeer[] newPeers = newDNs.stream().map(this::getRaftPeer)
          .toArray(RaftPeer[]::new);
      try (RaftClient client = newRaftClient(oldDNs)) {
        client.setConfiguration(newPeers);
      }

      final List<DatanodeID> notInOld = newDNs.stream().filter(oldDNs::contains)
          .collect(Collectors.toList());
      reinitialize(notInOld, newPeers);
      return newDNs;
    });
  }

  private RaftClient newRaftClient(List<DatanodeID> datanodes)
      throws IOException {
    final List<RaftPeer> peers = datanodes.stream().map(this::getRaftPeer)
        .collect(Collectors.toList());
    return RatisHelper.newRaftClient(rpcType, peers.get(0).getId(), peers);
  }
}
