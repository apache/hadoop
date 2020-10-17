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

package org.apache.hadoop.hdfs.server.diskbalancer.connectors;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * DBNameNodeConnector connects to Namenode and extracts information from a
 * given cluster.
 */
class DBNameNodeConnector implements ClusterConnector {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBNameNodeConnector.class);
  static final Path DISKBALANCER_ID_PATH = new Path("/system/diskbalancer.id");
  private final URI clusterURI;
  private final NameNodeConnector connector;

  /**
   * Constructs a DBNameNodeConnector.
   *
   * @param clusterURI - URL to connect to.
   */
  public DBNameNodeConnector(URI clusterURI, Configuration conf) throws
      IOException, URISyntaxException {

    // we don't care how many instances of disk balancers run.
    // The admission is controlled at the data node, where we will
    // execute only one plan at a given time.
    NameNodeConnector.setWrite2IdFile(false);

    try {
      connector = new NameNodeConnector("DiskBalancer",
          clusterURI, DISKBALANCER_ID_PATH, null, conf, 1);
    } catch (IOException ex) {
      LOG.error("Unable to connect to NameNode " + ex.toString());
      throw ex;
    }

    this.clusterURI = clusterURI;
  }

  /**
   * getNodes function returns a list of DiskBalancerDataNodes.
   *
   * @return Array of DiskBalancerDataNodes
   */
  @Override
  public List<DiskBalancerDataNode> getNodes() throws Exception {
    Preconditions.checkNotNull(this.connector);
    List<DiskBalancerDataNode> nodeList = new LinkedList<>();
    DatanodeStorageReport[] reports = this.connector
        .getLiveDatanodeStorageReport();

    for (DatanodeStorageReport report : reports) {
      DiskBalancerDataNode datanode = getBalancerNodeFromDataNode(
          report.getDatanodeInfo());
      getVolumeInfoFromStorageReports(datanode, report.getStorageReports());
      nodeList.add(datanode);
    }
    return nodeList;
  }

  /**
   * Returns info about the connector.
   *
   * @return String.
   */
  @Override
  public String getConnectorInfo() {
    return "Name Node Connector : " + clusterURI.toString();
  }

  /**
   * This function maps the required fields from DataNodeInfo to disk
   * BalancerDataNode.
   *
   * @param nodeInfo
   * @return DiskBalancerDataNode
   */
  private DiskBalancerDataNode
      getBalancerNodeFromDataNode(DatanodeInfo nodeInfo) {
    Preconditions.checkNotNull(nodeInfo);
    DiskBalancerDataNode dbDataNode = new DiskBalancerDataNode(nodeInfo
        .getDatanodeUuid());
    dbDataNode.setDataNodeIP(nodeInfo.getIpAddr());
    dbDataNode.setDataNodeName(nodeInfo.getHostName());
    dbDataNode.setDataNodePort(nodeInfo.getIpcPort());
    return dbDataNode;
  }

  /**
   * Reads the relevant fields from each storage volume and populate the
   * DiskBalancer Node.
   *
   * @param node    - Disk Balancer Node
   * @param reports - Array of StorageReport
   */
  private void getVolumeInfoFromStorageReports(DiskBalancerDataNode node,
                                               StorageReport[] reports)
      throws Exception {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(reports);
    for (StorageReport report : reports) {
      DatanodeStorage storage = report.getStorage();
      DiskBalancerVolume volume = new DiskBalancerVolume();
      volume.setCapacity(report.getCapacity());
      volume.setFailed(report.isFailed());
      volume.setUsed(report.getDfsUsed());

      // TODO : Should we do BlockPool level balancing at all ?
      // Does it make sense ? Balancer does do that. Right now
      // we only deal with volumes and not blockPools

      volume.setUuid(storage.getStorageID());

      // we will skip this volume for disk balancer if
      // it is read-only since we will not be able to delete
      // or if it is already failed.
      volume.setSkip((storage.getState() == DatanodeStorage.State
          .READ_ONLY_SHARED) || report.isFailed());
      volume.setStorageType(storage.getStorageType().name());
      volume.setIsTransient(storage.getStorageType().isTransient());
      node.addVolume(volume);
    }

  }
}
