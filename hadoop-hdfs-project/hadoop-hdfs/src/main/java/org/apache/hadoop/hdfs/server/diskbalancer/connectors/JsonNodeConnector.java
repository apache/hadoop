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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel
    .DiskBalancerDataNode;

import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * A connector that understands JSON data cluster models.
 */
public class JsonNodeConnector implements ClusterConnector {
  private static final Logger LOG =
      LoggerFactory.getLogger(JsonNodeConnector.class);
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(DiskBalancerCluster.class);
  private final URL clusterURI;

  /**
   * Constructs a JsonNodeConnector.
   * @param clusterURI - A file URL that contains cluster information.
   */
  public JsonNodeConnector(URL clusterURI) {
    this.clusterURI = clusterURI;
  }

  /**
   * getNodes function connects to a cluster definition file
   * and returns nodes defined in that file.
   *
   * @return Array of DiskBalancerDataNodes
   */
  @Override
  public List<DiskBalancerDataNode> getNodes() throws Exception {
    Preconditions.checkNotNull(this.clusterURI);
    String dataFilePath = this.clusterURI.getPath();
    LOG.info("Reading cluster info from file : " + dataFilePath);
    DiskBalancerCluster cluster = READER.readValue(new File(dataFilePath));
    String message = String.format("Found %d node(s)",
        cluster.getNodes().size());
    LOG.info(message);
    return cluster.getNodes();
  }

  /**
   * Returns info about the connector.
   *
   * @return String.
   */
  @Override
  public String getConnectorInfo() {
    return "Json Cluster Connector : Connects to a JSON file that describes a" +
        " cluster : " + clusterURI.toString();
  }
}
