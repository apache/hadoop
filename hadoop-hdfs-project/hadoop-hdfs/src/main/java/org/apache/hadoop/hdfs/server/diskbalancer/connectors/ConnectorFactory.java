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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Connector factory creates appropriate connector based on the URL.
 */
public final class ConnectorFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConnectorFactory.class);

  /**
   * Constructs an appropriate connector based on the URL.
   * @param clusterURI - URL
   * @return ClusterConnector
   */
  public static ClusterConnector getCluster(URI clusterURI, Configuration
      conf) throws IOException, URISyntaxException {
    LOG.debug("Cluster URI : {}" , clusterURI);
    LOG.debug("scheme : {}" , clusterURI.getScheme());
    if (clusterURI.getScheme().startsWith("file")) {
      LOG.debug("Creating a JsonNodeConnector");
      return new JsonNodeConnector(clusterURI.toURL());
    } else {
      LOG.debug("Creating NameNode connector");
      return new DBNameNodeConnector(clusterURI, conf);
    }
  }

  private ConnectorFactory() {
    // never constructed
  }
}
