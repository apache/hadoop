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

import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;

import java.util.List;

/**
 * ClusterConnector interface hides all specifics about how we communicate to
 * the HDFS cluster. This interface returns data in classes that diskbalancer
 * understands.
 */
public interface ClusterConnector {

  /**
   * getNodes function returns a list of DiskBalancerDataNodes.
   *
   * @return Array of DiskBalancerDataNodes
   */
  List<DiskBalancerDataNode> getNodes() throws Exception;

  /**
   * Returns info about the connector.
   *
   * @return String.
   */
  String getConnectorInfo();
}
