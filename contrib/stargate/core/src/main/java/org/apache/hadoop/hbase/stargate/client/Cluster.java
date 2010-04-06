/*
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

package org.apache.hadoop.hbase.stargate.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A list of 'host:port' addresses of HTTP servers operating as a single
 * entity, for example multiple redundant web service gateways.
 */
public class Cluster {
  protected List<String> nodes = 
    Collections.synchronizedList(new ArrayList<String>());
  protected String lastHost;

  /**
   * Constructor
   */
  public Cluster() {}

  /**
   * Constructor
   * @param nodes a list of service locations, in 'host:port' format
   */
  public Cluster(List<String> nodes) {
    nodes.addAll(nodes);
  }

  /**
   * @return true if no locations have been added, false otherwise
   */
  public boolean isEmpty() {
    return nodes.isEmpty();
  }

  /**
   * Add a node to the cluster
   * @param node the service location in 'host:port' format
   */
  public Cluster add(String node) {
    nodes.add(node);
    return this;
  }

  /**
   * Add a node to the cluster
   * @param name host name
   * @param port service port
   */
  public Cluster add(String name, int port) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append(':');
    sb.append(port);
    return add(sb.toString());
  }

  /**
   * Remove a node from the cluster
   * @param node the service location in 'host:port' format
   */
  public Cluster remove(String node) {
    nodes.remove(node);
    return this;
  }

  /**
   * Remove a node from the cluster
   * @param name host name
   * @param port service port
   */
  public Cluster remove(String name, int port) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append(':');
    sb.append(port);
    return remove(sb.toString());
  }
}
