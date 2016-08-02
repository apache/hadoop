/*
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

package org.apache.slider.api;

import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.api.types.PingInformation;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;

import java.io.IOException;
import java.util.Map;

/**
 * API exported by the slider remote REST/IPC endpoints.
 */
public interface SliderApplicationApi {
  /**
   * Get the aggregate desired model
   * @return the aggregate configuration of what was asked for
   * -before resolution has taken place
   * @throws IOException on any failure
   */
  AggregateConf getDesiredModel() throws IOException;

  /**
   * Get the desired application configuration
   * @return the application configuration asked for
   * -before resolution has taken place
   * @throws IOException on any failure
   */
  ConfTreeOperations getDesiredAppconf() throws IOException;

  /**
   * Get the desired YARN resources
   * @return the resources asked for
   * -before resolution has taken place
   * @throws IOException on any failure
   */
  ConfTreeOperations getDesiredResources() throws IOException;

  /**
   * Put an updated resources structure. This triggers a cluster flex
   * operation
   * @param updated updated resources
   * @throws IOException on any problem.
   */
  void putDesiredResources(ConfTree updated) throws IOException;

  /**
   * Get the aggregate resolved model
   * @return the aggregate configuration of what was asked for
   * -after resolution has taken place
   * @throws IOException on any failure
   */
  AggregateConf getResolvedModel() throws IOException;

  /**
   * Get the resolved application configuration
   * @return the application configuration asked for
   * -after resolution has taken place
   * @throws IOException on any failure
   */
  ConfTreeOperations getResolvedAppconf() throws IOException;

  /**
   * Get the resolved YARN resources
   * @return the resources asked for
   * -after resolution has taken place
   * @throws IOException on any failure
   */
  ConfTreeOperations getResolvedResources() throws IOException;

  /**
   * Get the live YARN resources
   * @return the live set of resources in the cluster
   * @throws IOException on any failure
   */
  ConfTreeOperations getLiveResources() throws IOException;

  /**
   * Get a map of live containers [containerId:info]
   * @return a possibly empty list of serialized containers
   * @throws IOException on any failure
   */
  Map<String, ContainerInformation> enumContainers() throws IOException;

  /**
   * Get a container from the container Id
   * @param containerId YARN container ID
   * @return the container information
   * @throws IOException on any failure
   */
  ContainerInformation getContainer(String containerId) throws IOException;

  /**
   * List all components into a map of [name:info]
   * @return a possibly empty map of components
   * @throws IOException on any failure
   */
  Map<String, ComponentInformation> enumComponents() throws IOException;

  /**
   * Get information about a component
   * @param componentName name of the component
   * @return the component details
   * @throws IOException on any failure
   */
  ComponentInformation getComponent(String componentName) throws IOException;

  /**
   * List all nodes into a map of [name:info]
   * @return a possibly empty list of nodes
   * @throws IOException on any failure
   */
  NodeInformationList getLiveNodes() throws IOException;

  /**
   * Get information about a node
   * @param hostname name of the node
   * @return the node details
   * @throws IOException on any failure
   */
  NodeInformation getLiveNode(String hostname) throws IOException;

  /**
   * Ping as a GET
   * @param text text to include
   * @return the response
   * @throws IOException on any failure
   */
  PingInformation ping(String text) throws IOException;

  /**
   * Stop the AM (async operation)
   * @param text text to include
   * @throws IOException on any failure
   */
  void stop(String text) throws IOException;

  /**
   * Get the application liveness
   * @return current liveness information
   * @throws IOException
   */
  ApplicationLivenessInformation getApplicationLiveness() throws IOException;
}
