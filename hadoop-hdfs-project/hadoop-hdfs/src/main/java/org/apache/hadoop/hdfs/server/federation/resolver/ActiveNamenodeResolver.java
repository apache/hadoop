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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Locates the most active NN for a given nameservice ID or blockpool ID. This
 * interface is used by the {@link org.apache.hadoop.hdfs.server.federation.
 * router.RouterRpcServer RouterRpcServer} to:
 * <ul>
 * <li>Determine the target NN for a given subcluster.
 * <li>List of all namespaces discovered/active in the federation.
 * <li>Update the currently active NN empirically.
 * </ul>
 * The interface is also used by the {@link org.apache.hadoop.hdfs.server.
 * federation.router.NamenodeHeartbeatService NamenodeHeartbeatService} to
 * register a discovered NN.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ActiveNamenodeResolver {

  /**
   * Report a successful, active NN address for a nameservice or blockPool.
   *
   * @param ns Nameservice identifier.
   * @param successfulAddress The address the successful responded to the
   *                          command.
   * @throws IOException If the state store cannot be accessed.
   */
  void updateActiveNamenode(
      String ns, InetSocketAddress successfulAddress) throws IOException;

  /**
   * Returns a prioritized list of the most recent cached registration entries
   * for a single nameservice ID.
   * Returns an empty list if none are found. Returns entries in preference of:
   * <ul>
   * <li>The most recent ACTIVE NN
   * <li>The most recent STANDBY NN
   * <li>The most recent UNAVAILABLE NN
   * </ul>
   *
   * @param nameserviceId Nameservice identifier.
   * @return Prioritized list of namenode contexts.
   * @throws IOException If the state store cannot be accessed.
   */
  List<? extends FederationNamenodeContext>
      getNamenodesForNameserviceId(String nameserviceId) throws IOException;

  /**
   * Returns a prioritized list of the most recent cached registration entries
   * for a single block pool ID.
   * Returns an empty list if none are found. Returns entries in preference of:
   * <ul>
   * <li>The most recent ACTIVE NN
   * <li>The most recent STANDBY NN
   * <li>The most recent UNAVAILABLE NN
   * </ul>
   *
   * @param blockPoolId Block pool identifier for the nameservice.
   * @return Prioritized list of namenode contexts.
   * @throws IOException If the state store cannot be accessed.
   */
  List<? extends FederationNamenodeContext>
      getNamenodesForBlockPoolId(String blockPoolId) throws IOException;

  /**
   * Register a namenode in the State Store.
   *
   * @param report Namenode status report.
   * @return True if the node was registered and successfully committed to the
   *         data store.
   * @throws IOException Throws exception if the namenode could not be
   *         registered.
   */
  boolean registerNamenode(NamenodeStatusReport report) throws IOException;

  /**
   * Get a list of all namespaces that are registered and active in the
   * federation.
   *
   * @return List of name spaces in the federation
   * @throws IOException Throws exception if the namespace list is not
   *         available.
   */
  Set<FederationNamespaceInfo> getNamespaces() throws IOException;

  /**
   * Assign a unique identifier for the parent router service.
   * Required to report the status to the namenode resolver.
   *
   * @param routerId Unique string identifier for the router.
   */
  void setRouterId(String routerId);
}
