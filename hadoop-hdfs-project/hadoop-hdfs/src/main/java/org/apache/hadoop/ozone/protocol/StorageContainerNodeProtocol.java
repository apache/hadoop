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
package org.apache.hadoop.ozone.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeReport;

import java.util.List;

/**
 * The protocol spoken between datanodes and SCM.
 *
 * Please note that the full protocol spoken between a datanode and SCM is
 * separated into 2 interfaces. One interface that deals with node state and
 * another interface that deals with containers.
 *
 * This interface has functions that deals with the state of datanode.
 */
@InterfaceAudience.Private
public interface StorageContainerNodeProtocol {
  /**
   * Gets the version info from SCM.
   * @param versionRequest - version Request.
   * @return - returns SCM version info and other required information needed
   * by datanode.
   */
  VersionResponse getVersion(SCMVersionRequestProto versionRequest);

  /**
   * Register the node if the node finds that it is not registered with any SCM.
   * @param datanodeID - Send datanodeID with Node info, but datanode UUID is
   *                   empty. Server returns a datanodeID for the given node.
   * @return  SCMHeartbeatResponseProto
   */
  SCMCommand register(DatanodeID datanodeID);

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   * @param datanodeID - Datanode ID.
   * @param nodeReport - node report.
   * @param reportState - container report.
   * @return SCMheartbeat response list
   */
  List<SCMCommand> sendHeartbeat(DatanodeID datanodeID,
      SCMNodeReport nodeReport, ReportState reportState);

}
