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
package org.apache.hadoop.ozone.protocol.commands;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto
    .ErrorCode;

/**
 * Response to Datanode Register call.
 */
public class RegisteredCommand {
  private String clusterID;
  private ErrorCode error;
  private DatanodeDetails datanode;

  public RegisteredCommand(final ErrorCode error, final DatanodeDetails node,
      final String clusterID) {
    this.datanode = node;
    this.clusterID = clusterID;
    this.error = error;
  }

  /**
   * Returns a new builder.
   *
   * @return - Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns datanode.
   *
   * @return - Datanode.
   */
  public DatanodeDetails getDatanode() {
    return datanode;
  }

  /**
   * Returns cluster ID.
   *
   * @return -- ClusterID
   */
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Returns ErrorCode.
   *
   * @return - ErrorCode
   */
  public ErrorCode getError() {
    return error;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  public SCMRegisteredResponseProto getProtoBufMessage() {
    SCMRegisteredResponseProto.Builder builder =
        SCMRegisteredResponseProto.newBuilder()
            // TODO : Fix this later when we have multiple SCM support.
            // .setAddressList(addressList)
            .setClusterID(this.clusterID)
            .setDatanodeUUID(this.datanode.getUuidString())
            .setErrorCode(this.error);
    if (!Strings.isNullOrEmpty(datanode.getHostName())) {
      builder.setHostname(datanode.getHostName());
    }
    if (!Strings.isNullOrEmpty(datanode.getIpAddress())) {
      builder.setIpAddress(datanode.getIpAddress());
    }
    if (!Strings.isNullOrEmpty(datanode.getNetworkName())) {
      builder.setNetworkName(datanode.getNetworkName());
    }
    if (!Strings.isNullOrEmpty(datanode.getNetworkLocation())) {
      builder.setNetworkLocation(datanode.getNetworkLocation());
    }

    return builder.build();
  }

  /**
   * A builder class to verify all values are sane.
   */
  public static class Builder {
    private DatanodeDetails datanode;
    private String clusterID;
    private ErrorCode error;

    /**
     * sets datanode details.
     *
     * @param node - datanode details
     * @return Builder
     */
    public Builder setDatanode(DatanodeDetails node) {
      this.datanode = node;
      return this;
    }

    /**
     * Sets cluster ID.
     *
     * @param cluster - clusterID
     * @return Builder
     */
    public Builder setClusterID(String cluster) {
      this.clusterID = cluster;
      return this;
    }

    /**
     * Sets Error code.
     *
     * @param errorCode - error code
     * @return Builder
     */
    public Builder setErrorCode(ErrorCode errorCode) {
      this.error = errorCode;
      return this;
    }

    /**
     * Build the command object.
     *
     * @return RegisteredCommand
     */
    public RegisteredCommand build() {
      if ((this.error == ErrorCode.success) && (this.datanode == null
          || Strings.isNullOrEmpty(this.datanode.getUuidString())
          || Strings.isNullOrEmpty(this.clusterID))) {
        throw new IllegalArgumentException("On success, RegisteredCommand "
            + "needs datanodeUUID and ClusterID.");
      }
      return new RegisteredCommand(this.error, this.datanode, this.clusterID);
    }
  }
}
