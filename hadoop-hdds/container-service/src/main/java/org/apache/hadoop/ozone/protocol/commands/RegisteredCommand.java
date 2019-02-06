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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto
    .ErrorCode;

/**
 * Response to Datanode Register call.
 */
public class RegisteredCommand {
  private String datanodeUUID;
  private String clusterID;
  private ErrorCode error;
  private String hostname;
  private String ipAddress;

  public RegisteredCommand(final ErrorCode error, final String datanodeUUID,
      final String clusterID) {
    this(error, datanodeUUID, clusterID, null, null);
  }
  public RegisteredCommand(final ErrorCode error, final String datanodeUUID,
      final String clusterID, final String hostname, final String ipAddress) {
    this.datanodeUUID = datanodeUUID;
    this.clusterID = clusterID;
    this.error = error;
    this.hostname = hostname;
    this.ipAddress = ipAddress;
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
   * Returns datanode UUID.
   *
   * @return - Datanode ID.
   */
  public String getDatanodeUUID() {
    return datanodeUUID;
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
   * Returns the hostname.
   *
   * @return - hostname
   */
  public String getHostName() {
    return hostname;
  }

  /**
   * Returns the ipAddress of the dataNode.
   */
  public String getIpAddress() {
    return ipAddress;
  }

  /**
   * Gets the protobuf message of this object.
   *
   * @return A protobuf message.
   */
  public byte[] getProtoBufMessage() {
    SCMRegisteredResponseProto.Builder builder =
        SCMRegisteredResponseProto.newBuilder()
            .setClusterID(this.clusterID)
            .setDatanodeUUID(this.datanodeUUID)
            .setErrorCode(this.error);
    if (hostname != null && ipAddress != null) {
      builder.setHostname(hostname).setIpAddress(ipAddress);
    }
    return builder.build().toByteArray();
  }

  /**
   * A builder class to verify all values are sane.
   */
  public static class Builder {
    private String datanodeUUID;
    private String clusterID;
    private ErrorCode error;
    private String ipAddress;
    private String hostname;

    /**
     * sets UUID.
     *
     * @param dnUUID - datanode UUID
     * @return Builder
     */
    public Builder setDatanodeUUID(String dnUUID) {
      this.datanodeUUID = dnUUID;
      return this;
    }

    /**
     * Create this object from a Protobuf message.
     *
     * @param response - RegisteredCmdResponseProto
     * @return RegisteredCommand
     */
    public  RegisteredCommand getFromProtobuf(SCMRegisteredResponseProto
                                                        response) {
      Preconditions.checkNotNull(response);
      if (response.hasHostname() && response.hasIpAddress()) {
        return new RegisteredCommand(response.getErrorCode(),
            response.getDatanodeUUID(), response.getClusterID(),
            response.getHostname(), response.getIpAddress());
      } else {
        return new RegisteredCommand(response.getErrorCode(),
            response.getDatanodeUUID(), response.getClusterID());
      }
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
     * sets the hostname.
     */
    public Builder setHostname(String host) {
      this.hostname = host;
      return this;
    }

    public Builder setIpAddress(String ipAddr) {
      this.ipAddress = ipAddr;
      return this;
    }

    /**
     * Build the command object.
     *
     * @return RegisteredCommand
     */
    public RegisteredCommand build() {
      if ((this.error == ErrorCode.success) && (this.datanodeUUID == null
          || this.datanodeUUID.isEmpty()) || (this.clusterID == null
          || this.clusterID.isEmpty())) {
        throw new IllegalArgumentException("On success, RegisteredCommand "
            + "needs datanodeUUID and ClusterID.");
      }
      if (hostname != null && ipAddress != null) {
        return new RegisteredCommand(this.error, this.datanodeUUID,
            this.clusterID, this.hostname, this.ipAddress);
      } else {
        return new RegisteredCommand(this.error, this.datanodeUUID,
            this.clusterID);
      }
    }
  }
}
