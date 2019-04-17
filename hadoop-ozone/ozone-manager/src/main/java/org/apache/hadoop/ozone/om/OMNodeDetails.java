/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.net.NetUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * This class stores OM node details.
 */
public final class OMNodeDetails {
  private String omServiceId;
  private String omNodeId;
  private InetSocketAddress rpcAddress;
  private int rpcPort;
  private int ratisPort;

  /**
   * Constructs OMNodeDetails object.
   */
  private OMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int rpcPort, int ratisPort) {
    this.omServiceId = serviceId;
    this.omNodeId = nodeId;
    this.rpcAddress = rpcAddr;
    this.rpcPort = rpcPort;
    this.ratisPort = ratisPort;
  }

  /**
   * Builder class for OMNodeDetails.
   */
  public static class Builder {
    private String omServiceId;
    private String omNodeId;
    private InetSocketAddress rpcAddress;
    private int rpcPort;
    private int ratisPort;

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      this.rpcPort = rpcAddress.getPort();
      return this;
    }

    public Builder setRatisPort(int port) {
      this.ratisPort = port;
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder setOMNodeId(String nodeId) {
      this.omNodeId = nodeId;
      return this;
    }

    public OMNodeDetails build() {
      return new OMNodeDetails(omServiceId, omNodeId, rpcAddress, rpcPort,
          ratisPort);
    }
  }

  public String getOMServiceId() {
    return omServiceId;
  }

  public String getOMNodeId() {
    return omNodeId;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public InetAddress getAddress() {
    return rpcAddress.getAddress();
  }

  public int getRatisPort() {
    return ratisPort;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(rpcAddress);
  }
}
