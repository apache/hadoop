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

import org.apache.hadoop.hdfs.server.federation.router.RemoteLocationContext;

/**
 * Represents information about a single nameservice/namespace in a federated
 * HDFS cluster.
 */
public class FederationNamespaceInfo
    implements Comparable<FederationNamespaceInfo>, RemoteLocationContext {

  /** Block pool identifier. */
  private String blockPoolId;
  /** Cluster identifier. */
  private String clusterId;
  /** Nameservice identifier. */
  private String nameserviceId;

  public FederationNamespaceInfo(String bpId, String clId, String nsId) {
    this.blockPoolId = bpId;
    this.clusterId = clId;
    this.nameserviceId = nsId;
  }

  /**
   * The HDFS nameservice id for this namespace.
   *
   * @return Nameservice identifier.
   */
  public String getNameserviceId() {
    return this.nameserviceId;
  }

  /**
   * The HDFS cluster id for this namespace.
   *
   * @return Cluster identifier.
   */
  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * The HDFS block pool id for this namespace.
   *
   * @return Block pool identifier.
   */
  public String getBlockPoolId() {
    return this.blockPoolId;
  }

  @Override
  public int hashCode() {
    return this.nameserviceId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof FederationNamespaceInfo) {
      return this.compareTo((FederationNamespaceInfo) obj) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(FederationNamespaceInfo info) {
    return this.nameserviceId.compareTo(info.getNameserviceId());
  }

  @Override
  public String toString() {
    return this.nameserviceId + "->" + this.blockPoolId + ":" + this.clusterId;
  }

  @Override
  public String getDest() {
    return this.nameserviceId;
  }
}