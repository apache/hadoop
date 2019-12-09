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
 * A location in a remote namespace consisting of a nameservice ID and a HDFS
 * path (destination). It also contains the federated location (source).
 */
public class RemoteLocation extends RemoteLocationContext {

  /** Identifier of the remote namespace for this location. */
  private final String nameserviceId;
  /** Identifier of the namenode in the namespace for this location. */
  private final String namenodeId;
  /** Path in the remote location. */
  private final String dstPath;
  /** Original path in federation. */
  private final String srcPath;

  /**
   * Create a new remote location.
   *
   * @param nsId Destination namespace.
   * @param dPath Path in the destination namespace.
   * @param sPath Path in the federated level.
   */
  public RemoteLocation(String nsId, String dPath, String sPath) {
    this(nsId, null, dPath, sPath);
  }

  /**
   * Create a new remote location pointing to a particular namenode in the
   * namespace.
   *
   * @param nsId Destination namespace.
   * @param nnId Destination namenode.
   * @param dPath Path in the destination namespace.
   * @param sPath Path in the federated level
   */
  public RemoteLocation(String nsId, String nnId, String dPath, String sPath) {
    this.nameserviceId = nsId;
    this.namenodeId = nnId;
    this.dstPath = dPath;
    this.srcPath = sPath;
  }

  @Override
  public String getNameserviceId() {
    String ret = this.nameserviceId;
    if (this.namenodeId != null) {
      ret += "-" + this.namenodeId;
    }
    return ret;
  }

  @Override
  public String getDest() {
    return this.dstPath;
  }

  @Override
  public String getSrc() {
    return this.srcPath;
  }

  @Override
  public String toString() {
    return getNameserviceId() + "->" + this.dstPath;
  }
}