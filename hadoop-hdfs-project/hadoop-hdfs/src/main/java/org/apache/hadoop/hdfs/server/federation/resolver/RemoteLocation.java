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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RemoteLocationContext;

/**
 * A single in a remote namespace consisting of a nameservice ID
 * and a HDFS path.
 */
public class RemoteLocation implements RemoteLocationContext {

  /** Identifier of the remote namespace for this location. */
  private String nameserviceId;
  /** Path in the remote location. */
  private String path;

  /**
   * Create a new remote location.
   *
   * @param nsId Destination namespace.
   * @param pPath Path in the destination namespace.
   */
  public RemoteLocation(String nsId, String pPath) {
    this.nameserviceId = nsId;
    this.path = pPath;
  }

  @Override
  public String getNameserviceId() {
    return this.nameserviceId;
  }

  @Override
  public String getDest() {
    return this.path;
  }

  @Override
  public String toString() {
    return this.nameserviceId + "->" + this.path;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(this.nameserviceId)
        .append(this.path)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj != null &&
        obj.getClass() == this.getClass() &&
        obj.hashCode() == this.hashCode());
  }
}