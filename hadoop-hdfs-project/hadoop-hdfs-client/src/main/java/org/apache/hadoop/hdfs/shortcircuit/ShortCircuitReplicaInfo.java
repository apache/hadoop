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
package org.apache.hadoop.hdfs.shortcircuit;

import org.apache.hadoop.security.token.SecretManager.InvalidToken;

public final class ShortCircuitReplicaInfo {
  private final ShortCircuitReplica replica;
  private final InvalidToken exc; 

  public ShortCircuitReplicaInfo() {
    this.replica = null;
    this.exc = null;
  }

  public ShortCircuitReplicaInfo(ShortCircuitReplica replica) {
    this.replica = replica;
    this.exc = null;
  }

  public ShortCircuitReplicaInfo(InvalidToken exc) {
    this.replica = null;
    this.exc = exc;
  }

  public ShortCircuitReplica getReplica() {
    return replica;
  }

  public InvalidToken getInvalidTokenException() {
    return exc; 
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder();
    String prefix = "";
    builder.append("ShortCircuitReplicaInfo{");
    if (replica != null) {
      builder.append(prefix).append(replica);
      prefix = ", ";
    }
    if (exc != null) {
      builder.append(prefix).append(exc);
      prefix = ", ";
    }
    builder.append("}");
    return builder.toString();
  }
}