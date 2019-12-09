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


package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * Exception indicating that DataNode does not have a replica
 * that matches the target block.
 */
public class ReplicaNotFoundException extends IOException {
  private static final long serialVersionUID = 1L;
  public final static String NON_RBW_REPLICA =
      "Cannot recover a non-RBW replica ";
  public final static String UNFINALIZED_REPLICA =
      "Cannot append to an unfinalized replica ";
  public final static String UNFINALIZED_AND_NONRBW_REPLICA =
      "Cannot recover append/close to a replica that's not FINALIZED and not RBW"
          + " ";
  public final static String NON_EXISTENT_REPLICA =
      "Replica does not exist ";
  public final static String UNEXPECTED_GS_REPLICA =
      "Cannot append to a replica with unexpected generation stamp ";

  public ReplicaNotFoundException() {
    super();
  }

  public ReplicaNotFoundException(ExtendedBlock b) {
    super("Replica not found for " + b);
  }

  public ReplicaNotFoundException(String msg) {
    super(msg);
  }
}
