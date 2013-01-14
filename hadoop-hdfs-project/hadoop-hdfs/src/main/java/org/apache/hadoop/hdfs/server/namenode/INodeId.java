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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.SequentialNumber;

/**
 * An id which uniquely identifies an inode
 */
@InterfaceAudience.Private
class INodeId extends SequentialNumber {
  /**
   * The last reserved inode id. Reserve id 1 to 1000 for potential future
   * usage. The id won't be recycled and is not expected to wrap around in a
   * very long time. Root inode id will be 1001.
   */
  public static final long LAST_RESERVED_ID = 1000L;

  /**
   * The inode id validation of lease check will be skipped when the request
   * uses GRANDFATHER_INODE_ID for backward compatibility.
   */
  public static final long GRANDFATHER_INODE_ID = 0;

  INodeId() {
    super(LAST_RESERVED_ID);
  }
}
