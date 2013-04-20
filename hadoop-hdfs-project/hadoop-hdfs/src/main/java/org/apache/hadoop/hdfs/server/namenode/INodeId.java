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

import java.io.FileNotFoundException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.SequentialNumber;

/**
 * An id which uniquely identifies an inode. Id 1 to 1000 are reserved for
 * potential future usage. The id won't be recycled and is not expected to wrap
 * around in a very long time. Root inode id is always 1001. Id 0 is used for
 * backward compatibility support.
 */
@InterfaceAudience.Private
public class INodeId extends SequentialNumber {
  /**
   * The last reserved inode id. 
   */
  public static final long LAST_RESERVED_ID = 1000L;

  /**
   * The inode id validation of lease check will be skipped when the request
   * uses GRANDFATHER_INODE_ID for backward compatibility.
   */
  public static final long GRANDFATHER_INODE_ID = 0;

  /**
   * To check if the request id is the same as saved id. Don't check fileId
   * with GRANDFATHER_INODE_ID for backward compatibility.
   */
  public static void checkId(long requestId, INode inode)
      throws FileNotFoundException {
    if (requestId != GRANDFATHER_INODE_ID && requestId != inode.getId()) {
      throw new FileNotFoundException(
          "ID mismatch. Request id and saved id: " + requestId + " , "
              + inode.getId());
    }
  }
  
  INodeId() {
    super(LAST_RESERVED_ID);
  }
}
