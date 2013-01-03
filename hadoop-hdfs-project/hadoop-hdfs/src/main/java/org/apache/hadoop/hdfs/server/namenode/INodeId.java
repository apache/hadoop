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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * An id which uniquely identifies an inode
 */
@InterfaceAudience.Private
class INodeId implements Comparable<INodeId> {
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

  private AtomicLong lastInodeId = new AtomicLong();

  /**
   * Create a new instance, initialized to LAST_RESERVED_ID.
   */
  INodeId() {
    lastInodeId.set(INodeId.LAST_RESERVED_ID);
  }
  
  /**
   * Set the last allocated inode id when fsimage is loaded or editlog is
   * applied.
   * @throws IOException
   */
  void resetLastInodeId(long newValue) throws IOException {
    if (newValue < getLastInodeId()) {
      throw new IOException(
          "Can't reset lastInodeId to be less than its current value "
              + getLastInodeId() + ", newValue=" + newValue);
    }

    lastInodeId.set(newValue);
  }

  void resetLastInodeIdWithoutChecking(long newValue) {
    lastInodeId.set(newValue);
  }

  long getLastInodeId() {
    return lastInodeId.get();
  }

  /**
   * First increment the counter and then get the id.
   */
  long allocateNewInodeId() {
    return lastInodeId.incrementAndGet();
  }

  @Override
  // Comparable
  public int compareTo(INodeId that) {
    long id1 = this.getLastInodeId();
    long id2 = that.getLastInodeId();
    return id1 < id2 ? -1 : id1 > id2 ? 1 : 0;
  }

  @Override
  // Object
  public boolean equals(Object o) {
    if (!(o instanceof INodeId)) {
      return false;
    }
    return compareTo((INodeId) o) == 0;
  }

  @Override
  // Object
  public int hashCode() {
    long id = getLastInodeId();
    return (int) (id ^ (id >>> 32));
  }
}
