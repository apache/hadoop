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

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Closeable;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reference to a memory-mapped region used by an HDFS client.
 */
@InterfaceAudience.Private
public class ClientMmap implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(ClientMmap.class);

  /**
   * A reference to the block replica which this mmap relates to.
   */
  private ShortCircuitReplica replica;

  /**
   * The java ByteBuffer object.
   */
  private final MappedByteBuffer map;

  /**
   * Whether or not this ClientMmap anchors the replica into memory while
   * it exists.  Closing an anchored ClientMmap unanchors the replica.
   */
  private final boolean anchored;

  ClientMmap(ShortCircuitReplica replica, MappedByteBuffer map,
      boolean anchored) {
    this.replica = replica;
    this.map = map;
    this.anchored = anchored;
  }

  /**
   * Close the ClientMmap object.
   */
  @Override
  public void close() {
    if (replica != null) {
      if (anchored) {
        replica.removeNoChecksumAnchor();
      }
      replica.unref();
    }
    replica = null;
  }

  public MappedByteBuffer getMappedByteBuffer() {
    return map;
  }
}
