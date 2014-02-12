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
package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.classification.InterfaceAudience;

import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A memory-mapped region used by an HDFS client.
 * 
 * This class includes a reference count and some other information used by
 * ClientMmapManager to track and cache mmaps.
 */
@InterfaceAudience.Private
public class ClientMmap {
  static final Log LOG = LogFactory.getLog(ClientMmap.class);
  
  /**
   * A reference to the block replica which this mmap relates to.
   */
  private final ShortCircuitReplica replica;
  
  /**
   * The java ByteBuffer object.
   */
  private final MappedByteBuffer map;

  /**
   * Reference count of this ClientMmap object.
   */
  private final AtomicInteger refCount = new AtomicInteger(1);

  ClientMmap(ShortCircuitReplica replica, MappedByteBuffer map) {
    this.replica = replica;
    this.map = map;
  }

  /**
   * Increment the reference count.
   *
   * @return   The new reference count.
   */
  void ref() {
    refCount.addAndGet(1);
  }

  /**
   * Decrement the reference count.
   *
   * The parent replica gets unreferenced each time the reference count 
   * of this object goes to 0.
   */
  public void unref() {
    refCount.addAndGet(-1);
    replica.unref();
  }

  public MappedByteBuffer getMappedByteBuffer() {
    return map;
  }
}