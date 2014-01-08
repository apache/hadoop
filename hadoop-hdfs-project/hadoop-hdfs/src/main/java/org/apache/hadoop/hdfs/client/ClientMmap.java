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

import java.io.FileInputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

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
   * A reference to the manager of this mmap.
   * 
   * This is only a weak reference to help minimize the damange done by
   * code which leaks references accidentally.
   */
  private final WeakReference<ClientMmapManager> manager;
  
  /**
   * The actual mapped memory region.
   */
  private final MappedByteBuffer map;
  
  /**
   * A reference count tracking how many threads are using this object.
   */
  private final AtomicInteger refCount = new AtomicInteger(1);

  /**
   * Block pertaining to this mmap
   */
  private final ExtendedBlock block;
  
  /**
   * The DataNode where this mmap came from.
   */
  private final DatanodeID datanodeID;

  /**
   * The monotonic time when this mmap was last evictable.
   */
  private long lastEvictableTimeNs;

  public static ClientMmap load(ClientMmapManager manager, FileInputStream in, 
      ExtendedBlock block, DatanodeID datanodeID) 
          throws IOException {
    MappedByteBuffer map =
        in.getChannel().map(MapMode.READ_ONLY, 0,
            in.getChannel().size());
    return new ClientMmap(manager, map, block, datanodeID);
  }

  private ClientMmap(ClientMmapManager manager, MappedByteBuffer map, 
        ExtendedBlock block, DatanodeID datanodeID) 
            throws IOException {
    this.manager = new WeakReference<ClientMmapManager>(manager);
    this.map = map;
    this.block = block;
    this.datanodeID = datanodeID;
    this.lastEvictableTimeNs = 0;
  }

  /**
   * Decrement the reference count on this object.
   * Should be called with the ClientMmapManager lock held.
   */
  public void unref() {
    int count = refCount.decrementAndGet();
    if (count < 0) {
      throw new IllegalArgumentException("can't decrement the " +
          "reference count on this ClientMmap lower than 0.");
    } else if (count == 0) {
      ClientMmapManager man = manager.get();
      if (man == null) {
        unmap();
      } else {
        man.makeEvictable(this);
      }
    }
  }

  /**
   * Increment the reference count on this object.
   *
   * @return     The new reference count.
   */
  public int ref() {
    return refCount.getAndIncrement();
  }

  @VisibleForTesting
  public ExtendedBlock getBlock() {
    return block;
  }

  DatanodeID getDatanodeID() {
    return datanodeID;
  }

  public MappedByteBuffer getMappedByteBuffer() {
    return map;
  }

  public void setLastEvictableTimeNs(long lastEvictableTimeNs) {
    this.lastEvictableTimeNs = lastEvictableTimeNs;
  }

  public long getLastEvictableTimeNs() {
    return this.lastEvictableTimeNs;
  }

  /**
   * Unmap the memory region.
   */
  void unmap() {
    assert(refCount.get() == 0);
    NativeIO.POSIX.munmap(map);
  }
}
