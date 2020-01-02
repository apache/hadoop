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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.nio.MappedByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * Represents an HDFS block that is mapped to memory by the DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryMappedBlock implements MappableBlock {
  private MappedByteBuffer mmap;
  private final long length;

  MemoryMappedBlock(MappedByteBuffer mmap, long length) {
    this.mmap = mmap;
    this.length = length;
    assert length > 0;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public long getAddress() {
    return -1L;
  }

  @Override
  public ExtendedBlockId getKey() {
    return null;
  }

  @Override
  public void close() {
    if (mmap != null) {
      NativeIO.POSIX.munmap(mmap);
      mmap = null;
    }
  }
}
