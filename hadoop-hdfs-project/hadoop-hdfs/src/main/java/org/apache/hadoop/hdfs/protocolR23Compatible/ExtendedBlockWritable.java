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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Identifies a Block uniquely across the block pools
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ExtendedBlockWritable implements Writable {
  private String poolId;
  private long blockId;
  private long numBytes;
  private long generationStamp;

  static { // register a ctor
    WritableFactories.setFactory(ExtendedBlockWritable.class, new WritableFactory() {
      public Writable newInstance() {
        return new ExtendedBlockWritable();
      }
    });
  }

  static public org.apache.hadoop.hdfs.protocol.ExtendedBlock convertExtendedBlock(ExtendedBlockWritable eb) {
    if (eb == null) return null;
    return new org.apache.hadoop.hdfs.protocol.ExtendedBlock( eb.getBlockPoolId(),  eb.getBlockId(),   eb.getNumBytes(),
       eb.getGenerationStamp());
  }
  
  public static ExtendedBlockWritable convertExtendedBlock(final org.apache.hadoop.hdfs.protocol.ExtendedBlock b) {
    if (b == null) return null;
    return new ExtendedBlockWritable(b.getBlockPoolId(), 
        b.getBlockId(), b.getNumBytes(), b.getGenerationStamp());
  }
  
  public ExtendedBlockWritable() {
    this(null, 0, 0, 0);
  }

  public ExtendedBlockWritable(final ExtendedBlockWritable b) {
    this(b.poolId, b.blockId, b.numBytes, b.generationStamp);
  }
  
  public ExtendedBlockWritable(final String poolId, final long blockId) {
    this(poolId, blockId, 0, 0);
  }

  public ExtendedBlockWritable(final String poolId, final long blkid, final long len,
      final long genstamp) {
    this.poolId = poolId;
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genstamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    DeprecatedUTF8.writeString(out, poolId);
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.poolId = DeprecatedUTF8.readString(in);
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }

  public String getBlockPoolId() {
    return poolId;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }
  
  @Override // Object
  public String toString() {
    return poolId + ":" + (new org.apache.hadoop.hdfs.protocol.Block(blockId, numBytes, generationStamp));
  }
}
