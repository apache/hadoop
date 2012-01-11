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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**************************************************
 * A Block is a Hadoop FS primitive, identified by a long.
 **************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockWritable implements Writable {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (BlockWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockWritable(); }
       });
  }


  private long blockId;
  private long numBytes;
  private long generationStamp;

  public BlockWritable() {this(0, 0, 0);}

  public BlockWritable(final long blkid, final long len, final long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  @Override // Writable
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
  }

  public static BlockWritable convert(Block b) {
    return new BlockWritable(b.getBlockId(), b.getNumBytes(),
        b.getGenerationStamp());
  }

  public Block convert() {
    return new Block(blockId, numBytes, generationStamp);
  }
  
  public long getBlockId() {
    return blockId;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }
  
  public static Block[] convert(BlockWritable[] blocks) {
    Block[] ret = new Block[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      ret[i] = blocks[i].convert();
    }
    return ret;
  }
  
  public static BlockWritable[] convert(Block[] blocks) {
    BlockWritable[] ret = new BlockWritable[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      ret[i] = BlockWritable.convert(blocks[i]);
    }
    return ret;
  }
}
