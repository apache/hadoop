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
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import org.apache.avro.reflect.Nullable;

/**
 * Collection of blocks with their locations and the file length.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class LocatedBlocksWritable implements Writable {
  private long fileLength;
  private List<LocatedBlockWritable> blocks; // array of blocks with prioritized locations
  private boolean underConstruction;
  @Nullable
  private LocatedBlockWritable lastLocatedBlock = null;
  private boolean isLastBlockComplete = false;

  public static org.apache.hadoop.hdfs.protocol.LocatedBlocks convertLocatedBlocks(
      LocatedBlocksWritable lb) {
    if (lb == null) {
      return null;
    }
    return new org.apache.hadoop.hdfs.protocol.LocatedBlocks(
        lb.getFileLength(), lb.isUnderConstruction(),
        LocatedBlockWritable.convertLocatedBlock(lb.getLocatedBlocks()),
        LocatedBlockWritable.convertLocatedBlock(lb.getLastLocatedBlock()),
        lb.isLastBlockComplete());
  }
  
  public static LocatedBlocksWritable convertLocatedBlocks(
      org.apache.hadoop.hdfs.protocol.LocatedBlocks lb) {
    if (lb == null) {
      return null;
    }
    return new LocatedBlocksWritable(lb.getFileLength(), lb.isUnderConstruction(),
        LocatedBlockWritable.convertLocatedBlock2(lb.getLocatedBlocks()),
        LocatedBlockWritable.convertLocatedBlock(lb.getLastLocatedBlock()),
        lb.isLastBlockComplete());
  }
  
  public LocatedBlocksWritable() {
    this(0, false, null, null, false);
  }
  
  /** public Constructor */
  public LocatedBlocksWritable(long flength, boolean isUnderConstuction,
      List<LocatedBlockWritable> blks, 
      LocatedBlockWritable lastBlock, boolean isLastBlockCompleted) {
    fileLength = flength;
    blocks = blks;
    underConstruction = isUnderConstuction;
    this.lastLocatedBlock = lastBlock;
    this.isLastBlockComplete = isLastBlockCompleted;
  }
  
  /**
   * Get located blocks.
   */
  public List<LocatedBlockWritable> getLocatedBlocks() {
    return blocks;
  }
  
  /** Get the last located block. */
  public LocatedBlockWritable getLastLocatedBlock() {
    return lastLocatedBlock;
  }
  
  /** Is the last block completed? */
  public boolean isLastBlockComplete() {
    return isLastBlockComplete;
  }

  /**
   * Get located block.
   */
  public LocatedBlockWritable get(int index) {
    return blocks.get(index);
  }
  
  /**
   * Get number of located blocks.
   */
  public int locatedBlockCount() {
    return blocks == null ? 0 : blocks.size();
  }

  /**
   * Get file length
   */
  public long getFileLength() {
    return this.fileLength;
  }

  /**
   * Return ture if file was under construction when 
   * this LocatedBlocks was constructed, false otherwise.
   */
  public boolean isUnderConstruction() {
    return underConstruction;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlocksWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlocksWritable(); }
       });
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.fileLength);
    out.writeBoolean(underConstruction);

    //write the last located block
    final boolean isNull = lastLocatedBlock == null;
    out.writeBoolean(isNull);
    if (!isNull) {
      lastLocatedBlock.write(out);
    }
    out.writeBoolean(isLastBlockComplete);

    // write located blocks
    int nrBlocks = locatedBlockCount();
    out.writeInt(nrBlocks);
    if (nrBlocks == 0) {
      return;
    }
    for (LocatedBlockWritable blk : this.blocks) {
      blk.write(out);
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.fileLength = in.readLong();
    underConstruction = in.readBoolean();

    //read the last located block
    final boolean isNull = in.readBoolean();
    if (!isNull) {
      lastLocatedBlock = LocatedBlockWritable.read(in);
    }
    isLastBlockComplete = in.readBoolean();

    // read located blocks
    int nrBlocks = in.readInt();
    this.blocks = new ArrayList<LocatedBlockWritable>(nrBlocks);
    for (int idx = 0; idx < nrBlocks; idx++) {
      LocatedBlockWritable blk = new LocatedBlockWritable();
      blk.readFields(in);
      this.blocks.add(blk);
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("{")
     .append("\n  fileLength=").append(fileLength)
     .append("\n  underConstruction=").append(underConstruction)
     .append("\n  blocks=").append(blocks)
     .append("\n  lastLocatedBlock=").append(lastLocatedBlock)
     .append("\n  isLastBlockComplete=").append(isLastBlockComplete)
     .append("}");
    return b.toString();
  }
}
