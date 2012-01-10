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
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** A class to implement an array of BlockLocations
 *  It provide efficient customized serialization/deserialization methods
 *  in stead of using the default array (de)serialization provided by RPC
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlocksWithLocationsWritable implements Writable {

  /**
   * A class to keep track of a block and its locations
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockWithLocationsWritable  implements Writable {
    private BlockWritable block;
    private String datanodeIDs[];
    
    /** default constructor */
    public BlockWithLocationsWritable() {
      block = new BlockWritable();
      datanodeIDs = null;
    }
    
    /** constructor */
    public BlockWithLocationsWritable(BlockWritable b, String[] datanodes) {
      block = b;
      datanodeIDs = datanodes;
    }
    
    /** deserialization method */
    public void readFields(DataInput in) throws IOException {
      block.readFields(in);
      int len = WritableUtils.readVInt(in); // variable length integer
      datanodeIDs = new String[len];
      for(int i=0; i<len; i++) {
        datanodeIDs[i] = Text.readString(in);
      }
    }
    
    /** serialization method */
    public void write(DataOutput out) throws IOException {
      block.write(out);
      WritableUtils.writeVInt(out, datanodeIDs.length); // variable length int
      for(String id:datanodeIDs) {
        Text.writeString(out, id);
      }
    }
  }

  private BlockWithLocationsWritable[] blocks;

  /** default constructor */
  BlocksWithLocationsWritable() {
  }

  /** Constructor with one parameter */
  public BlocksWithLocationsWritable( BlockWithLocationsWritable[] blocks ) {
    this.blocks = blocks;
  }

  /** serialization method */
  public void write( DataOutput out ) throws IOException {
    WritableUtils.writeVInt(out, blocks.length);
    for(int i=0; i<blocks.length; i++) {
      blocks[i].write(out);
    }
  }

  /** deserialization method */
  public void readFields(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    blocks = new BlockWithLocationsWritable[len];
    for(int i=0; i<len; i++) {
      blocks[i] = new BlockWithLocationsWritable();
      blocks[i].readFields(in);
    }
  }

  public static BlocksWithLocationsWritable convert(BlocksWithLocations locs) {
    BlockWithLocations[] blocks = locs.getBlocks();
    BlockWithLocationsWritable[] blocksWritable = 
        new BlockWithLocationsWritable[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      blocksWritable[i] = new BlockWithLocationsWritable(
          BlockWritable.convert(blocks[i].getBlock()), blocks[i].getDatanodes());
    }
    return new BlocksWithLocationsWritable(blocksWritable);
  }
  
  public BlocksWithLocations convert() {
    BlockWithLocations[] locs = new BlockWithLocations[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      locs[i] = new BlockWithLocations(blocks[i].block.convert(),
          blocks[i].datanodeIDs);
    }
    return new BlocksWithLocations(locs);
  }
}
