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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocolR23Compatible.BlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.DatanodeInfoWritable;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/****************************************************
 * A BlockCommand is an instruction to a datanode regarding some blocks under
 * its control. It tells the DataNode to either invalidate a set of indicated
 * blocks, or to copy a set of indicated blocks to another DataNode.
 * 
 ****************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockCommandWritable extends DatanodeCommandWritable {

  /**
   * This constant is used to indicate that the block deletion does not need
   * explicit ACK from the datanode. When a block is put into the list of blocks
   * to be deleted, it's size is set to this constant. We assume that no block
   * would actually have this size. Otherwise, we would miss ACKs for blocks
   * with such size. Positive number is used for compatibility reasons.
   */
  public static final long NO_ACK = Long.MAX_VALUE;

  String poolId;
  BlockWritable blocks[];
  DatanodeInfoWritable targets[][];

  public BlockCommandWritable() {
  }

  /**
   * Create BlockCommand for the given action
   * 
   * @param blocks blocks related to the action
   */
  public BlockCommandWritable(int action, String poolId, BlockWritable[] blocks,
      DatanodeInfoWritable[][] targets) {
    super(action);
    this.poolId = poolId;
    this.blocks = blocks;
    this.targets = targets;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(BlockCommandWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new BlockCommandWritable();
          }
        });
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, poolId);
    out.writeInt(blocks.length);
    for (int i = 0; i < blocks.length; i++) {
      blocks[i].write(out);
    }
    out.writeInt(targets.length);
    for (int i = 0; i < targets.length; i++) {
      out.writeInt(targets[i].length);
      for (int j = 0; j < targets[i].length; j++) {
        targets[i][j].write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.poolId = Text.readString(in);
    this.blocks = new BlockWritable[in.readInt()];
    for (int i = 0; i < blocks.length; i++) {
      blocks[i] = new BlockWritable();
      blocks[i].readFields(in);
    }

    this.targets = new DatanodeInfoWritable[in.readInt()][];
    for (int i = 0; i < targets.length; i++) {
      this.targets[i] = new DatanodeInfoWritable[in.readInt()];
      for (int j = 0; j < targets[i].length; j++) {
        targets[i][j] = new DatanodeInfoWritable();
        targets[i][j].readFields(in);
      }
    }
  }

  @Override
  public BlockCommand convert() {
    DatanodeInfo[][] dinfo = new DatanodeInfo[targets.length][];
    for (int i = 0; i < targets.length; i++) {
      dinfo[i] = DatanodeInfoWritable.convertDatanodeInfo(targets[i]);
    }
    return new BlockCommand(getAction(), poolId, BlockWritable.convert(blocks),
        dinfo);
  }

  public static BlockCommandWritable convert(BlockCommand cmd) {
    if (cmd == null) return null;
    DatanodeInfo[][] targets = cmd.getTargets();
    DatanodeInfoWritable[][] dinfo = new DatanodeInfoWritable[targets.length][];
    for (int i = 0; i < targets.length; i++) {
      dinfo[i] = DatanodeInfoWritable.convertDatanodeInfo(targets[i]);
    }
    return new BlockCommandWritable(cmd.getAction(), cmd.getBlockPoolId(),
        BlockWritable.convert(cmd.getBlocks()), dinfo);
  }
}
