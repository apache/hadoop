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
package org.apache.hadoop.dfs;

import java.io.*;
import org.apache.hadoop.io.*;

class DatanodeCommand implements Writable {
  DatanodeProtocol.DataNodeAction action;
  
  public DatanodeCommand() {
    this(DatanodeProtocol.DataNodeAction.DNA_UNKNOWN);
  }
  
  public DatanodeCommand(DatanodeProtocol.DataNodeAction action) {
    this.action = action;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (BlockCommand.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeCommand(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, action);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.action = (DatanodeProtocol.DataNodeAction)
      WritableUtils.readEnum(in, DatanodeProtocol.DataNodeAction.class);
  }
}

/****************************************************
 * A BlockCommand is an instruction to a datanode 
 * regarding some blocks under its control.  It tells
 * the DataNode to either invalidate a set of indicated
 * blocks, or to copy a set of indicated blocks to 
 * another DataNode.
 * 
 ****************************************************/
class BlockCommand extends DatanodeCommand {
  Block blocks[];
  DatanodeInfo targets[][];

  public BlockCommand() {}

  /**
   * Create BlockCommand for transferring blocks to another datanode
   * @param blocks    blocks to be transferred 
   * @param targets   nodes to transfer
   */
  public BlockCommand(Block blocks[], DatanodeInfo targets[][]) {
    super( DatanodeProtocol.DataNodeAction.DNA_TRANSFER);
    this.blocks = blocks;
    this.targets = targets;
  }

  /**
   * Create BlockCommand for block invalidation
   * @param blocks  blocks to invalidate
   */
  public BlockCommand(Block blocks[]) {
    super(DatanodeProtocol.DataNodeAction.DNA_INVALIDATE);
    this.blocks = blocks;
    this.targets = new DatanodeInfo[0][];
  }

  public Block[] getBlocks() {
    return blocks;
  }

  public DatanodeInfo[][] getTargets() {
    return targets;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (BlockCommand.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockCommand(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
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

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.blocks = new Block[in.readInt()];
    for (int i = 0; i < blocks.length; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }

    this.targets = new DatanodeInfo[in.readInt()][];
    for (int i = 0; i < targets.length; i++) {
      this.targets[i] = new DatanodeInfo[in.readInt()];
      for (int j = 0; j < targets[i].length; j++) {
        targets[i][j] = new DatanodeInfo();
        targets[i][j].readFields(in);
      }
    }
  }
}
