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
import java.util.List;

import org.apache.hadoop.dfs.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.io.*;

abstract class DatanodeCommand implements Writable {
  static class Register extends DatanodeCommand {
    private Register() {super(DatanodeProtocol.DNA_REGISTER);}
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }

  static class BlockReport extends DatanodeCommand {
    private BlockReport() {super(DatanodeProtocol.DNA_BLOCKREPORT);}
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }

  static class Finalize extends DatanodeCommand {
    private Finalize() {super(DatanodeProtocol.DNA_FINALIZE);}
    public void readFields(DataInput in) {}
    public void write(DataOutput out) {}
  }

  static {                                      // register a ctor
    WritableFactories.setFactory(Register.class,
        new WritableFactory() {
          public Writable newInstance() {return new Register();}
        });
    WritableFactories.setFactory(BlockReport.class,
        new WritableFactory() {
          public Writable newInstance() {return new BlockReport();}
        });
    WritableFactories.setFactory(Finalize.class,
        new WritableFactory() {
          public Writable newInstance() {return new Finalize();}
        });
  }

  static final DatanodeCommand REGISTER = new Register();
  static final DatanodeCommand BLOCKREPORT = new BlockReport();
  static final DatanodeCommand FINALIZE = new Finalize();

  private int action;
  
  public DatanodeCommand() {
    this(DatanodeProtocol.DNA_UNKNOWN);
  }
  
  DatanodeCommand(int action) {
    this.action = action;
  }

  int getAction() {
    return this.action;
  }
  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.action);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.action = in.readInt();
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
   */
  BlockCommand(int action, List<BlockTargetPair> blocktargetlist) {
    super(action);

    blocks = new Block[blocktargetlist.size()]; 
    targets = new DatanodeInfo[blocks.length][];
    for(int i = 0; i < blocks.length; i++) {
      BlockTargetPair p = blocktargetlist.get(i);
      blocks[i] = p.block;
      targets[i] = p.targets;
    }
  }

  private static final DatanodeInfo[][] EMPTY_TARGET = {};

  /**
   * Create BlockCommand for the given action
   * @param blocks blocks related to the action
   */
  BlockCommand(int action, Block blocks[]) {
    super(action);
    this.blocks = blocks;
    this.targets = EMPTY_TARGET;
  }

  Block[] getBlocks() {
    return blocks;
  }

  DatanodeInfo[][] getTargets() {
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
