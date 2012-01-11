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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * BlockRecoveryCommand is an instruction to a data-node to recover the
 * specified blocks.
 * 
 * The data-node that receives this command treats itself as a primary data-node
 * in the recover process.
 * 
 * Block recovery is identified by a recoveryId, which is also the new
 * generation stamp, which the block will have after the recovery succeeds.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockRecoveryCommandWritable extends DatanodeCommandWritable {
  Collection<RecoveringBlockWritable> recoveringBlocks;

  /**
   * Create empty BlockRecoveryCommand.
   */
  public BlockRecoveryCommandWritable() { }

  /**
   * Create BlockRecoveryCommand with the specified capacity for recovering
   * blocks.
   */
  public BlockRecoveryCommandWritable(int capacity) {
    this(new ArrayList<RecoveringBlockWritable>(capacity));
  }
  
  public BlockRecoveryCommandWritable(Collection<RecoveringBlockWritable> blocks) {
    super(DatanodeWireProtocol.DNA_RECOVERBLOCK);
    recoveringBlocks = blocks;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(BlockRecoveryCommandWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new BlockRecoveryCommandWritable();
          }
        });
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(recoveringBlocks.size());
    for (RecoveringBlockWritable block : recoveringBlocks) {
      block.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int numBlocks = in.readInt();
    recoveringBlocks = new ArrayList<RecoveringBlockWritable>(numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      RecoveringBlockWritable b = new RecoveringBlockWritable();
      b.readFields(in);
      recoveringBlocks.add(b);
    }
  }

  @Override
  public DatanodeCommand convert() {
    Collection<RecoveringBlock> blks = 
        new ArrayList<RecoveringBlock>(recoveringBlocks.size());
    for (RecoveringBlockWritable b : recoveringBlocks) {
      blks.add(b.convert());
    }
    return new BlockRecoveryCommand(blks);
  }

  public static BlockRecoveryCommandWritable convert(BlockRecoveryCommand cmd) {
    if (cmd == null) return null;
    Collection<RecoveringBlockWritable> blks = 
        new ArrayList<RecoveringBlockWritable>(cmd.getRecoveringBlocks().size());
    for (RecoveringBlock b : cmd.getRecoveringBlocks()) {
      blks.add(RecoveringBlockWritable.convert(b));
    }
    return new BlockRecoveryCommandWritable(blks);
  }
}
