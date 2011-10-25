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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocolR23Compatible.DatanodeInfoWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExtendedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.LocatedBlockWritable;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * This is a block with locations from which it should be recovered and the new
 * generation stamp, which the block will have after successful recovery.
 * 
 * The new generation stamp of the block, also plays role of the recovery id.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RecoveringBlockWritable implements Writable {
  private long newGenerationStamp;
  private LocatedBlockWritable locatedBlock;

  /**
   * Create empty RecoveringBlock.
   */
  public RecoveringBlockWritable() {
    locatedBlock = new LocatedBlockWritable();
    newGenerationStamp = -1L;
  }

  /**
   * Create RecoveringBlock.
   */
  public RecoveringBlockWritable(ExtendedBlockWritable b,
      DatanodeInfoWritable[] locs, long newGS) {
    locatedBlock = new LocatedBlockWritable(b, locs, -1, false);
    this.newGenerationStamp = newGS;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(RecoveringBlockWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new RecoveringBlockWritable();
          }
        });
  }

  public void write(DataOutput out) throws IOException {
    locatedBlock.write(out);
    out.writeLong(newGenerationStamp);
  }

  public void readFields(DataInput in) throws IOException {
    locatedBlock = new LocatedBlockWritable();
    locatedBlock.readFields(in);
    newGenerationStamp = in.readLong();
  }

  public RecoveringBlock convert() {
    ExtendedBlockWritable eb = locatedBlock.getBlock();
    DatanodeInfoWritable[] dnInfo = locatedBlock.getLocations();
    return new RecoveringBlock(ExtendedBlockWritable.convertExtendedBlock(eb),
        DatanodeInfoWritable.convertDatanodeInfo(dnInfo), newGenerationStamp);
  }

  public static RecoveringBlockWritable convert(RecoveringBlock rBlock) {
    if (rBlock == null) {
      return null;
    }
    ExtendedBlockWritable eb = ExtendedBlockWritable
        .convertExtendedBlock(rBlock.getBlock());
    DatanodeInfoWritable[] dnInfo = DatanodeInfoWritable
        .convertDatanodeInfo(rBlock.getLocations());
    return new RecoveringBlockWritable(eb, dnInfo,
        rBlock.getNewGenerationStamp());
  }
}