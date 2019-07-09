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
package org.apache.hadoop.tools.dynamometer.blockgenerator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * This is the MapOutputValue class. It has the blockId and the block generation
 * stamp which is needed to generate the block images in the reducer.
 *
 * This also stores the replication of the block, but note that it does not
 * serialize this value as part of its {@link Writable} interface, and does not
 * consider the replication when doing equality / hash comparisons.
 */

public class BlockInfo implements Writable {

  LongWritable getBlockId() {
    return blockId;
  }

  LongWritable getBlockGenerationStamp() {
    return blockGenerationStamp;
  }

  LongWritable getSize() {
    return size;
  }

  short getReplication() {
    return replication;
  }

  private LongWritable blockId;
  private LongWritable blockGenerationStamp;
  private LongWritable size;
  private transient short replication;

  @SuppressWarnings("unused") // Used via reflection
  private BlockInfo() {
    this.blockId = new LongWritable();
    this.blockGenerationStamp = new LongWritable();
    this.size = new LongWritable();
  }

  BlockInfo(long blockid, long blockgenerationstamp, long size,
      short replication) {
    this.blockId = new LongWritable(blockid);
    this.blockGenerationStamp = new LongWritable(blockgenerationstamp);
    this.size = new LongWritable(size);
    this.replication = replication;
  }

  public void write(DataOutput dataOutput) throws IOException {
    blockId.write(dataOutput);
    blockGenerationStamp.write(dataOutput);
    size.write(dataOutput);
  }

  public void readFields(DataInput dataInput) throws IOException {
    blockId.readFields(dataInput);
    blockGenerationStamp.readFields(dataInput);
    size.readFields(dataInput);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BlockInfo)) {
      return false;
    }
    BlockInfo blkInfo = (BlockInfo) o;
    return blkInfo.getBlockId().equals(this.getBlockId())
        && blkInfo.getBlockGenerationStamp()
            .equals(this.getBlockGenerationStamp())
        && blkInfo.getSize().equals(this.getSize());
  }

  @Override
  public int hashCode() {
    return blockId.hashCode() + 357 * blockGenerationStamp.hashCode()
        + 9357 * size.hashCode();
  }
}
