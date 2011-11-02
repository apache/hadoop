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

import org.apache.hadoop.hdfs.protocolR23Compatible.BlockWritable;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A data structure to store Block and delHints together, used to send
 * received/deleted ACKs.
 */
public class ReceivedDeletedBlockInfoWritable implements Writable {
  BlockWritable block;
  String delHints;

  public final static String TODELETE_HINT = "-";

  public ReceivedDeletedBlockInfoWritable() {
  }

  public ReceivedDeletedBlockInfoWritable(BlockWritable blk, String delHints) {
    this.block = blk;
    this.delHints = delHints;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.block.write(out);
    Text.writeString(out, this.delHints);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.block = new BlockWritable();
    this.block.readFields(in);
    this.delHints = Text.readString(in);
  }

  public String toString() {
    return block.toString() + ", delHint: " + delHints;
  }

  public static ReceivedDeletedBlockInfo[] convert(
      ReceivedDeletedBlockInfoWritable[] rdBlocks) {
    ReceivedDeletedBlockInfo[] ret = 
        new ReceivedDeletedBlockInfo[rdBlocks.length];
    for (int i = 0; i < rdBlocks.length; i++) {
      ret[i] = rdBlocks[i].convert();
    }
    return ret;
  }
  
  public static ReceivedDeletedBlockInfoWritable[] convert(
      ReceivedDeletedBlockInfo[] blocks) {
    ReceivedDeletedBlockInfoWritable[] ret = 
        new ReceivedDeletedBlockInfoWritable[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      ret[i] = convert(blocks[i]);
    }
    return ret;
  }

  public ReceivedDeletedBlockInfo convert() {
    return new ReceivedDeletedBlockInfo(block.convert(), delHints);
  }

  public static ReceivedDeletedBlockInfoWritable convert(
      ReceivedDeletedBlockInfo b) {
    if (b == null) return null;
    return new ReceivedDeletedBlockInfoWritable(BlockWritable.convert(b
        .getBlock()), b.getDelHints());
  }
}
