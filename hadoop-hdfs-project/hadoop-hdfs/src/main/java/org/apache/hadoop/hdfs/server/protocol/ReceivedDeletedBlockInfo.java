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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A data structure to store Block and delHints together, used to send
 * received/deleted ACKs.
 */
public class ReceivedDeletedBlockInfo implements Writable {
  Block block;
  String delHints;

  public final static String TODELETE_HINT = "-";

  public ReceivedDeletedBlockInfo() {
  }

  public ReceivedDeletedBlockInfo(Block blk, String delHints) {
    this.block = blk;
    this.delHints = delHints;
  }

  public Block getBlock() {
    return this.block;
  }

  public void setBlock(Block blk) {
    this.block = blk;
  }

  public String getDelHints() {
    return this.delHints;
  }

  public void setDelHints(String hints) {
    this.delHints = hints;
  }

  public boolean equals(Object o) {
    if (!(o instanceof ReceivedDeletedBlockInfo)) {
      return false;
    }
    ReceivedDeletedBlockInfo other = (ReceivedDeletedBlockInfo) o;
    return this.block.equals(other.getBlock())
        && this.delHints.equals(other.delHints);
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return 0; 
  }

  public boolean blockEquals(Block b) {
    return this.block.equals(b);
  }

  public boolean isDeletedBlock() {
    return delHints.equals(TODELETE_HINT);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.block.write(out);
    Text.writeString(out, this.delHints);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.block = new Block();
    this.block.readFields(in);
    this.delHints = Text.readString(in);
  }

  public String toString() {
    return block.toString() + ", delHint: " + delHints;
  }
}
