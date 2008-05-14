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

/**************************************************
 * A Block is a Hadoop FS primitive, identified by a 
 * long.
 *
 **************************************************/
class Block implements Writable, Comparable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (Block.class,
       new WritableFactory() {
         public Writable newInstance() { return new Block(); }
       });
  }

  // generation stamp of blocks that pre-date the introduction of
  // a generation stamp.
  static final long GRANDFATHER_GENERATION_STAMP = 0;

  /**
   */
  static boolean isBlockFilename(File f) {
    String name = f.getName();
    if ( name.startsWith( "blk_" ) && 
        name.indexOf( '.' ) < 0 ) {
      return true;
    } else {
      return false;
    }
  }

  long blkid;
  long len;
  long generationStamp;

  /**
   */
  public Block() {
    this.blkid = 0;
    this.len = 0;
    this.generationStamp = 0;
  }

  /**
   */
  public Block(final long blkid, final long len, final long generationStamp) {
    this.blkid = blkid;
    this.len = len;
    this.generationStamp = generationStamp;
  }

  /**
   */
  public Block(Block blk) {
    this.blkid = blk.blkid;
    this.len = blk.len;
    this.generationStamp = blk.generationStamp;
  }

  /**
   * Find the blockid from the given filename
   */
  public Block(File f, long len, long genstamp) {
    String name = f.getName();
    name = name.substring("blk_".length());
    this.blkid = Long.parseLong(name);
    this.len = len;
    this.generationStamp = genstamp;
  }

  public void set(long blkid, long len, long genStamp) {
    this.blkid = blkid;
    this.len = len;
    this.generationStamp = genStamp;
  }
  /**
   */
  public long getBlockId() {
    return blkid;
  }

  /**
   */
  public String getBlockName() {
    return "blk_" + String.valueOf(blkid);
  }

  /**
   */
  public long getNumBytes() {
    return len;
  }
  public void setNumBytes(long len) {
    this.len = len;
  }

  long getGenerationStamp() {
    return generationStamp;
  }

  /**
   */
  public String toString() {
    return getBlockName() + "_" + getGenerationStamp();
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(blkid);
    out.writeLong(len);
    out.writeLong(generationStamp);
  }

  public void readFields(DataInput in) throws IOException {
    this.blkid = in.readLong();
    this.len = in.readLong();
    this.generationStamp = in.readLong();
    if (len < 0) {
      throw new IOException("Unexpected block size: " + len);
    }
  }

  /////////////////////////////////////
  // Comparable
  /////////////////////////////////////
  public int compareTo(Object o) {
    Block b = (Block) o;
    if (blkid < b.blkid) {
      return -1;
    } else if (blkid == b.blkid) {
      if (generationStamp < b.generationStamp) {
        return -1;
      } else if (generationStamp == b.generationStamp) {
        return 0;
      } else {
        return 1;
      }
    } else {
      return 1;
    }
  }
  public boolean equals(Object o) {
    if (!(o instanceof Block)) {
      return false;
    }
    return blkid == ((Block)o).blkid &&
           generationStamp == ((Block)o).generationStamp;
  }
    
  public int hashCode() {
    return 37 * 17 + (int) (blkid^(blkid>>>32));
  }
}
