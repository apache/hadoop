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
class Block implements Writable, Comparable<Block> {

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

  static long filename2id(String name) {
    return Long.parseLong(name.substring("blk_".length()));
  }

  long blkid;
  long len;
  long generationStamp;

  Block() {this(0, 0, 0);}

  Block(final long blkid, final long len, final long generationStamp) {
    set(blkid, len, generationStamp);
  }

  Block(final long blkid) {this(blkid, 0, GenerationStamp.WILDCARD_STAMP);}

  Block(Block blk) {this(blk.blkid, blk.len, blk.generationStamp);}

  /**
   * Find the blockid from the given filename
   */
  public Block(File f, long len, long genstamp) {
    this(filename2id(f.getName()), len, genstamp);
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
  static void validateGenerationStamp(long generationstamp) {
    if (generationstamp == GenerationStamp.WILDCARD_STAMP) {
      throw new IllegalStateException("generationStamp (=" + generationstamp
          + ") == GenerationStamp.WILDCARD_STAMP");
    }    
  }

  /** {@inheritDoc} */
  public int compareTo(Block b) {
    //Wildcard generationStamp is NOT ALLOWED here
    validateGenerationStamp(this.generationStamp);
    validateGenerationStamp(b.generationStamp);

    if (blkid < b.blkid) {
      return -1;
    } else if (blkid == b.blkid) {
      return GenerationStamp.compare(generationStamp, b.generationStamp);
    } else {
      return 1;
    }
  }

  /** {@inheritDoc} */
  public boolean equals(Object o) {
    if (!(o instanceof Block)) {
      return false;
    }
    final Block that = (Block)o;
    //Wildcard generationStamp is ALLOWED here
    return this.blkid == that.blkid
      && GenerationStamp.equalsWithWildcard(
          this.generationStamp, that.generationStamp);
  }

  /** {@inheritDoc} */
  public int hashCode() {
    //GenerationStamp is IRRELEVANT and should not be used here
    return 37 * 17 + (int) (blkid^(blkid>>>32));
  }
}
