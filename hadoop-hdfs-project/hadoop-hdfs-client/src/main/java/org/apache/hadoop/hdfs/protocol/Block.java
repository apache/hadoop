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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * A Block is a Hadoop FS primitive, identified by its block ID (a long). A
 * block also has an accompanying generation stamp. A generation stamp is a
 * monotonically increasing 8-byte number for each block that is maintained
 * persistently by the NameNode. However, for the purposes of this class, two
 * Blocks are considered equal iff they have the same block ID.
 *
 * @see Block#equals(Object)
 * @see Block#hashCode()
 * @see Block#compareTo(Block)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Block implements Writable, Comparable<Block> {
  public static final String BLOCK_FILE_PREFIX = "blk_";
  public static final String METADATA_EXTENSION = ".meta";
  static {                                      // register a ctor
    WritableFactories.setFactory(Block.class, new WritableFactory() {
      @Override
      public Writable newInstance() { return new Block(); }
    });
  }

  public static final Pattern blockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)$");
  public static final Pattern metaFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)_(\\d++)\\" + METADATA_EXTENSION
          + "$");
  public static final Pattern metaOrBlockFilePattern = Pattern
      .compile(BLOCK_FILE_PREFIX + "(-??\\d++)(_(\\d++)\\" + METADATA_EXTENSION
          + ")?$");

  public static boolean isBlockFilename(File f) {
    String name = f.getName();
    return blockFilePattern.matcher(name).matches();
  }

  public static long filename2id(String name) {
    Matcher m = blockFilePattern.matcher(name);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  public static boolean isMetaFilename(String name) {
    return metaFilePattern.matcher(name).matches();
  }

  public static File metaToBlockFile(File metaFile) {
    return new File(metaFile.getParent(), metaFile.getName().substring(
        0, metaFile.getName().lastIndexOf('_')));
  }

  /**
   * Get generation stamp from the name of the metafile name
   */
  public static long getGenerationStamp(String metaFile) {
    Matcher m = metaFilePattern.matcher(metaFile);
    return m.matches() ? Long.parseLong(m.group(2))
        : HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }

  /**
   * Get the blockId from the name of the meta or block file
   */
  public static long getBlockId(String metaOrBlockFile) {
    Matcher m = metaOrBlockFilePattern.matcher(metaOrBlockFile);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  private long blockId;
  private long numBytes;
  private long generationStamp;

  public Block() {this(0, 0, 0);}

  public Block(final long blkid, final long len, final long generationStamp) {
    set(blkid, len, generationStamp);
  }

  public Block(final long blkid) {
    this(blkid, 0, HdfsConstants.GRANDFATHER_GENERATION_STAMP);
  }

  public Block(Block blk) {
    this(blk.blockId, blk.numBytes, blk.generationStamp);
  }

  /**
   * Find the blockid from the given filename
   */
  public Block(File f, long len, long genstamp) {
    this(filename2id(f.getName()), len, genstamp);
  }

  public void set(long blkid, long len, long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long bid) {
    blockId = bid;
  }

  /**
   * Get the block name. The format of the name is in the format:
   * <p>
   * blk_1, blk_2, blk_3, etc.
   * </p>
   *
   * @return the block name
   */
  public String getBlockName() {
    return BLOCK_FILE_PREFIX + blockId;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public void setNumBytes(long len) {
    this.numBytes = len;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStamp(long stamp) {
    generationStamp = stamp;
  }

  /**
   * A helper method to output the string representation of the Block portion of
   * a derived class' instance.
   *
   * @param b the target object
   * @return the string representation of the block
   */
  public static String toString(final Block b) {
    return new StringBuilder(BLOCK_FILE_PREFIX)
        .append(b.blockId)
        .append('_')
        .append(b.generationStamp)
        .toString();
  }

  /**
   * Get the block name. The format of the name is in the format:
   * <p>
   * blk_block-id_generation, blk_1_1, blk_1_2, blk_2_1, etc.
   * </p>
   *
   * @return the full block name
   */
  @Override
  public String toString() {
    return Block.toString(this);
  }

  public void appendStringTo(StringBuilder sb) {
    sb.append(BLOCK_FILE_PREFIX)
      .append(blockId)
      .append('_')
      .append(getGenerationStamp());
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  @Override // Writable
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }

  final void writeHelper(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }

  final void readHelper(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }

  // write only the identifier part of the block
  public void writeId(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(generationStamp);
  }

  // Read only the identifier part of the block
  public void readId(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.generationStamp = in.readLong();
  }

  /**
   * Compares this Block with the specified Block for order. Returns a negative
   * integer, zero, or a positive integer as this Block is less than, equal to,
   * or greater than the specified Block. Blocks are ordered based on their
   * block ID.
   *
   * @param b the Block to be compared
   * @return a negative integer, zero, or a positive integer as this Block is
   *         less than, equal to, or greater than the specified Block.
   */
  @Override
  public int compareTo(@Nonnull Block b) {
    return Long.compare(blockId, b.blockId);
  }

  /**
   * Indicates whether some Block is "equal to" this one. Two blocks are
   * considered equal if they have the same block ID.
   *
   * @param obj the reference object with which to compare.
   * @return true if this Block is the same as the argument; false otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Block)) {
      return false;
    }
    Block other = (Block) obj;
    return (blockId == other.blockId);
  }

  /**
   * Returns a hash code value for the Block. The hash code adheres to the
   * general contract of hashCode. If two Blocks are equal according to the
   * equals(Object) method, then calling the hashCode method on each of the two
   * blocks produce the same integer result.
   *
   * @return a hash code value for this block
   * @see Block#equals(Object)
   */
  @Override
  public int hashCode() {
    return Long.hashCode(blockId);
  }

  /**
   * A helper function to determine if two blocks are equal, based on the block
   * ID and the generation stamp. This is a different equalities check than the
   * default behavior of the Block class. Two blocks are considered equal by
   * this function iff the two blocks have the same block ID and the same
   * generation stamp, or if both blocks are null.
   *
   * @param a an object
   * @param b an object to be compared with {@code a} for equality
   * @return {@code true} if the blocks are deeply equal to each other and
   *         {@code false} otherwise
   * @see Block
   */
  public static boolean matchingIdAndGenStamp(Block a, Block b) {
    if (a == b) {
      return true;
    } else if (a == null || b == null) {
      return false;
    } else {
      return a.blockId == b.blockId && a.generationStamp == b.generationStamp;
    }
  }
}
