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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/****************************************************
 * A LocatedBlock is a pair of Block, DatanodeInfo[]
 * objects.  It tells where to find a Block.
 * 
 ****************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlockWritable implements Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (LocatedBlockWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new LocatedBlockWritable(); }
       });
  }

  private ExtendedBlockWritable b;
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfoWritable[] locs;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and 
  // their locations are not part of this object
  private boolean corrupt;
  private TokenWritable blockToken = new TokenWritable();

  
  static public org.apache.hadoop.hdfs.protocol.LocatedBlock
    convertLocatedBlock(LocatedBlockWritable lb) {
    if (lb == null) return null;
    org.apache.hadoop.hdfs.protocol.LocatedBlock result =  
        new org.apache.hadoop.hdfs.protocol.LocatedBlock(ExtendedBlockWritable.
            convertExtendedBlock(lb.getBlock()),
        DatanodeInfoWritable.convertDatanodeInfo(
            lb.getLocations()), lb.getStartOffset(), lb.isCorrupt());
    
    // Fill in the token
    TokenWritable tok = lb.getBlockToken();
    result.setBlockToken(
        new org.apache.hadoop.security.token.Token<BlockTokenIdentifier>(
            tok.getIdentifier(), tok.getPassword(), tok.getKind(),
            tok.getService()));
    return result;
  }
  
  public static LocatedBlockWritable 
    convertLocatedBlock(org.apache.hadoop.hdfs.protocol.LocatedBlock lb) {
    if (lb == null) return null;
    LocatedBlockWritable result =  
        new LocatedBlockWritable(ExtendedBlockWritable.convertExtendedBlock(lb.getBlock()), 
        DatanodeInfoWritable.convertDatanodeInfo(lb.getLocations()),
        lb.getStartOffset(), lb.isCorrupt());
    
    // Fill in the token
    org.apache.hadoop.security.token.Token<BlockTokenIdentifier> tok = 
        lb.getBlockToken();
    result.setBlockToken(new TokenWritable(tok.getIdentifier(), tok.getPassword(), 
        tok.getKind(), tok.getService()));
    return result;
  }
  
  static public LocatedBlockWritable[] 
      convertLocatedBlock(org.apache.hadoop.hdfs.protocol.LocatedBlock[] lb) {
    if (lb == null) return null;
    final int len = lb.length;
    LocatedBlockWritable[] result = new LocatedBlockWritable[len];
    for (int i = 0; i < len; ++i) {
      result[i] = new LocatedBlockWritable(
          ExtendedBlockWritable.convertExtendedBlock(lb[i].getBlock()),
          DatanodeInfoWritable.convertDatanodeInfo(lb[i].getLocations()), 
          lb[i].getStartOffset(), lb[i].isCorrupt());
    }
    return result;
  }
  
  static public org.apache.hadoop.hdfs.protocol.LocatedBlock[] 
      convertLocatedBlock(LocatedBlockWritable[] lb) {
    if (lb == null) return null;
    final int len = lb.length;
    org.apache.hadoop.hdfs.protocol.LocatedBlock[] result = 
        new org.apache.hadoop.hdfs.protocol.LocatedBlock[len];
    for (int i = 0; i < len; ++i) {
      result[i] = new org.apache.hadoop.hdfs.protocol.LocatedBlock(
          ExtendedBlockWritable.convertExtendedBlock(lb[i].getBlock()),
          DatanodeInfoWritable.convertDatanodeInfo(lb[i].getLocations()), 
          lb[i].getStartOffset(), lb[i].isCorrupt());
    }
    return result;
  }
  
  static public List<org.apache.hadoop.hdfs.protocol.LocatedBlock> 
    convertLocatedBlock(
        List<org.apache.hadoop.hdfs.protocolR23Compatible.LocatedBlockWritable> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<org.apache.hadoop.hdfs.protocol.LocatedBlock> result = 
        new ArrayList<org.apache.hadoop.hdfs.protocol.LocatedBlock>(len);
    for (int i = 0; i < len; ++i) {
      result.add(LocatedBlockWritable.convertLocatedBlock(lb.get(i)));
    }
    return result;
  }
  
  static public List<LocatedBlockWritable> 
  convertLocatedBlock2(List<org.apache.hadoop.hdfs.protocol.LocatedBlock> lb) {
    if (lb == null) return null;
    final int len = lb.size();
    List<LocatedBlockWritable> result = new ArrayList<LocatedBlockWritable>(len);
    for (int i = 0; i < len; ++i) {
      result.add(LocatedBlockWritable.convertLocatedBlock(lb.get(i)));
    }
    return result;
  }
  
  public LocatedBlockWritable() {
    this(new ExtendedBlockWritable(), new DatanodeInfoWritable[0], 0L, false);
  }

  public LocatedBlockWritable(ExtendedBlockWritable eb) {
    this(eb, new DatanodeInfoWritable[0], 0L, false);
  }
  
  public LocatedBlockWritable(ExtendedBlockWritable b, DatanodeInfoWritable[] locs) {
    this(b, locs, -1, false); // startOffset is unknown
  }

  public LocatedBlockWritable(ExtendedBlockWritable b, DatanodeInfoWritable[] locs, long startOffset) {
    this(b, locs, startOffset, false);
  }

  public LocatedBlockWritable(ExtendedBlockWritable b, DatanodeInfoWritable[] locs, long startOffset, 
                      boolean corrupt) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if (locs==null) {
      this.locs = new DatanodeInfoWritable[0];
    } else {
      this.locs = locs;
    }
  }

  public TokenWritable getBlockToken() {
    return blockToken;
  }

  public void setBlockToken(TokenWritable token) {
    this.blockToken = token;
  }

  public ExtendedBlockWritable getBlock() {
    return b;
  }

  public DatanodeInfoWritable[] getLocations() {
    return locs;
  }
  
  public long getStartOffset() {
    return offset;
  }
  
  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }
  
  public boolean isCorrupt() {
    return this.corrupt;
  }

  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    blockToken.write(out);
    out.writeBoolean(corrupt);
    out.writeLong(offset);
    b.write(out);
    out.writeInt(locs.length);
    for (int i = 0; i < locs.length; i++) {
      locs[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blockToken.readFields(in);
    this.corrupt = in.readBoolean();
    offset = in.readLong();
    this.b = new ExtendedBlockWritable();
    b.readFields(in);
    int count = in.readInt();
    this.locs = new DatanodeInfoWritable[count];
    for (int i = 0; i < locs.length; i++) {
      locs[i] = new DatanodeInfoWritable();
      locs[i].readFields(in);
    }
  }

  /** Read LocatedBlock from in. */
  public static LocatedBlockWritable read(DataInput in) throws IOException {
    final LocatedBlockWritable lb = new LocatedBlockWritable();
    lb.readFields(in);
    return lb;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b
        + "; getBlockSize()=" + getBlockSize()
        + "; corrupt=" + corrupt
        + "; offset=" + offset
        + "; locs=" + java.util.Arrays.asList(locs)
        + "}";
  }
}
