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

import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;

/**
 * Collection of blocks with their locations and the file length.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlocks {
  private final long fileLength;
  private final List<LocatedBlock> blocks; // array of blocks with prioritized locations
  private final boolean underConstruction;
  private final LocatedBlock lastLocatedBlock;
  private final boolean isLastBlockComplete;
  private final FileEncryptionInfo fileEncryptionInfo;

  public LocatedBlocks() {
    fileLength = 0;
    blocks = null;
    underConstruction = false;
    lastLocatedBlock = null;
    isLastBlockComplete = false;
    fileEncryptionInfo = null;
  }

  public LocatedBlocks(long flength, boolean isUnderConstuction,
    List<LocatedBlock> blks, LocatedBlock lastBlock,
    boolean isLastBlockCompleted, FileEncryptionInfo feInfo) {
    fileLength = flength;
    blocks = blks;
    underConstruction = isUnderConstuction;
    this.lastLocatedBlock = lastBlock;
    this.isLastBlockComplete = isLastBlockCompleted;
    this.fileEncryptionInfo = feInfo;
  }
  
  /**
   * Get located blocks.
   */
  public List<LocatedBlock> getLocatedBlocks() {
    return blocks;
  }
  
  /** Get the last located block. */
  public LocatedBlock getLastLocatedBlock() {
    return lastLocatedBlock;
  }
  
  /** Is the last block completed? */
  public boolean isLastBlockComplete() {
    return isLastBlockComplete;
  }

  /**
   * Get located block.
   */
  public LocatedBlock get(int index) {
    return blocks.get(index);
  }
  
  /**
   * Get number of located blocks.
   */
  public int locatedBlockCount() {
    return blocks == null ? 0 : blocks.size();
  }

  /**
   * 
   */
  public long getFileLength() {
    return this.fileLength;
  }

  /**
   * Return true if file was under construction when this LocatedBlocks was
   * constructed, false otherwise.
   */
  public boolean isUnderConstruction() {
    return underConstruction;
  }

  /**
   * @return the FileEncryptionInfo for the LocatedBlocks
   */
  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  /**
   * Find block containing specified offset.
   * 
   * @return block if found, or null otherwise.
   */
  public int findBlock(long offset) {
    // create fake block of size 0 as a key
    LocatedBlock key = new LocatedBlock(
        new ExtendedBlock(), new DatanodeInfo[0], 0L, false);
    key.setStartOffset(offset);
    key.getBlock().setNumBytes(1);
    Comparator<LocatedBlock> comp = 
      new Comparator<LocatedBlock>() {
        // Returns 0 iff a is inside b or b is inside a
        @Override
        public int compare(LocatedBlock a, LocatedBlock b) {
          long aBeg = a.getStartOffset();
          long bBeg = b.getStartOffset();
          long aEnd = aBeg + a.getBlockSize();
          long bEnd = bBeg + b.getBlockSize();
          if(aBeg <= bBeg && bEnd <= aEnd 
              || bBeg <= aBeg && aEnd <= bEnd)
            return 0; // one of the blocks is inside the other
          if(aBeg < bBeg)
            return -1; // a's left bound is to the left of the b's
          return 1;
        }
      };
    return Collections.binarySearch(blocks, key, comp);
  }
  
  public void insertRange(int blockIdx, List<LocatedBlock> newBlocks) {
    int oldIdx = blockIdx;
    int insStart = 0, insEnd = 0;
    for(int newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size(); 
                                                        newIdx++) {
      long newOff = newBlocks.get(newIdx).getStartOffset();
      long oldOff = blocks.get(oldIdx).getStartOffset();
      if(newOff < oldOff) {
        insEnd++;
      } else if(newOff == oldOff) {
        // replace old cached block by the new one
        blocks.set(oldIdx, newBlocks.get(newIdx));
        if(insStart < insEnd) { // insert new blocks
          blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
          oldIdx += insEnd - insStart;
        }
        insStart = insEnd = newIdx+1;
        oldIdx++;
      } else {  // newOff > oldOff
        assert false : "List of LocatedBlock must be sorted by startOffset";
      }
    }
    insEnd = newBlocks.size();
    if(insStart < insEnd) { // insert new blocks
      blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
    }
  }
  
  public static int getInsertIndex(int binSearchResult) {
    return binSearchResult >= 0 ? binSearchResult : -(binSearchResult+1);
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName());
    b.append("{")
     .append("\n  fileLength=").append(fileLength)
     .append("\n  underConstruction=").append(underConstruction)
     .append("\n  blocks=").append(blocks)
     .append("\n  lastLocatedBlock=").append(lastLocatedBlock)
     .append("\n  isLastBlockComplete=").append(isLastBlockComplete)
     .append("}");
    return b.toString();
  }
}
