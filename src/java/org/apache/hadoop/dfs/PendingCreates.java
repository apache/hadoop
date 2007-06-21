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
import java.util.*;

/***************************************************
 * PendingCreates does the bookkeeping of all
 * blocks that are in transit.
 *
 * It does the following:
 * 1)  keep a map of pending-file to its blocks.
 *     Mapping: fileName -> FileUnderConstruction
 * 2)  a global set of all blocks that are part of all pending files.
 *
 ***************************************************/
class PendingCreates {
  private Map<StringBytesWritable, FileUnderConstruction> pendingCreates = 
                    new TreeMap<StringBytesWritable, FileUnderConstruction>();

  //
  // Keeps track of the blocks that are part of files that are being
  // created. 
  //
  private Collection<Block> pendingCreateBlocks = new TreeSet<Block>();


  //
  // returns a file if it is being created. Otherwise returns null.
  //
  FileUnderConstruction get(String src) throws IOException {
    return pendingCreates.get(new StringBytesWritable(src));
  }

  //
  // inserts a filename into pendingCreates. throws exception if it 
  // already exists
  //
  void put(String src, FileUnderConstruction file) throws IOException {
    FileUnderConstruction oldfile = pendingCreates.put(
                                          new StringBytesWritable(src), file);
    if (oldfile != null && oldfile != file) {
      throw new IOException("Duplicate entry " + src + " in pendingCreates.");
    }
  }

  //
  // The specified file is no longer pending.
  //
  boolean remove(String src) throws IOException {
    FileUnderConstruction v = pendingCreates.remove(
                                          new StringBytesWritable(src));
    if (v != null) {
      for (Iterator<Block> it2 = v.getBlocks().iterator(); it2.hasNext(); ) {
        Block b = it2.next();
        pendingCreateBlocks.remove(b);
      }
      return true;
    }
    return false;
  }

  //
  // Make this block part of this file. This block
  // should not already exists in here.
  //
  boolean addBlock(String src, Block b) throws IOException {
    FileUnderConstruction v =  get(src);
    assert !pendingCreateBlocks.contains(b);
    v.getBlocks().add(b);
    pendingCreateBlocks.add(b);
    return true;
  }

  //
  // Remove this block from a file.
  //
  boolean removeBlock(String src, Block b) throws IOException {
    FileUnderConstruction v =  get(src);
    if (v != null) {
      Collection<Block> pendingVector = v.getBlocks();
      for (Iterator<Block> it = pendingVector.iterator(); it.hasNext(); ) {
        Block cur = it.next();
        if (cur.compareTo(b) == 0) {
          pendingCreateBlocks.remove(b);
          it.remove();
          return true;
        }
      }
    }
    return false;
  }

  //
  // Returns true if this block is is pendingCreates
  //
  boolean contains(Block b) {
    return pendingCreateBlocks.contains(b);
  }
}
  
