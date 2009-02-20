/*
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

package org.apache.hadoop.chukwa;

import java.util.*;

import org.apache.hadoop.io.DataOutputBuffer;
import java.io.*;

/**
 * Right now, just handles record collection.
 *
 */
public class ChunkBuilder {
  
  ArrayList<Integer> recOffsets = new ArrayList<Integer>();
  int lastRecOffset = -1;
  DataOutputBuffer buf = new DataOutputBuffer();
  /**
   * Adds the data in rec to an internal buffer; rec can be reused immediately.
   * @param rec
   */
  public void addRecord(byte[] rec)  {
    lastRecOffset = lastRecOffset + rec.length;
    recOffsets.add(lastRecOffset);
    try {
    buf.write(rec);
    } catch(IOException e) {
      throw new RuntimeException("buffer write failed.  Out of memory?", e);
    }
  }
  
  public Chunk getChunk() {
    ChunkImpl c = new ChunkImpl();
    c.setData(buf.getData());
    c.setSeqID(buf.getLength());
    int[] offsets = new int[recOffsets.size()];
    for(int i = 0; i < offsets.length; ++i)
      offsets[i] = recOffsets.get(i);
    c.setRecordOffsets(offsets);
    
    return c;
  }
  
  

}
