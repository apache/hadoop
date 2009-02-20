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

package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import java.util.ArrayList;

/**
 * A subclass of FileTailingAdaptor that reads UTF8/ascii
 * files and splits records at carriage returns.
 *
 */
public class CharFileTailingAdaptorUTF8 extends FileTailingAdaptor {
  


  private static final char SEPARATOR = '\n';
  
  private ArrayList<Integer> offsets = new ArrayList<Integer>();
  
  /**
   * 
   * Note: this method uses a temporary ArrayList (shared across instances).
   * This means we're copying ints each time. This could be a performance issue.
   * Also, 'offsets' never shrinks, and will be of size proportional to the 
   * largest number of lines ever seen in an event.
   */
  @Override
    protected int extractRecords(ChunkReceiver eq, long buffOffsetInFile, byte[] buf)
    throws InterruptedException
  {
      for(int i = 0; i < buf.length; ++i) {
        if(buf[i] == SEPARATOR) {
          offsets.add(i);
        }
      }

      if(offsets.size() > 0)  {
        int[] offsets_i = new int[offsets.size()];
        for(int i = 0; i < offsets_i.length ; ++i)
          offsets_i[i] = offsets.get(i);
      
        int bytesUsed = offsets_i[offsets_i.length-1]  + 1; //char at last offset uses a byte
        assert bytesUsed > 0: " shouldn't send empty events";
        ChunkImpl event = new ChunkImpl(type, toWatch.getAbsolutePath(),buffOffsetInFile + bytesUsed, buf, this );

        event.setRecordOffsets(offsets_i);
        eq.add(event);
        
        offsets.clear();
        return bytesUsed;
      }
      else
        return 0;
      
  }

  
}
