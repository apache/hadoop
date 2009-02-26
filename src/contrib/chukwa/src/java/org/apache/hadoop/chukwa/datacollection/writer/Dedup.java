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
package org.apache.hadoop.chukwa.datacollection.writer;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;

public class Dedup implements PipelineableWriter {
  
  static final class DedupKey {
    String name;
    long val; //sequence number
    
    public DedupKey(String n, long p) {
      name = n;
      val = p;
    }

    public int hashCode() {
      return (int) (name.hashCode() ^ val ^ (val >> 32));
    }

    public boolean equals(Object dk) {
      if(dk instanceof DedupKey)
        return name.equals(((DedupKey)dk).name) && val == ((DedupKey)dk).val;
      else return false;
    }
  }
  
  static class FixedSizeCache<EntryType> {
    final HashSet<EntryType> hs;
    final Queue<EntryType> toDrop;
    final int maxSize;
    volatile long dupchunks =0;
    public FixedSizeCache(int size) {
      maxSize = size;
      hs = new HashSet<EntryType>(maxSize);
      toDrop = new ArrayDeque<EntryType>(maxSize);
    }
    
    public synchronized void add(EntryType t) {
      if(maxSize == 0)
        return;
      
      if(hs.size() >= maxSize) 
        while(hs.size() >= maxSize) {
          EntryType td = toDrop.remove();
          hs.remove(td);
        }
      
      hs.add(t);
      toDrop.add(t);
    }
    
    private synchronized boolean addAndCheck(EntryType t) {
      if(maxSize == 0)
        return false;
      
      boolean b= hs.contains(t);
      if(b)
        dupchunks++;
      else {
        hs.add(t);
        toDrop.add(t);
      }
      return b;
    }
    
    private long dupCount() {
      return dupchunks;
    }
  }
  

  FixedSizeCache<DedupKey> cache;
  ChukwaWriter next;

  @Override
  public void setNextStage(ChukwaWriter next) {
    this.next = next;
  }

  @Override
  public void add(List<Chunk> chunks) throws WriterException {
    ArrayList<Chunk> passedThrough = new ArrayList<Chunk>();
    for(Chunk c: chunks)
      if(! cache.addAndCheck(new DedupKey(c.getStreamName(), c.getSeqID())))
        passedThrough.add(c);
    
    if(!passedThrough.isEmpty())
      next.add(passedThrough);
  }

  @Override
  public void close() throws WriterException {
    next.close();
  }

  @Override
  public void init(Configuration c) throws WriterException {
    int csize = c.getInt("chukwaCollector.chunkSuppressBufferSize", 0);
    cache = new FixedSizeCache<DedupKey>(csize);
  }

}
