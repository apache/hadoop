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
package org.apache.hadoop.hdfs.server.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.Time;

/**
 * This window makes sure to keep blocks that have been moved within a fixed
 * time interval (default is 1.5 hour). Old window has blocks that are older;
 * Current window has blocks that are more recent; Cleanup method triggers the
 * check if blocks in the old window are more than the fixed time interval. If
 * yes, purge the old window and then move blocks in current window to old
 * window.
 * 
 * @param <L> Location type
 */
public class MovedBlocks<L> {
  /** A class for keeping track of a block and its locations */
  public static class Locations<L> {
    private final Block block; // the block
    /** The locations of the replicas of the block. */
    private final List<L> locations = new ArrayList<L>(3);
    
    public Locations(Block block) {
      this.block = block;
    }
    
    /** clean block locations */
    public synchronized void clearLocations() {
      locations.clear();
    }
    
    /** add a location */
    public synchronized void addLocation(L loc) {
      if (!locations.contains(loc)) {
        locations.add(loc);
      }
    }
    
    /** @return if the block is located on the given location. */
    public synchronized boolean isLocatedOn(L loc) {
      return locations.contains(loc);
    }
    
    /** @return its locations */
    public synchronized List<L> getLocations() {
      return locations;
    }
    
    /* @return the block */
    public Block getBlock() {
      return block;
    }
    
    /* Return the length of the block */
    public long getNumBytes() {
      return block.getNumBytes();
    }
  }

  private static final int CUR_WIN = 0;
  private static final int OLD_WIN = 1;
  private static final int NUM_WINS = 2;

  private final long winTimeInterval;
  private long lastCleanupTime = Time.monotonicNow();
  private final List<Map<Block, Locations<L>>> movedBlocks
      = new ArrayList<Map<Block, Locations<L>>>(NUM_WINS);
  
  /** initialize the moved blocks collection */
  public MovedBlocks(long winTimeInterval) {
    this.winTimeInterval = winTimeInterval;
    movedBlocks.add(newMap());
    movedBlocks.add(newMap());
  }

  private Map<Block, Locations<L>> newMap() {
    return new HashMap<Block, Locations<L>>();
  }

  /** add a block thus marking a block to be moved */
  public synchronized void put(Locations<L> block) {
    movedBlocks.get(CUR_WIN).put(block.getBlock(), block);
  }

  /** @return if a block is marked as moved */
  public synchronized boolean contains(Block block) {
    return movedBlocks.get(CUR_WIN).containsKey(block) ||
      movedBlocks.get(OLD_WIN).containsKey(block);
  }

  /** remove old blocks */
  public synchronized void cleanup() {
    long curTime = Time.monotonicNow();
    // check if old win is older than winWidth
    if (lastCleanupTime + winTimeInterval <= curTime) {
      // purge the old window
      movedBlocks.set(OLD_WIN, movedBlocks.get(CUR_WIN));
      movedBlocks.set(CUR_WIN, newMap());
      lastCleanupTime = curTime;
    }
  }
}