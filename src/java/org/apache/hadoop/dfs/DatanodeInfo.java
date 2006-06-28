/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/**************************************************
 * DatanodeInfo tracks stats on a given DataNode,
 * such as available storage capacity, last update
 * time, etc.
 *
 * @author Mike Cafarella
 **************************************************/
public class DatanodeInfo extends DatanodeID implements Writable, Comparable {
  private int xceiverCount;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (DatanodeInfo.class,
         new WritableFactory() {
           public Writable newInstance() { return new DatanodeInfo(); }
         });
    }
  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }
  
    private long capacityBytes, remainingBytes, lastUpdate;
    private volatile TreeSet blocks;
    /** Create an empty DatanodeInfo.
     */
    public DatanodeInfo() {
        this(new String(), new String(), 0, 0, 0);
    }
    public DatanodeInfo( DatanodeID nodeID ) {
      this( nodeID.getName(), nodeID.getStorageID(), 0, 0, 0);
    }
    
   /**
    * Create an empty DatanodeInfo.
    */
    public DatanodeInfo(DatanodeID nodeID, 
                        long capacity, 
                        long remaining,
                        int xceiverCount) {
      this( nodeID.getName(), nodeID.getStorageID(), capacity, remaining, xceiverCount );
    }
   /**
    * @param name hostname:portNumber as String object.
    */
    public DatanodeInfo(String name, 
                        String storageID, 
                        long capacity, 
                        long remaining,
                        int xceiverCount) {
        super( name, storageID );
        this.blocks = new TreeSet();
        updateHeartbeat(capacity, remaining, xceiverCount);
    }
   /**
    */
    public void updateBlocks(Block newBlocks[]) {
        blocks.clear();
        for (int i = 0; i < newBlocks.length; i++) {
            blocks.add(newBlocks[i]);
        }
    }

   /**
    */
    public void addBlock(Block b) {
        blocks.add(b);
    }

    /**
     */
    public void updateHeartbeat(long capacity, long remaining, int xceiverCount) {
        this.capacityBytes = capacity;
        this.remainingBytes = remaining;
        this.xceiverCount = xceiverCount;
        this.lastUpdate = System.currentTimeMillis();
    }

    public Block[] getBlocks() {
        return (Block[]) blocks.toArray(new Block[blocks.size()]);
    }
    public Iterator getBlockIterator() {
        return blocks.iterator();
    }
    public long getCapacity() {
        return capacityBytes;
    }
    public long getRemaining() {
        return remainingBytes;
    }
    public long lastUpdate() {
        return lastUpdate;
    }

  /** Comparable.
   * Basis of compare is the String name (host:portNumber) only.
   * @param o
   * @return as specified by Comparable.
   */
    public int compareTo(Object o) {
        DatanodeInfo d = (DatanodeInfo) o;
        return name.compareTo(d.getName());
    }
    /////////////////////////////////////////////////
    // Writable
    /////////////////////////////////////////////////
    /**
     */
    public void write(DataOutput out) throws IOException {
        new UTF8( this.name ).write(out);
        new UTF8( this.storageID ).write(out);
        out.writeLong(capacityBytes);
        out.writeLong(remainingBytes);
        out.writeLong(lastUpdate);
        out.writeInt(xceiverCount);

        /**
        out.writeInt(blocks.length);
        for (int i = 0; i < blocks.length; i++) {
            blocks[i].write(out);
        }
        **/
    }

    /**
     */
    public void readFields(DataInput in) throws IOException {
        UTF8 uStr = new UTF8();
        uStr.readFields(in);
        this.name = uStr.toString();
        uStr.readFields(in);
        this.storageID = uStr.toString();
        this.capacityBytes = in.readLong();
        this.remainingBytes = in.readLong();
        this.lastUpdate = in.readLong();
        this.xceiverCount = in.readInt();
        /**
        int numBlocks = in.readInt();
        this.blocks = new Block[numBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new Block();
            blocks[i].readFields(in);
        }
        **/
    }
}

