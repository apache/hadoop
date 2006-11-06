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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/** 
 * DatanodeInfo represents the status of a DataNode.
 *
 * @author Mike Cafarella
 * @author Konstantin Shvachko
 */
public class DatanodeInfo extends DatanodeID {
  protected long capacity;
  protected long remaining;
  protected long lastUpdate;
  protected int xceiverCount;

  DatanodeInfo() {
    super();
  }
  
  DatanodeInfo( DatanodeInfo from ) {
    super( from );
    this.capacity = from.getCapacity();
    this.remaining = from.getRemaining();
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
  }

  DatanodeInfo( DatanodeID nodeID ) {
    super( nodeID );
    this.capacity = 0L;
    this.remaining = 0L;
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
  }
  
  /** The raw capacity. */
  public long getCapacity() { return capacity; }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }

  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }

  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = c - r;
    buffer.append("Name: "+name+"\n");
    buffer.append("Total raw bytes: "+c+" ("+DFSShell.byteDesc(c)+")"+"\n");
    buffer.append("Used raw bytes: "+u+" ("+DFSShell.byteDesc(u)+")"+"\n");
    buffer.append("% used: "+DFSShell.limitDecimal(((1.0*u)/c)*100,2)+"%"+"\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfo(); }
       });
  }

  /**
   */
  public void write(DataOutput out) throws IOException {
    super.write( out );
    out.writeLong(capacity);
    out.writeLong(remaining);
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.capacity = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
  }
}
