/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.executor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * Data serialized into ZooKeeper for region transitions.
 */
public class RegionTransitionData implements Writable {
  /**
   * Type of transition event (offline, opening, opened, closing, closed).
   * Required.
   */
  private EventType eventType;

  /** Region being transitioned.  Required. */
  private byte [] regionName;

  /** Server event originated from.  Optional. */
  private ServerName origin;

  /** Time the event was created.  Required but automatically set. */
  private long stamp;

  private byte [] payload;

  /**
   * Writable constructor.  Do not use directly.
   */
  public RegionTransitionData() {}

  /**
   * Construct data for a new region transition event with the specified event
   * type and region name.
   *
   * <p>Used when the server name is not known (the master is setting it).  This
   * happens during cluster startup or during failure scenarios.  When
   * processing a failed regionserver, the master assigns the regions from that
   * server to other servers though the region was never 'closed'.  During
   * master failover, the new master may have regions stuck in transition
   * without a destination so may have to set regions offline and generate a new
   * assignment.
   *
   * <p>Since only the master uses this constructor, the type should always be
   * {@link EventType#M_ZK_REGION_OFFLINE}.
   *
   * @param eventType type of event
   * @param regionName name of region as per <code>HRegionInfo#getRegionName()</code>
   */
  public RegionTransitionData(EventType eventType, byte [] regionName) {
    this(eventType, regionName, null);
  }

  /**
   * Construct data for a new region transition event with the specified event
   * type, region name, and server name.
   *
   * <p>Used when the server name is known (a regionserver is setting it).
   *
   * <p>Valid types for this constructor are {@link EventType#RS_ZK_REGION_CLOSING},
   * {@link EventType#RS_ZK_REGION_CLOSED}, {@link EventType#RS_ZK_REGION_OPENING},
   * {@link EventType#RS_ZK_REGION_SPLITTING},
   * and {@link EventType#RS_ZK_REGION_OPENED}.
   *
   * @param eventType type of event
   * @param regionName name of region as per <code>HRegionInfo#getRegionName()</code>
   * @param origin Originating {@link ServerName}
   */
  public RegionTransitionData(EventType eventType, byte [] regionName,
      final ServerName origin) {
    this(eventType, regionName, origin, null);
  }

  /**
   * Construct data for a new region transition event with the specified event
   * type, region name, and server name.
   *
   * <p>Used when the server name is known (a regionserver is setting it).
   *
   * <p>Valid types for this constructor are {@link EventType#RS_ZK_REGION_SPLIT}
   * since SPLIT is only type that currently carries a payload.
   *
   * @param eventType type of event
   * @param regionName name of region as per <code>HRegionInfo#getRegionName()</code>
   * @param origin Originating {@link ServerName}
   * @param payload Payload examples include the daughters involved in a
   * {@link EventType#RS_ZK_REGION_SPLIT}. Can be null
   */
  public RegionTransitionData(EventType eventType, byte [] regionName,
      final ServerName serverName, final byte [] payload) {
    this.eventType = eventType;
    this.stamp = System.currentTimeMillis();
    this.regionName = regionName;
    this.origin = serverName;
    this.payload = payload;
  }

  /**
   * Gets the type of region transition event.
   *
   * <p>One of:
   * <ul>
   * <li>{@link EventType#M_ZK_REGION_OFFLINE}
   * <li>{@link EventType#RS_ZK_REGION_CLOSING}
   * <li>{@link EventType#RS_ZK_REGION_CLOSED}
   * <li>{@link EventType#RS_ZK_REGION_OPENING}
   * <li>{@link EventType#RS_ZK_REGION_OPENED}
   * <li>{@link EventType#RS_ZK_REGION_SPLITTING}
   * <li>{@link EventType#RS_ZK_REGION_SPLIT}
   * </ul>
   * @return type of region transition event
   */
  public EventType getEventType() {
    return eventType;
  }

  /**
   * Gets the name of the region being transitioned.
   *
   * <p>Region name is required so this never returns null.
   * @return region name, the result of a call to <code>HRegionInfo#getRegionName()</code>
   */
  public byte [] getRegionName() {
    return regionName;
  }

  /**
   * Gets the server the event originated from.  If null, this event originated
   * from the master.
   *
   * @return server name of originating regionserver, or null if from master
   */
  public ServerName getOrigin() {
    return origin;
  }

  /**
   * Gets the timestamp when this event was created.
   *
   * @return stamp event was created
   */
  public long getStamp() {
    return stamp;
  }

  /**
   * @return Payload if any.
   */
  public byte [] getPayload() {
    return this.payload;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // the event type byte
    eventType = EventType.values()[in.readShort()];
    // the timestamp
    stamp = in.readLong();
    // the encoded name of the region being transitioned
    regionName = Bytes.readByteArray(in);
    // remaining fields are optional so prefixed with boolean
    // the name of the regionserver sending the data
    if (in.readBoolean()) {
      this.origin = new ServerName(in.readUTF());
    }
    if (in.readBoolean()) {
      this.payload = Bytes.readByteArray(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(eventType.ordinal());
    out.writeLong(System.currentTimeMillis());
    Bytes.writeByteArray(out, regionName);
    // remaining fields are optional so prefixed with boolean
    out.writeBoolean(this.origin != null);
    if(this.origin != null) {
      out.writeUTF(this.origin.toString());
    }
    out.writeBoolean(this.payload != null);
    if (this.payload != null) {
      Bytes.writeByteArray(out, this.payload);
    }
  }

  /**
   * Get the bytes for this instance.  Throws a {@link RuntimeException} if
   * there is an error deserializing this instance because it represents a code
   * bug.
   * @return binary representation of this instance
   */
  public byte [] getBytes() {
    try {
      return Writables.getBytes(this);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get an instance from bytes.  Throws a {@link RuntimeException} if
   * there is an error serializing this instance from bytes because it
   * represents a code bug.
   * @param bytes binary representation of this instance
   * @return instance of this class
   */
  public static RegionTransitionData fromBytes(byte [] bytes) {
    try {
      RegionTransitionData data = new RegionTransitionData();
      Writables.getWritable(bytes, data);
      return data;
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "region=" + Bytes.toStringBinary(regionName) + ", origin=" + this.origin +
      ", state=" + eventType;
  }
}