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

import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.io.Writable;

public class RegionTransitionEventData implements Writable {
  private HBaseEventType hbEvent;
  private String rsName;
  private long timeStamp;
  private HMsg hmsg;
  
  public RegionTransitionEventData() {
  }

  public RegionTransitionEventData(HBaseEventType hbEvent, String rsName) {
    this(hbEvent, rsName, null);
  }

  public RegionTransitionEventData(HBaseEventType hbEvent, String rsName, HMsg hmsg) {
    this.hbEvent = hbEvent;
    this.rsName = rsName;
    this.timeStamp = System.currentTimeMillis();
    this.hmsg = hmsg;
  }
  
  public HBaseEventType getHbEvent() {
    return hbEvent;
  }

  public String getRsName() {
    return rsName;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public HMsg getHmsg() {
    return hmsg;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // the event type byte
    hbEvent = HBaseEventType.fromByte(in.readByte());
    // the hostname of the RS sending the data
    rsName = in.readUTF();
    // the timestamp
    timeStamp = in.readLong();
    if(in.readBoolean()) {
      // deserialized the HMsg from ZK
      hmsg = new HMsg();
      hmsg.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(hbEvent.getByteValue());
    out.writeUTF(rsName);
    out.writeLong(System.currentTimeMillis());
    out.writeBoolean((hmsg != null));
    if(hmsg != null) {
      hmsg.write(out);
    }
  }

}
