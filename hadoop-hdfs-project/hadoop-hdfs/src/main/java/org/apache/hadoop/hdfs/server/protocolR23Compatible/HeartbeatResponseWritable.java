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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HeartbeatResponseWritable implements Writable {
  private DatanodeCommandWritable[] commands;
  
  public HeartbeatResponseWritable() {
    // Empty constructor for Writable
  }
  
  public HeartbeatResponseWritable(DatanodeCommandWritable[] cmds) {
    commands = cmds;
  }
  
  public HeartbeatResponse convert() {
    return new HeartbeatResponse(DatanodeCommandWritable.convert(commands));
  }
  
  ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    int length = commands == null ? 0 : commands.length;
    out.writeInt(length);
    for (int i = 0; i < length; i++) {
      ObjectWritable.writeObject(out, commands[i], commands[i].getClass(),
                                 null, true);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    commands = new DatanodeCommandWritable[length];
    ObjectWritable objectWritable = new ObjectWritable();
    for (int i = 0; i < length; i++) {
      commands[i] = (DatanodeCommandWritable) ObjectWritable.readObject(in,
          objectWritable, null);
    }
  }

  public static HeartbeatResponseWritable convert(
      HeartbeatResponse resp) {
    return new HeartbeatResponseWritable(DatanodeCommandWritable.convert(resp
        .getCommands()));
  }
}