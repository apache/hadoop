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
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

/**
 * A FinalizeCommand is an instruction from namenode to finalize the previous
 * upgrade to a datanode
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FinalizeCommandWritable extends DatanodeCommandWritable {
  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(FinalizeCommandWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new FinalizeCommandWritable();
          }
        });
  }

  String blockPoolId;

  private FinalizeCommandWritable() {
    this(null);
  }

  public FinalizeCommandWritable(String bpid) {
    super(DatanodeWireProtocol.DNA_FINALIZE);
    blockPoolId = bpid;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blockPoolId = WritableUtils.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, blockPoolId);
  }

  @Override
  public DatanodeCommand convert() {
    return new FinalizeCommand(blockPoolId);
  }

  public static FinalizeCommandWritable convert(FinalizeCommand cmd) {
    if (cmd == null) {
      return null;
    }
    return new FinalizeCommandWritable(cmd.getBlockPoolId());
  }
}