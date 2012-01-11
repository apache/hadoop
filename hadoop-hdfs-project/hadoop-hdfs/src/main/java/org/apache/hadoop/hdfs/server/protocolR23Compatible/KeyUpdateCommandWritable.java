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
import org.apache.hadoop.hdfs.protocolR23Compatible.ExportedBlockKeysWritable;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class KeyUpdateCommandWritable extends DatanodeCommandWritable {
  private ExportedBlockKeysWritable keys;

  KeyUpdateCommandWritable() {
    this(new ExportedBlockKeysWritable());
  }

  public KeyUpdateCommandWritable(ExportedBlockKeysWritable keys) {
    super(DatanodeWireProtocol.DNA_ACCESSKEYUPDATE);
    this.keys = keys;
  }

  public ExportedBlockKeysWritable getExportedKeys() {
    return this.keys;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  static { // register a ctor
    WritableFactories.setFactory(KeyUpdateCommandWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new KeyUpdateCommandWritable();
          }
        });
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    keys.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    keys.readFields(in);
  }

  @Override
  public DatanodeCommand convert() {
    return new KeyUpdateCommand(keys.convert());
  }

  public static KeyUpdateCommandWritable convert(KeyUpdateCommand cmd) {
    if (cmd == null) {
      return null;
    }
    return new KeyUpdateCommandWritable(ExportedBlockKeysWritable.convert(cmd
        .getExportedKeys()));
  }
}
