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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import com.google.common.collect.Lists;

/**
 * An enumeration of logs available on a remote NameNode.
 */
public class RemoteEditLogManifestWritable implements Writable {
  private List<RemoteEditLogWritable> logs;
  
  static { // register a ctor
    WritableFactories.setFactory(RemoteEditLogManifestWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new RemoteEditLogManifestWritable();
          }
        });
  }
  
  public RemoteEditLogManifestWritable() {
  }
  
  public RemoteEditLogManifestWritable(List<RemoteEditLogWritable> logs) {
    this.logs = logs;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(logs.size());
    for (RemoteEditLogWritable log : logs) {
      log.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numLogs = in.readInt();
    logs = Lists.newArrayList();
    for (int i = 0; i < numLogs; i++) {
      RemoteEditLogWritable log = new RemoteEditLogWritable();
      log.readFields(in);
      logs.add(log);
    }
  }

  public static RemoteEditLogManifestWritable convert(
      RemoteEditLogManifest editLogManifest) {
    List<RemoteEditLogWritable> list = Lists.newArrayList();
    for (RemoteEditLog log : editLogManifest.getLogs()) {
      list.add(RemoteEditLogWritable.convert(log));
    }
    return new RemoteEditLogManifestWritable(list);
  }

  public RemoteEditLogManifest convert() {
    List<RemoteEditLog> list = Lists.newArrayList();
    for (RemoteEditLogWritable log : logs) {
      list.add(log.convert());
    }
    return new RemoteEditLogManifest(list);
  }
}
