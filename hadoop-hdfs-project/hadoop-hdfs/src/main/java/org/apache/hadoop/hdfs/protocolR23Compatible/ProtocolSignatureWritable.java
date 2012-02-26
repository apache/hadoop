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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProtocolSignatureWritable implements Writable {
  static {               // register a ctor
    WritableFactories.setFactory
      (ProtocolSignatureWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new ProtocolSignatureWritable(); }
       });
  }

  private long version;
  private int[] methods = null; // an array of method hash codes
  
  public static org.apache.hadoop.ipc.ProtocolSignature convert(
      final ProtocolSignatureWritable ps) {
    if (ps == null) return null;
    return new org.apache.hadoop.ipc.ProtocolSignature(
        ps.getVersion(), ps.getMethods());
  }
  
  public static ProtocolSignatureWritable convert(
      final org.apache.hadoop.ipc.ProtocolSignature ps) {
    if (ps == null) return null;
    return new ProtocolSignatureWritable(ps.getVersion(), ps.getMethods());
  }
  
  /**
   * default constructor
   */
  public ProtocolSignatureWritable() {
  }
  
  /**
   * Constructor
   * 
   * @param version server version
   * @param methodHashcodes hash codes of the methods supported by server
   */
  public ProtocolSignatureWritable(long version, int[] methodHashcodes) {
    this.version = version;
    this.methods = methodHashcodes;
  }
  
  public long getVersion() {
    return version;
  }
  
  public int[] getMethods() {
    return methods;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    version = in.readLong();
    boolean hasMethods = in.readBoolean();
    if (hasMethods) {
      int numMethods = in.readInt();
      methods = new int[numMethods];
      for (int i=0; i<numMethods; i++) {
        methods[i] = in.readInt();
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(version);
    if (methods == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(methods.length);
      for (int method : methods) {
        out.writeInt(method);
      }
    }
  }
}

