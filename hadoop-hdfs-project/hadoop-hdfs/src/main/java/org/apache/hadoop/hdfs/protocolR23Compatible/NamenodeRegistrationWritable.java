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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;

/**
 * Information sent by a subordinate name-node to the active name-node
 * during the registration process. 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamenodeRegistrationWritable implements Writable {
  private String rpcAddress;    // RPC address of the node
  private String httpAddress;   // HTTP address of the node
  private NamenodeRole role;    // node role
  private StorageInfoWritable storageInfo;

  public NamenodeRegistrationWritable() { }

  public NamenodeRegistrationWritable(String address,
                              String httpAddress,
                              NamenodeRole role,
                              StorageInfo storageInfo) {
    this.rpcAddress = address;
    this.httpAddress = httpAddress;
    this.role = role;
    this.storageInfo = StorageInfoWritable.convert(storageInfo);
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {
    WritableFactories.setFactory
      (NamenodeRegistrationWritable.class,
       new WritableFactory() {
          public Writable newInstance() {
            return new NamenodeRegistrationWritable();
          }
       });
  }

  @Override // Writable
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, rpcAddress);
    Text.writeString(out, httpAddress);
    Text.writeString(out, role.name());
    storageInfo.write(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    rpcAddress = Text.readString(in);
    httpAddress = Text.readString(in);
    role = NamenodeRole.valueOf(Text.readString(in));
    storageInfo = new StorageInfoWritable();
    storageInfo.readFields(in);
  }

  public static NamenodeRegistrationWritable convert(NamenodeRegistration reg) {
    return new NamenodeRegistrationWritable(reg.getAddress(),
        reg.getHttpAddress(), reg.getRole(), reg);
  }

  public NamenodeRegistration convert() {
    return new NamenodeRegistration(rpcAddress, httpAddress,
        storageInfo.convert(), role);
  }
}
