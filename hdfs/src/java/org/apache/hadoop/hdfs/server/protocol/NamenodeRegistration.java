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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;

/**
 * Information sent by a subordinate name-node to the active name-node
 * during the registration process. 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamenodeRegistration extends StorageInfo
implements NodeRegistration {
  String rpcAddress;          // RPC address of the node
  String httpAddress;         // HTTP address of the node
  NamenodeRole role;          // node role
  long checkpointTime = -1L;  // the age of the image

  public NamenodeRegistration() {
    super();
  }

  public NamenodeRegistration(String address,
                              String httpAddress,
                              StorageInfo storageInfo,
                              NamenodeRole role,
                              long checkpointTime) {
    super();
    this.rpcAddress = address;
    this.httpAddress = httpAddress;
    this.setStorageInfo(storageInfo);
    this.role = role;
    this.checkpointTime = checkpointTime;
  }

  @Override // NodeRegistration
  public String getAddress() {
    return rpcAddress;
  }
  
  @Override // NodeRegistration
  public String getRegistrationID() {
    return Storage.getRegistrationID(this);
  }

  @Override // NodeRegistration
  public int getVersion() {
    return super.getLayoutVersion();
  }

  @Override // NodeRegistration
  public String toString() {
    return getClass().getSimpleName()
    + "(" + rpcAddress
    + ", role=" + getRole()
    + ")";
  }

  /**
   * Get name-node role.
   */
  public NamenodeRole getRole() {
    return role;
  }

  public boolean isRole(NamenodeRole that) {
    return role.equals(that);
  }

  /**
   * Get the age of the image.
   */
  public long getCheckpointTime() {
    return checkpointTime;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {
    WritableFactories.setFactory
      (NamenodeRegistration.class,
       new WritableFactory() {
         public Writable newInstance() { return new NamenodeRegistration(); }
       });
  }

  @Override // Writable
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, rpcAddress);
    Text.writeString(out, httpAddress);
    Text.writeString(out, role.name());
    super.write(out);
    out.writeLong(checkpointTime);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    rpcAddress = Text.readString(in);
    httpAddress = Text.readString(in);
    role = NamenodeRole.valueOf(Text.readString(in));
    super.readFields(in);
    checkpointTime = in.readLong();
  }
}
