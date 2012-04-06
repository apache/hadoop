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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/** 
 * DatanodeRegistration class contains all information the name-node needs
 * to identify and verify a data-node when it contacts the name-node.
 * This information is sent by data-node with each communication request.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeRegistration extends DatanodeID
implements Writable, NodeRegistration {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeRegistration.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeRegistration(); }
       });
  }

  private StorageInfo storageInfo;
  private ExportedBlockKeys exportedKeys;

  public DatanodeRegistration() {
    this("", DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        new StorageInfo(), new ExportedBlockKeys());
  }
  
  public DatanodeRegistration(DatanodeID dn, StorageInfo info,
      ExportedBlockKeys keys) {
    super(dn);
    this.storageInfo = info;
    this.exportedKeys = keys;
  }

  public DatanodeRegistration(String ipAddr, int xferPort) {
    this(ipAddr, xferPort, new StorageInfo(), new ExportedBlockKeys());
  }

  public DatanodeRegistration(String ipAddr, int xferPort, StorageInfo info,
      ExportedBlockKeys keys) {
    super(ipAddr, xferPort);
    this.storageInfo = info;
    this.exportedKeys = keys;
  }
  
  public void setStorageInfo(StorageInfo storage) {
    this.storageInfo = new StorageInfo(storage);
  }

  public StorageInfo getStorageInfo() {
    return storageInfo;
  }

  public void setExportedKeys(ExportedBlockKeys keys) {
    this.exportedKeys = keys;
  }

  public ExportedBlockKeys getExportedKeys() {
    return exportedKeys;
  }

  @Override // NodeRegistration
  public int getVersion() {
    return storageInfo.getLayoutVersion();
  }
  
  @Override // NodeRegistration
  public String getRegistrationID() {
    return Storage.getRegistrationID(storageInfo);
  }

  @Override // NodeRegistration
  public String getAddress() {
    return getXferAddr();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
      + "(" + getIpAddr()
      + ", storageID=" + storageID
      + ", infoPort=" + infoPort
      + ", ipcPort=" + ipcPort
      + ", storageInfo=" + storageInfo
      + ")";
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //TODO: move it to DatanodeID once HADOOP-2797 has been committed
    out.writeShort(ipcPort);

    storageInfo.write(out);
    exportedKeys.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    //TODO: move it to DatanodeID once HADOOP-2797 has been committed
    this.ipcPort = in.readShort() & 0x0000ffff;

    storageInfo.readFields(in);
    exportedKeys.readFields(in);
  }
  @Override
  public boolean equals(Object to) {
    return super.equals(to);
  }
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
