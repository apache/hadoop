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
import org.apache.hadoop.hdfs.protocolR23Compatible.DatanodeIDWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExportedBlockKeysWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.StorageInfoWritable;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
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
public class DatanodeRegistrationWritable implements Writable {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeRegistrationWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeRegistrationWritable(); }
       });
  }

  private DatanodeIDWritable datanodeId;
  private StorageInfoWritable storageInfo;
  private ExportedBlockKeysWritable exportedKeys;

  /**
   * Default constructor.
   */
  public DatanodeRegistrationWritable() {
    this("", new StorageInfo(), new ExportedBlockKeys());
  }
  
  /**
   * Create DatanodeRegistration
   */
  public DatanodeRegistrationWritable(String nodeName, StorageInfo info,
      ExportedBlockKeys keys) {
    this.datanodeId = new DatanodeIDWritable(nodeName);
    this.storageInfo = StorageInfoWritable.convert(info);
    this.exportedKeys = ExportedBlockKeysWritable.convert(keys);
  }
  
  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    datanodeId.write(out);

    //TODO: move it to DatanodeID once HADOOP-2797 has been committed
    out.writeShort(datanodeId.ipcPort);

    storageInfo.write(out);
    exportedKeys.write(out);
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    datanodeId.readFields(in);

    //TODO: move it to DatanodeID once HADOOP-2797 has been committed
    datanodeId.ipcPort = in.readShort() & 0x0000ffff;

    storageInfo.readFields(in);
    exportedKeys.readFields(in);
  }

  public DatanodeRegistration convert() {
    DatanodeRegistration dnReg = new DatanodeRegistration(datanodeId.name,
        storageInfo.convert(), exportedKeys.convert());
    dnReg.setIpcPort(datanodeId.ipcPort);
    return dnReg;
  }

  public static DatanodeRegistrationWritable convert(DatanodeRegistration dnReg) {
    if (dnReg == null) return null;
    DatanodeRegistrationWritable ret = new DatanodeRegistrationWritable(
        dnReg.getName(), dnReg.storageInfo, dnReg.exportedKeys);
    ret.datanodeId.ipcPort = dnReg.ipcPort;
    return ret;
  }
}