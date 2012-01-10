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
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

/**
 * NamespaceInfoWritable is returned by the name-node in reply 
 * to a data-node handshake.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamespaceInfoWritable extends StorageInfo {
  private String  buildVersion;
  private int distributedUpgradeVersion;
  private String blockPoolID = "";
  private StorageInfoWritable storageInfo;

  public NamespaceInfoWritable() {
    super();
    buildVersion = null;
  }
  
  public NamespaceInfoWritable(int nsID, String clusterID, String bpID, 
      long cT, int duVersion) {
    this.blockPoolID = bpID;
    this.buildVersion = Storage.getBuildVersion();
    this.distributedUpgradeVersion = duVersion;
    storageInfo = new StorageInfoWritable(HdfsConstants.LAYOUT_VERSION, nsID,
        clusterID, cT);
  }
  
  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {  // register a ctor
    WritableFactories.setFactory
      (NamespaceInfoWritable.class,
       new WritableFactory() {
         public Writable newInstance() { return new NamespaceInfoWritable(); }
       });
  }

  @Override
  public void write(DataOutput out) throws IOException {
    DeprecatedUTF8.writeString(out, buildVersion);
    storageInfo.write(out);
    out.writeInt(distributedUpgradeVersion);
    WritableUtils.writeString(out, blockPoolID);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    buildVersion = DeprecatedUTF8.readString(in);
    storageInfo.readFields(in);
    distributedUpgradeVersion = in.readInt();
    blockPoolID = WritableUtils.readString(in);
  }

  public static NamespaceInfoWritable convert(NamespaceInfo info) {
    return new NamespaceInfoWritable(info.getNamespaceID(), info.getClusterID(),
        info.getBlockPoolID(), info.getCTime(),
        info.getDistributedUpgradeVersion());
  }
  
  public NamespaceInfo convert() {
    return new NamespaceInfo(namespaceID, clusterID, blockPoolID, cTime,
        distributedUpgradeVersion);
  }
}
