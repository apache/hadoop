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
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

/**
 * Common writable class for storage information.
 */
@InterfaceAudience.Private
public class StorageInfoWritable implements Writable {
  private int layoutVersion;
  private int namespaceID;
  private String clusterID;
  private long cTime;
  
  public StorageInfoWritable () {
    this(0, 0, "", 0L);
  }
  
  public StorageInfoWritable(int layoutV, int nsID, String cid, long cT) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;
  }
  
  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {
    WritableFactories.setFactory(StorageInfoWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new StorageInfoWritable();
          }
        });
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(layoutVersion);
    out.writeInt(namespaceID);
    WritableUtils.writeString(out, clusterID);
    out.writeLong(cTime);
  }

  public void readFields(DataInput in) throws IOException {
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    clusterID = WritableUtils.readString(in);
    cTime = in.readLong();
  }

  public StorageInfo convert() {
    return new StorageInfo(layoutVersion, namespaceID, clusterID, cTime);
  }
  
  public static StorageInfoWritable convert(StorageInfo from) {
    return new StorageInfoWritable(from.getLayoutVersion(),
        from.getNamespaceID(), from.getClusterID(), from.getCTime());
  }
}