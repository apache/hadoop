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
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;

import com.google.protobuf.ByteString;

/**
 * Utilities for converting protobuf classes to and from 
 * implementation classes.
 */
class PBHelper {
  private PBHelper() {
    /** Hidden constructor */
  }
  
  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }
  
  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRole.NAMENODE;
    case BACKUP:
      return NamenodeRole.BACKUP;
    case CHECKPOINT:
      return NamenodeRole.CHECKPOINT;
    }
    return null;
  }
  
  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
    case NAMENODE:
      return NamenodeRoleProto.NAMENODE;
    case BACKUP:
      return NamenodeRoleProto.BACKUP;
    case CHECKPOINT:
      return NamenodeRoleProto.CHECKPOINT;
    }
    return null;
  }
  
  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime())
        .setLayoutVersion(info.getLayoutVersion())
        .setNamespceID(info.getNamespaceID())
        .build();
  }
  
  public static StorageInfo convert(StorageInfoProto info) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespceID(),
        info.getClusterID(), info.getCTime());
  }
  
  
  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress())
        .setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }
  
  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        convert(reg.getStorageInfo()), convert(reg.getRole()));
  }
}
