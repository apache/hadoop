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

import static junit.framework.Assert.*;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.junit.Test;

/**
 * Tests for {@link PBHelper}
 */
public class TestPBHelper {
  @Test
  public void testConvertNamenodeRole() {
    assertEquals(NamenodeRoleProto.BACKUP,
        PBHelper.convert(NamenodeRole.BACKUP));
    assertEquals(NamenodeRoleProto.CHECKPOINT,
        PBHelper.convert(NamenodeRole.CHECKPOINT));
    assertEquals(NamenodeRoleProto.NAMENODE,
        PBHelper.convert(NamenodeRole.NAMENODE));
    assertEquals(NamenodeRole.BACKUP,
        PBHelper.convert(NamenodeRoleProto.BACKUP));
    assertEquals(NamenodeRole.CHECKPOINT,
        PBHelper.convert(NamenodeRoleProto.CHECKPOINT));
    assertEquals(NamenodeRole.NAMENODE,
        PBHelper.convert(NamenodeRoleProto.NAMENODE));
  }
  
  @Test
  public void testConvertStoragInfo() {
    StorageInfo info = new StorageInfo(1, 2, "cid", 3);
    StorageInfoProto infoProto = PBHelper.convert(info);
    StorageInfo info2 = PBHelper.convert(infoProto);
    assertEquals(info.getClusterID(), info2.getClusterID());
    assertEquals(info.getCTime(), info2.getCTime());
    assertEquals(info.getLayoutVersion(), info2.getLayoutVersion());
    assertEquals(info.getNamespaceID(), info2.getNamespaceID());
  }
  
  @Test
  public void testConvertNamenodeRegistration() {
    StorageInfo info = new StorageInfo(1, 2, "cid", 3);
    NamenodeRegistration reg = new NamenodeRegistration("address:999",
        "http:1000", info, NamenodeRole.NAMENODE);
    NamenodeRegistrationProto regProto = PBHelper.convert(reg);
    NamenodeRegistration reg2 = PBHelper.convert(regProto);
    assertEquals(reg.getAddress(), reg2.getAddress());
    assertEquals(reg.getClusterID(), reg2.getClusterID());
    assertEquals(reg.getCTime(), reg2.getCTime());
    assertEquals(reg.getHttpAddress(), reg2.getHttpAddress());
    assertEquals(reg.getLayoutVersion(), reg2.getLayoutVersion());
    assertEquals(reg.getNamespaceID(), reg2.getNamespaceID());
    assertEquals(reg.getRegistrationID(), reg2.getRegistrationID());
    assertEquals(reg.getRole(), reg2.getRole());
    assertEquals(reg.getVersion(), reg2.getVersion());
    
  }
}
