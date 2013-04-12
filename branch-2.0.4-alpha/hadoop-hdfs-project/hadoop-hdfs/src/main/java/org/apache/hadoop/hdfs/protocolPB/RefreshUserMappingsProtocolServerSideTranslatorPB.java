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

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshUserMappingsProtocolServerSideTranslatorPB implements RefreshUserMappingsProtocolPB {

  private final RefreshUserMappingsProtocol impl;
  
  private final static RefreshUserToGroupsMappingsResponseProto 
  VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE =
      RefreshUserToGroupsMappingsResponseProto.newBuilder().build();

  private final static RefreshSuperUserGroupsConfigurationResponseProto
  VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE = 
      RefreshSuperUserGroupsConfigurationResponseProto.newBuilder()
      .build();

  public RefreshUserMappingsProtocolServerSideTranslatorPB(RefreshUserMappingsProtocol impl) {
    this.impl = impl;
  }
  
  @Override
  public RefreshUserToGroupsMappingsResponseProto 
      refreshUserToGroupsMappings(RpcController controller, 
      RefreshUserToGroupsMappingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshUserToGroupsMappings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE;
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto 
      refreshSuperUserGroupsConfiguration(RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.refreshSuperUserGroupsConfiguration();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE;
  }
}
