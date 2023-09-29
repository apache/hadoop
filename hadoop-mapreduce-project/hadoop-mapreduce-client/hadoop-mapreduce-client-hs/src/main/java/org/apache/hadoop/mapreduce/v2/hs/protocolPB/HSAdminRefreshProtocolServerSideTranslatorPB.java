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

package org.apache.hadoop.mapreduce.v2.hs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsResponseProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

@Private
public class HSAdminRefreshProtocolServerSideTranslatorPB implements
    HSAdminRefreshProtocolPB {

  private final HSAdminRefreshProtocol impl;

  private final static RefreshAdminAclsResponseProto 
    VOID_REFRESH_ADMIN_ACLS_RESPONSE = RefreshAdminAclsResponseProto
      .newBuilder().build();

  private final static RefreshLoadedJobCacheResponseProto 
    VOID_REFRESH_LOADED_JOB_CACHE_RESPONSE = RefreshLoadedJobCacheResponseProto
      .newBuilder().build();

  private final static RefreshJobRetentionSettingsResponseProto 
    VOID_REFRESH_JOB_RETENTION_SETTINGS_RESPONSE = 
      RefreshJobRetentionSettingsResponseProto.newBuilder().build();

  private final static RefreshLogRetentionSettingsResponseProto 
    VOID_REFRESH_LOG_RETENTION_SETTINGS_RESPONSE = 
      RefreshLogRetentionSettingsResponseProto.newBuilder().build();

  public HSAdminRefreshProtocolServerSideTranslatorPB(
      HSAdminRefreshProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RefreshAdminAclsResponseProto refreshAdminAcls(
      RpcController controller, RefreshAdminAclsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshAdminAcls();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_ADMIN_ACLS_RESPONSE;
  }

  @Override
  public RefreshLoadedJobCacheResponseProto refreshLoadedJobCache(
      RpcController controller, RefreshLoadedJobCacheRequestProto request)
      throws ServiceException {
    try {
      impl.refreshLoadedJobCache();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_LOADED_JOB_CACHE_RESPONSE;
  }

  @Override
  public RefreshJobRetentionSettingsResponseProto refreshJobRetentionSettings(
      RpcController controller, 
      RefreshJobRetentionSettingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshJobRetentionSettings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_JOB_RETENTION_SETTINGS_RESPONSE;
  }

  @Override
  public RefreshLogRetentionSettingsResponseProto refreshLogRetentionSettings(
      RpcController controller, 
      RefreshLogRetentionSettingsRequestProto request)
      throws ServiceException {
    try {
      impl.refreshLogRetentionSettings();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_LOG_RETENTION_SETTINGS_RESPONSE;
  }
}
