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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshAdminAclsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLoadedJobCacheRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshJobRetentionSettingsRequestProto;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.RefreshLogRetentionSettingsRequestProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

@Private
public class HSAdminRefreshProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, HSAdminRefreshProtocol, Closeable {

  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;

  private final HSAdminRefreshProtocolPB rpcProxy;

  private final static RefreshAdminAclsRequestProto 
    VOID_REFRESH_ADMIN_ACLS_REQUEST = RefreshAdminAclsRequestProto
      .newBuilder().build();

  private final static RefreshLoadedJobCacheRequestProto 
    VOID_REFRESH_LOADED_JOB_CACHE_REQUEST = RefreshLoadedJobCacheRequestProto
      .newBuilder().build();
  
  private final static RefreshJobRetentionSettingsRequestProto 
    VOID_REFRESH_JOB_RETENTION_SETTINGS_REQUEST = 
       RefreshJobRetentionSettingsRequestProto.newBuilder().build();
  
  private final static RefreshLogRetentionSettingsRequestProto 
    VOID_REFRESH_LOG_RETENTION_SETTINGS_REQUEST = 
      RefreshLogRetentionSettingsRequestProto.newBuilder().build();

  public HSAdminRefreshProtocolClientSideTranslatorPB(
      HSAdminRefreshProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void refreshAdminAcls() throws IOException {
    try {
      rpcProxy.refreshAdminAcls(NULL_CONTROLLER,
          VOID_REFRESH_ADMIN_ACLS_REQUEST);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }


  @Override
  public void refreshLoadedJobCache() throws IOException {
    try {
      rpcProxy.refreshLoadedJobCache(NULL_CONTROLLER,
          VOID_REFRESH_LOADED_JOB_CACHE_REQUEST);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }
  
  @Override
  public void refreshJobRetentionSettings() throws IOException {
    try {
      rpcProxy.refreshJobRetentionSettings(NULL_CONTROLLER,
          VOID_REFRESH_JOB_RETENTION_SETTINGS_REQUEST);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void refreshLogRetentionSettings() throws IOException {
    try {
      rpcProxy.refreshLogRetentionSettings(NULL_CONTROLLER,
          VOID_REFRESH_LOG_RETENTION_SETTINGS_REQUEST);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        HSAdminRefreshProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(HSAdminRefreshProtocolPB.class), methodName);
  }

}
