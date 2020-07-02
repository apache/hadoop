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

package org.apache.hadoop.security.protocolPB;

import java.io.IOException;

import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class RefreshAuthorizationPolicyProtocolServerSideTranslatorPB implements
    RefreshAuthorizationPolicyProtocolPB {

  private final RefreshAuthorizationPolicyProtocol impl;

  private final static RefreshServiceAclResponseProto
  VOID_REFRESH_SERVICE_ACL_RESPONSE = RefreshServiceAclResponseProto
      .newBuilder().build();

  public RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(
      RefreshAuthorizationPolicyProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RefreshServiceAclResponseProto refreshServiceAcl(
      RpcController controller, RefreshServiceAclRequestProto request)
      throws ServiceException {
    try {
      impl.refreshServiceAcl();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REFRESH_SERVICE_ACL_RESPONSE;
  }
}
