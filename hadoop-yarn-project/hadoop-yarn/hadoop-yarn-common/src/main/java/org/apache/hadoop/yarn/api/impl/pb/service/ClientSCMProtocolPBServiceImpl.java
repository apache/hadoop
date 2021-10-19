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

package org.apache.hadoop.yarn.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.ClientSCMProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReleaseSharedCacheResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReleaseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReleaseSharedCacheResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReleaseSharedCacheResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UseSharedCacheResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UseSharedCacheResourceResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class ClientSCMProtocolPBServiceImpl implements ClientSCMProtocolPB {

  private ClientSCMProtocol real;

  public ClientSCMProtocolPBServiceImpl(ClientSCMProtocol impl) {
    this.real = impl;
  }

  @Override
  public UseSharedCacheResourceResponseProto use(RpcController controller,
      UseSharedCacheResourceRequestProto proto) throws ServiceException {
    UseSharedCacheResourceRequestPBImpl request =
        new UseSharedCacheResourceRequestPBImpl(proto);
    try {
      UseSharedCacheResourceResponse response = real.use(request);
      return ((UseSharedCacheResourceResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReleaseSharedCacheResourceResponseProto release(
      RpcController controller, ReleaseSharedCacheResourceRequestProto proto)
      throws ServiceException {
    ReleaseSharedCacheResourceRequestPBImpl request =
        new ReleaseSharedCacheResourceRequestPBImpl(proto);
    try {
      ReleaseSharedCacheResourceResponse response = real.release(request);
      return ((ReleaseSharedCacheResourceResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
