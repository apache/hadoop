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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;

import com.google.protobuf.ServiceException;

public class ApplicationHistoryProtocolPBClientImpl implements
    ApplicationHistoryProtocol, Closeable {

  private ApplicationHistoryProtocolPB proxy;

  public ApplicationHistoryProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ApplicationHistoryProtocolPB.class,
      ProtobufRpcEngine.class);
    proxy =
        RPC.getProxy(ApplicationHistoryProtocolPB.class, clientVersion, addr,
          conf);
  }

  @Override
  public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    GetApplicationReportRequestProto requestProto =
        ((GetApplicationReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationReportResponsePBImpl(proxy.getApplicationReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationsResponse
      getApplications(GetApplicationsRequest request) throws YarnException,
          IOException {
    GetApplicationsRequestProto requestProto =
        ((GetApplicationsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationsResponsePBImpl(proxy.getApplications(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    GetApplicationAttemptReportRequestProto requestProto =
        ((GetApplicationAttemptReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptReportResponsePBImpl(
        proxy.getApplicationAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    GetApplicationAttemptsRequestProto requestProto =
        ((GetApplicationAttemptsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptsResponsePBImpl(
        proxy.getApplicationAttempts(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    GetContainerReportRequestProto requestProto =
        ((GetContainerReportRequestPBImpl) request).getProto();
    try {
      return new GetContainerReportResponsePBImpl(proxy.getContainerReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    GetContainersRequestProto requestProto =
        ((GetContainersRequestPBImpl) request).getProto();
    try {
      return new GetContainersResponsePBImpl(proxy.getContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    GetDelegationTokenRequestProto requestProto =
        ((GetDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    RenewDelegationTokenRequestProto requestProto =
        ((RenewDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    CancelDelegationTokenRequestProto requestProto =
        ((CancelDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new CancelDelegationTokenResponsePBImpl(
        proxy.cancelDelegationToken(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
