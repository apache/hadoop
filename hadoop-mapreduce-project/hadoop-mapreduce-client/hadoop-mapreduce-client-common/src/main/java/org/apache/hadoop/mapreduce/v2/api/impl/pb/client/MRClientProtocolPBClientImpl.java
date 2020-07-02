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

package org.apache.hadoop.mapreduce.v2.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.CancelDelegationTokenResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.FailTaskAttemptRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.FailTaskAttemptResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetCountersRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetCountersResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDiagnosticsRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDiagnosticsResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetJobReportRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetJobReportResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskAttemptCompletionEventsRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskAttemptCompletionEventsResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskAttemptReportRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskAttemptReportResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskReportRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskReportResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskReportsRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetTaskReportsResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillJobRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillJobResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskAttemptRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskAttemptResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.KillTaskResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.RenewDelegationTokenResponsePBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class MRClientProtocolPBClientImpl implements MRClientProtocol,
    Closeable {

  protected MRClientProtocolPB proxy;
  
  public MRClientProtocolPBClientImpl() {};
  
  public MRClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, MRClientProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy = RPC.getProxy(MRClientProtocolPB.class, clientVersion, addr, conf);
  }
  
  @Override
  public InetSocketAddress getConnectAddress() {
    return RPC.getServerAddress(proxy);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public GetJobReportResponse getJobReport(GetJobReportRequest request)
      throws IOException {
    GetJobReportRequestProto requestProto = ((GetJobReportRequestPBImpl)request).getProto();
    try {
      return new GetJobReportResponsePBImpl(proxy.getJobReport(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
      throws IOException {
    GetTaskReportRequestProto requestProto = ((GetTaskReportRequestPBImpl)request).getProto();
    try {
      return new GetTaskReportResponsePBImpl(proxy.getTaskReport(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetTaskAttemptReportResponse getTaskAttemptReport(
      GetTaskAttemptReportRequest request) throws IOException {
    GetTaskAttemptReportRequestProto requestProto = ((GetTaskAttemptReportRequestPBImpl)request).getProto();
    try {
      return new GetTaskAttemptReportResponsePBImpl(proxy.getTaskAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetCountersResponse getCounters(GetCountersRequest request)
      throws IOException {
    GetCountersRequestProto requestProto = ((GetCountersRequestPBImpl)request).getProto();
    try {
      return new GetCountersResponsePBImpl(proxy.getCounters(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
      GetTaskAttemptCompletionEventsRequest request) throws IOException {
    GetTaskAttemptCompletionEventsRequestProto requestProto = ((GetTaskAttemptCompletionEventsRequestPBImpl)request).getProto();
    try {
      return new GetTaskAttemptCompletionEventsResponsePBImpl(proxy.getTaskAttemptCompletionEvents(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
      throws IOException {
    GetTaskReportsRequestProto requestProto = ((GetTaskReportsRequestPBImpl)request).getProto();
    try {
      return new GetTaskReportsResponsePBImpl(proxy.getTaskReports(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
      throws IOException {
    GetDiagnosticsRequestProto requestProto = ((GetDiagnosticsRequestPBImpl)request).getProto();
    try {
      return new GetDiagnosticsResponsePBImpl(proxy.getDiagnostics(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }
  
  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws IOException {
    GetDelegationTokenRequestProto requestProto = ((GetDelegationTokenRequestPBImpl)
        request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }
  
  @Override
  public KillJobResponse killJob(KillJobRequest request)
      throws IOException {
    KillJobRequestProto requestProto = ((KillJobRequestPBImpl)request).getProto();
    try {
      return new KillJobResponsePBImpl(proxy.killJob(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public KillTaskResponse killTask(KillTaskRequest request)
      throws IOException {
    KillTaskRequestProto requestProto = ((KillTaskRequestPBImpl)request).getProto();
    try {
      return new KillTaskResponsePBImpl(proxy.killTask(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request)
      throws IOException {
    KillTaskAttemptRequestProto requestProto = ((KillTaskAttemptRequestPBImpl)request).getProto();
    try {
      return new KillTaskAttemptResponsePBImpl(proxy.killTaskAttempt(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request)
      throws IOException {
    FailTaskAttemptRequestProto requestProto = ((FailTaskAttemptRequestPBImpl)request).getProto();
    try {
      return new FailTaskAttemptResponsePBImpl(proxy.failTaskAttempt(null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }
 
  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws IOException {
    RenewDelegationTokenRequestProto requestProto = 
        ((RenewDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws IOException {
    CancelDelegationTokenRequestProto requestProto =
        ((CancelDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new CancelDelegationTokenResponsePBImpl(
          proxy.cancelDelegationToken(null, requestProto));

    } catch (ServiceException e) {
      throw unwrapAndThrowException(e);
    }
  }

  private IOException unwrapAndThrowException(ServiceException se) {
    if (se.getCause() instanceof RemoteException) {
      return ((RemoteException) se.getCause()).unwrapRemoteException();
    } else if (se.getCause() instanceof IOException) {
      return (IOException)se.getCause();
    } else {
      throw new UndeclaredThrowableException(se.getCause());
    }
  }
}
