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

package org.apache.hadoop.mapreduce.v2.api.impl.pb.service;

import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsResponse;
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
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class MRClientProtocolPBServiceImpl implements MRClientProtocolPB {

  private MRClientProtocol real;
  
  public MRClientProtocolPBServiceImpl(MRClientProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public GetJobReportResponseProto getJobReport(RpcController controller,
      GetJobReportRequestProto proto) throws ServiceException {
    GetJobReportRequestPBImpl request = new GetJobReportRequestPBImpl(proto);
    try {
      GetJobReportResponse response = real.getJobReport(request);
      return ((GetJobReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskReportResponseProto getTaskReport(RpcController controller,
      GetTaskReportRequestProto proto) throws ServiceException {
    GetTaskReportRequest request = new GetTaskReportRequestPBImpl(proto);
    try {
      GetTaskReportResponse response = real.getTaskReport(request);
      return ((GetTaskReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskAttemptReportResponseProto getTaskAttemptReport(
      RpcController controller, GetTaskAttemptReportRequestProto proto)
      throws ServiceException {
    GetTaskAttemptReportRequest request = new GetTaskAttemptReportRequestPBImpl(proto);
    try {
      GetTaskAttemptReportResponse response = real.getTaskAttemptReport(request);
      return ((GetTaskAttemptReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCountersResponseProto getCounters(RpcController controller,
      GetCountersRequestProto proto) throws ServiceException {
    GetCountersRequest request = new GetCountersRequestPBImpl(proto);
    try {
      GetCountersResponse response = real.getCounters(request);
      return ((GetCountersResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskAttemptCompletionEventsResponseProto getTaskAttemptCompletionEvents(
      RpcController controller,
      GetTaskAttemptCompletionEventsRequestProto proto)
      throws ServiceException {
    GetTaskAttemptCompletionEventsRequest request = new GetTaskAttemptCompletionEventsRequestPBImpl(proto);
    try {
      GetTaskAttemptCompletionEventsResponse response = real.getTaskAttemptCompletionEvents(request);
      return ((GetTaskAttemptCompletionEventsResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskReportsResponseProto getTaskReports(RpcController controller,
      GetTaskReportsRequestProto proto) throws ServiceException {
    GetTaskReportsRequest request = new GetTaskReportsRequestPBImpl(proto);
    try {
      GetTaskReportsResponse response = real.getTaskReports(request);
      return ((GetTaskReportsResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDiagnosticsResponseProto getDiagnostics(RpcController controller,
      GetDiagnosticsRequestProto proto) throws ServiceException {
    GetDiagnosticsRequest request = new GetDiagnosticsRequestPBImpl(proto);
    try {
      GetDiagnosticsResponse response = real.getDiagnostics(request);
      return ((GetDiagnosticsResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, GetDelegationTokenRequestProto proto)
      throws ServiceException {
    GetDelegationTokenRequest request = new GetDelegationTokenRequestPBImpl(proto);
    try {
      GetDelegationTokenResponse response = real.getDelegationToken(request);
      return ((GetDelegationTokenResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public KillJobResponseProto killJob(RpcController controller,
      KillJobRequestProto proto) throws ServiceException {
    KillJobRequest request = new KillJobRequestPBImpl(proto);
    try {
      KillJobResponse response = real.killJob(request);
      return ((KillJobResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public KillTaskResponseProto killTask(RpcController controller,
      KillTaskRequestProto proto) throws ServiceException {
    KillTaskRequest request = new KillTaskRequestPBImpl(proto);
    try {
      KillTaskResponse response = real.killTask(request);
      return ((KillTaskResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public KillTaskAttemptResponseProto killTaskAttempt(RpcController controller,
      KillTaskAttemptRequestProto proto) throws ServiceException {
    KillTaskAttemptRequest request = new KillTaskAttemptRequestPBImpl(proto);
    try {
      KillTaskAttemptResponse response = real.killTaskAttempt(request);
      return ((KillTaskAttemptResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FailTaskAttemptResponseProto failTaskAttempt(RpcController controller,
      FailTaskAttemptRequestProto proto) throws ServiceException {
    FailTaskAttemptRequest request = new FailTaskAttemptRequestPBImpl(proto);
    try {
      FailTaskAttemptResponse response = real.failTaskAttempt(request);
      return ((FailTaskAttemptResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, RenewDelegationTokenRequestProto proto)
      throws ServiceException {
    RenewDelegationTokenRequestPBImpl request =
        new RenewDelegationTokenRequestPBImpl(proto);
      try {
        RenewDelegationTokenResponse response = real.renewDelegationToken(request);
        return ((RenewDelegationTokenResponsePBImpl)response).getProto();
      } catch (YarnRemoteException e) {
        throw new ServiceException(e);
      }
  }

  @Override
  public CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, CancelDelegationTokenRequestProto proto)
      throws ServiceException {
    CancelDelegationTokenRequestPBImpl request =
        new CancelDelegationTokenRequestPBImpl(proto);
      try {
        CancelDelegationTokenResponse response = real.cancelDelegationToken(request);
        return ((CancelDelegationTokenResponsePBImpl)response).getProto();
      } catch (YarnRemoteException e) {
        throw new ServiceException(e);
      }
  }
  
}
