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

package org.apache.hadoop.mapreduce.v2;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;

import static org.junit.jupiter.api.Assertions.fail;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.junit.jupiter.api.Test;

public class TestRPCFactories {


  @Test
  public void test() {
    testPbServerFactory();

    testPbClientFactory();
  }
  
  
  
  private void testPbServerFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    Configuration conf = new Configuration();
    MRClientProtocol instance = new MRClientProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
          MRClientProtocol.class, instance, addr, conf, null, 1);
      server.start();
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete server");
    } finally {
      server.stop();
    }
  }

  
  private void testPbClientFactory() {
    InetSocketAddress addr = new InetSocketAddress(0);
    System.err.println(addr.getHostName() + addr.getPort());
    Configuration conf = new Configuration();
    MRClientProtocol instance = new MRClientProtocolTestImpl();
    Server server = null;
    try {
      server = 
        RpcServerFactoryPBImpl.get().getServer(
            MRClientProtocol.class, instance, addr, conf, null, 1);
      server.start();
      System.err.println(server.getListenerAddress());
      System.err.println(NetUtils.getConnectAddress(server));

      MRClientProtocol client = null;
      try {
        client = (MRClientProtocol) RpcClientFactoryPBImpl.get().getClient(MRClientProtocol.class, 1, NetUtils.getConnectAddress(server), conf);
      } catch (YarnRuntimeException e) {
        e.printStackTrace();
        fail("Failed to crete client");
      }
      
    } catch (YarnRuntimeException e) {
      e.printStackTrace();
      fail("Failed to crete server");
    } finally {
      server.stop();
    }     
  }

  
  public class MRClientProtocolTestImpl implements MRClientProtocol {

    @Override
    public InetSocketAddress getConnectAddress() {
      return null;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws IOException {
      return null;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
        GetTaskAttemptCompletionEventsRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
        throws IOException {
      return null;
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request)
        throws IOException {
      return null;
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
        throws IOException {
      return null;
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws IOException {
      return null;
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws IOException {
      return null;
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws IOException {
      return null;
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
        RenewDelegationTokenRequest request) throws IOException {
      return null;
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
        CancelDelegationTokenRequest request) throws IOException {
      return null;
    }   
  }
}
