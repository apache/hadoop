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
package org.apache.hadoop.yarn.server.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewAggregatorsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewAggregatorsInfoResponseProto;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewAggregatorsInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewAggregatorsInfoResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class AggregatorNodemanagerProtocolPBServiceImpl implements
    AggregatorNodemanagerProtocolPB {

  private AggregatorNodemanagerProtocol real;
  
  public AggregatorNodemanagerProtocolPBServiceImpl(AggregatorNodemanagerProtocol impl) {
    this.real = impl;
  }

  @Override
  public ReportNewAggregatorsInfoResponseProto reportNewAggregatorInfo(
      RpcController arg0, ReportNewAggregatorsInfoRequestProto proto) 
      throws ServiceException {
    ReportNewAggregatorsInfoRequestPBImpl request = 
        new ReportNewAggregatorsInfoRequestPBImpl(proto);
    try {
      ReportNewAggregatorsInfoResponse response = real.reportNewAggregatorInfo(request);
      return ((ReportNewAggregatorsInfoResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

}
