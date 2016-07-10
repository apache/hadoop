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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ReportNewCollectorInfoResponseProto;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.GetTimelineCollectorContextResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReportNewCollectorInfoResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class CollectorNodemanagerProtocolPBServiceImpl implements
    CollectorNodemanagerProtocolPB {

  private CollectorNodemanagerProtocol real;

  public CollectorNodemanagerProtocolPBServiceImpl(
      CollectorNodemanagerProtocol impl) {
    this.real = impl;
  }

  @Override
  public ReportNewCollectorInfoResponseProto reportNewCollectorInfo(
      RpcController arg0, ReportNewCollectorInfoRequestProto proto)
      throws ServiceException {
    ReportNewCollectorInfoRequestPBImpl request =
        new ReportNewCollectorInfoRequestPBImpl(proto);
    try {
      ReportNewCollectorInfoResponse response =
          real.reportNewCollectorInfo(request);
      return ((ReportNewCollectorInfoResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTimelineCollectorContextResponseProto getTimelineCollectorContext(
      RpcController controller,
      GetTimelineCollectorContextRequestProto proto) throws ServiceException {
    GetTimelineCollectorContextRequestPBImpl request =
        new GetTimelineCollectorContextRequestPBImpl(proto);
    try {
      GetTimelineCollectorContextResponse response =
          real.getTimelineCollectorContext(request);
      return ((GetTimelineCollectorContextResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
