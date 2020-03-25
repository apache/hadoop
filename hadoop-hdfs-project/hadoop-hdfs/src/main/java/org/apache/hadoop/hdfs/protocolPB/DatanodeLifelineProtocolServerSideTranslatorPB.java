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

package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeLifelineProtocolProtos.LifelineResponseProto;
import org.apache.hadoop.hdfs.server.protocol.DatanodeLifelineProtocol;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link DatanodeLifelineProtocolPB} to the
 * {@link DatanodeLifelineProtocol} server implementation.
 */
@InterfaceAudience.Private
public class DatanodeLifelineProtocolServerSideTranslatorPB implements
    DatanodeLifelineProtocolPB {

  private static final LifelineResponseProto VOID_LIFELINE_RESPONSE_PROTO =
      LifelineResponseProto.newBuilder().build();

  private final DatanodeLifelineProtocol impl;

  public DatanodeLifelineProtocolServerSideTranslatorPB(
      DatanodeLifelineProtocol impl) {
    this.impl = impl;
  }

  @Override
  public LifelineResponseProto sendLifeline(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    try {
      final StorageReport[] report = PBHelperClient.convertStorageReports(
          request.getReportsList());
      VolumeFailureSummary volumeFailureSummary =
          request.hasVolumeFailureSummary() ?
              PBHelper.convertVolumeFailureSummary(
                  request.getVolumeFailureSummary()) : null;
      impl.sendLifeline(PBHelper.convert(request.getRegistration()), report,
          request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(), request.getXceiverCount(),
          request.getFailedVolumes(), volumeFailureSummary);
      return VOID_LIFELINE_RESPONSE_PROTO;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
