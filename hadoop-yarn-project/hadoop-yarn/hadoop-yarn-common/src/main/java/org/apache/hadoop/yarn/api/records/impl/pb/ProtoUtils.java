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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.AMCommandProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAccessTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerSubStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceVisibilityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueACLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestInterpreterProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationAttemptStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryPolicyProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceTypesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeUpdateTypeProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerUpdateTypeProto;
import org.apache.hadoop.yarn.server.api.ContainerType;

import com.google.protobuf.ByteString;

@Private
@Unstable
public class ProtoUtils {


  /*
   * ContainerState
   */
  private final static String CONTAINER_STATE_PREFIX = "C_";
  public static ContainerStateProto convertToProtoFormat(ContainerState e) {
    return ContainerStateProto.valueOf(CONTAINER_STATE_PREFIX + e.name());
  }
  public static ContainerState convertFromProtoFormat(ContainerStateProto e) {
    return ContainerState.valueOf(e.name().replace(CONTAINER_STATE_PREFIX, ""));
  }

  /*
   * Container SubState
   */
  private final static String CONTAINER_SUB_STATE_PREFIX = "CSS_";
  public static ContainerSubStateProto convertToProtoFormat(
      ContainerSubState e) {
    return ContainerSubStateProto.valueOf(
        CONTAINER_SUB_STATE_PREFIX + e.name());
  }
  public static ContainerSubState convertFromProtoFormat(
      ContainerSubStateProto e) {
    return ContainerSubState.valueOf(
        e.name().substring(CONTAINER_SUB_STATE_PREFIX.length()));
  }
  /*
   * NodeState
   */
  private final static String NODE_STATE_PREFIX = "NS_";
  public static NodeStateProto convertToProtoFormat(NodeState e) {
    return NodeStateProto.valueOf(NODE_STATE_PREFIX + e.name());
  }
  public static NodeState convertFromProtoFormat(NodeStateProto e) {
    return NodeState.valueOf(e.name().replace(NODE_STATE_PREFIX, ""));
  }
  
  /*
   * NodeId
   */
  public static NodeIdProto convertToProtoFormat(NodeId e) {
    return ((NodeIdPBImpl)e).getProto();
  }
  public static NodeId convertFromProtoFormat(NodeIdProto e) {
    return new NodeIdPBImpl(e);
  }

  /*
   * YarnApplicationState
   */
  public static YarnApplicationStateProto convertToProtoFormat(YarnApplicationState e) {
    return YarnApplicationStateProto.valueOf(e.name());
  }
  public static YarnApplicationState convertFromProtoFormat(YarnApplicationStateProto e) {
    return YarnApplicationState.valueOf(e.name());
  }

  /*
   * YarnApplicationAttemptState
   */
  private static String YARN_APPLICATION_ATTEMPT_STATE_PREFIX = "APP_ATTEMPT_";
  public static YarnApplicationAttemptStateProto convertToProtoFormat(
      YarnApplicationAttemptState e) {
    return YarnApplicationAttemptStateProto
        .valueOf(YARN_APPLICATION_ATTEMPT_STATE_PREFIX + e.name());
  }
  public static YarnApplicationAttemptState convertFromProtoFormat(
      YarnApplicationAttemptStateProto e) {
    return YarnApplicationAttemptState.valueOf(e.name().replace(
        YARN_APPLICATION_ATTEMPT_STATE_PREFIX, ""));
  }

  /*
   * ApplicationsRequestScope
   */
  public static YarnServiceProtos.ApplicationsRequestScopeProto
      convertToProtoFormat(ApplicationsRequestScope e) {
    return YarnServiceProtos.ApplicationsRequestScopeProto.valueOf(e.name());
  }
  public static ApplicationsRequestScope convertFromProtoFormat
      (YarnServiceProtos.ApplicationsRequestScopeProto e) {
    return ApplicationsRequestScope.valueOf(e.name());
  }

  /*
   * ApplicationResourceUsageReport
   */
  public static ApplicationResourceUsageReportProto convertToProtoFormat(ApplicationResourceUsageReport e) {
    return ((ApplicationResourceUsageReportPBImpl)e).getProto();
  }

  public static ApplicationResourceUsageReport convertFromProtoFormat(ApplicationResourceUsageReportProto e) {
    return new ApplicationResourceUsageReportPBImpl(e);
  }

  /*
   * FinalApplicationStatus
   */
  private static String FINAL_APPLICATION_STATUS_PREFIX = "APP_";
  public static FinalApplicationStatusProto convertToProtoFormat(FinalApplicationStatus e) {
    return FinalApplicationStatusProto.valueOf(FINAL_APPLICATION_STATUS_PREFIX + e.name());
  }
  public static FinalApplicationStatus convertFromProtoFormat(FinalApplicationStatusProto e) {
    return FinalApplicationStatus.valueOf(e.name().replace(FINAL_APPLICATION_STATUS_PREFIX, ""));
  }

  /*
   * LocalResourceType
   */
  public static LocalResourceTypeProto convertToProtoFormat(LocalResourceType e) {
    return LocalResourceTypeProto.valueOf(e.name());
  }
  public static LocalResourceType convertFromProtoFormat(LocalResourceTypeProto e) {
    return LocalResourceType.valueOf(e.name());
  }

  /*
   * LocalResourceVisibility
   */
  public static LocalResourceVisibilityProto convertToProtoFormat(LocalResourceVisibility e) {
    return LocalResourceVisibilityProto.valueOf(e.name());
  }
  public static LocalResourceVisibility convertFromProtoFormat(LocalResourceVisibilityProto e) {
    return LocalResourceVisibility.valueOf(e.name());
  }
  
  /*
   * AMCommand
   */
  public static AMCommandProto convertToProtoFormat(AMCommand e) {
    return AMCommandProto.valueOf(e.name());
  }
  public static AMCommand convertFromProtoFormat(AMCommandProto e) {
    return AMCommand.valueOf(e.name());
  }

  /*
   * ByteBuffer
   */
  public static ByteBuffer convertFromProtoFormat(ByteString byteString) {
    int capacity = byteString.asReadOnlyByteBuffer().rewind().remaining();
    byte[] b = new byte[capacity];
    byteString.asReadOnlyByteBuffer().get(b, 0, capacity);
    return ByteBuffer.wrap(b);
  }

  public static ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
//    return ByteString.copyFrom((ByteBuffer)byteBuffer.duplicate().rewind());
    int oldPos = byteBuffer.position();
    byteBuffer.rewind();
    ByteString bs = ByteString.copyFrom(byteBuffer);
    byteBuffer.position(oldPos);
    return bs;
  }

  /*
   * QueueState
   */
  private static String QUEUE_STATE_PREFIX = "Q_";
  public static QueueStateProto convertToProtoFormat(QueueState e) {
    return QueueStateProto.valueOf(QUEUE_STATE_PREFIX + e.name());
  }
  public static QueueState convertFromProtoFormat(QueueStateProto e) {
    return QueueState.valueOf(e.name().replace(QUEUE_STATE_PREFIX, ""));
  }

  /*
   * QueueACL
   */
  private static String QUEUE_ACL_PREFIX = "QACL_";
  public static QueueACLProto convertToProtoFormat(QueueACL e) {
    return QueueACLProto.valueOf(QUEUE_ACL_PREFIX + e.name());
  }
  public static QueueACL convertFromProtoFormat(QueueACLProto e) {
    return QueueACL.valueOf(e.name().replace(QUEUE_ACL_PREFIX, ""));
  }


  /*
   * ApplicationAccessType
   */
  private static String APP_ACCESS_TYPE_PREFIX = "APPACCESS_";

  public static ApplicationAccessTypeProto convertToProtoFormat(
      ApplicationAccessType e) {
    return ApplicationAccessTypeProto.valueOf(APP_ACCESS_TYPE_PREFIX
        + e.name());
  }

  public static ApplicationAccessType convertFromProtoFormat(
      ApplicationAccessTypeProto e) {
    return ApplicationAccessType.valueOf(e.name().replace(
        APP_ACCESS_TYPE_PREFIX, ""));
  }

  /*
   * ApplicationTimeoutType
   */
  private static String APP_TIMEOUT_TYPE_PREFIX = "APP_TIMEOUT_";

  public static ApplicationTimeoutTypeProto convertToProtoFormat(
      ApplicationTimeoutType e) {
    return ApplicationTimeoutTypeProto
        .valueOf(APP_TIMEOUT_TYPE_PREFIX + e.name());
  }

  public static ApplicationTimeoutType convertFromProtoFormat(
      ApplicationTimeoutTypeProto e) {
    return ApplicationTimeoutType
        .valueOf(e.name().replace(APP_TIMEOUT_TYPE_PREFIX, ""));
  }
  
  /*
   * Reservation Request interpreter type
   */
  public static ReservationRequestInterpreterProto convertToProtoFormat(
      ReservationRequestInterpreter e) {
    return ReservationRequestInterpreterProto.valueOf(e.name());
  }

  public static ReservationRequestInterpreter convertFromProtoFormat(
      ReservationRequestInterpreterProto e) {
    return ReservationRequestInterpreter.valueOf(e.name());
  }

  /*
   * Log Aggregation Status
   */
  private static final String LOG_AGGREGATION_STATUS_PREFIX = "LOG_";
  private static final int LOG_AGGREGATION_STATUS_PREFIX_LEN =
      LOG_AGGREGATION_STATUS_PREFIX.length();
  public static LogAggregationStatusProto convertToProtoFormat(
      LogAggregationStatus e) {
    return LogAggregationStatusProto.valueOf(LOG_AGGREGATION_STATUS_PREFIX
        + e.name());
  }

  public static LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto e) {
    return LogAggregationStatus.valueOf(e.name().substring(
        LOG_AGGREGATION_STATUS_PREFIX_LEN));
  }

  /*
   * ContainerType
   */
  public static ContainerTypeProto convertToProtoFormat(ContainerType e) {
    return ContainerTypeProto.valueOf(e.name());
  }
  public static ContainerType convertFromProtoFormat(ContainerTypeProto e) {
    return ContainerType.valueOf(e.name());
  }

  /*
  * NodeUpdateType
  */
  public static NodeUpdateTypeProto convertToProtoFormat(NodeUpdateType e) {
    return NodeUpdateTypeProto.valueOf(e.name());
  }
  public static NodeUpdateType convertFromProtoFormat(NodeUpdateTypeProto e) {
    return NodeUpdateType.valueOf(e.name());
  }

  /*
   * ExecutionType
   */
  public static ExecutionTypeProto convertToProtoFormat(ExecutionType e) {
    return ExecutionTypeProto.valueOf(e.name());
  }
  public static ExecutionType convertFromProtoFormat(ExecutionTypeProto e) {
    return ExecutionType.valueOf(e.name());
  }

  /*
   * ContainerUpdateType
   */
  public static ContainerUpdateTypeProto convertToProtoFormat(
      ContainerUpdateType e) {
    return ContainerUpdateTypeProto.valueOf(e.name());
  }
  public static ContainerUpdateType convertFromProtoFormat(
      ContainerUpdateTypeProto e) {
    return ContainerUpdateType.valueOf(e.name());
  }

  /*
   * Resource
   */
  public static synchronized ResourceProto convertToProtoFormat(Resource r) {
    return ResourcePBImpl.getProto(r);
  }

  public static Resource convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  /*
   * ContainerRetryPolicy
   */
  public static ContainerRetryPolicyProto convertToProtoFormat(
      ContainerRetryPolicy e) {
    return ContainerRetryPolicyProto.valueOf(e.name());
  }

  public static ContainerRetryPolicy convertFromProtoFormat(
      ContainerRetryPolicyProto e) {
    return ContainerRetryPolicy.valueOf(e.name());
  }

  /*
   * ExecutionTypeRequest
   */
  public static ExecutionTypeRequestProto convertToProtoFormat(
      ExecutionTypeRequest e) {
    return ((ExecutionTypeRequestPBImpl)e).getProto();
  }

  public static ExecutionTypeRequest convertFromProtoFormat(
      ExecutionTypeRequestProto e) {
    return new ExecutionTypeRequestPBImpl(e);
  }

  /*
   * Container
   */
  public static YarnProtos.ContainerProto convertToProtoFormat(
      Container t) {
    return ((ContainerPBImpl)t).getProto();
  }

  public static ContainerPBImpl convertFromProtoFormat(
      YarnProtos.ContainerProto t) {
    return new ContainerPBImpl(t);
  }

  public static ContainerStatusPBImpl convertFromProtoFormat(
      YarnProtos.ContainerStatusProto p) {
    return new ContainerStatusPBImpl(p);
  }

  /*
   * ContainerId
   */
  public static ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  public static ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  /*
   * UpdateContainerRequest
   */
  public static UpdateContainerRequestPBImpl convertFromProtoFormat(
      YarnServiceProtos.UpdateContainerRequestProto p) {
    return new UpdateContainerRequestPBImpl(p);
  }

  public static YarnServiceProtos.UpdateContainerRequestProto
      convertToProtoFormat(UpdateContainerRequest t) {
    return ((UpdateContainerRequestPBImpl) t).getProto();
  }

  /*
   * UpdateContainerError
   */
  public static UpdateContainerErrorPBImpl convertFromProtoFormat(
      YarnServiceProtos.UpdateContainerErrorProto p) {
    return new UpdateContainerErrorPBImpl(p);
  }

  public static YarnServiceProtos.UpdateContainerErrorProto
      convertToProtoFormat(UpdateContainerError t) {
    return ((UpdateContainerErrorPBImpl) t).getProto();
  }

  /*
   * ResourceTypes
   */
  public static ResourceTypesProto converToProtoFormat(ResourceTypes e) {
    return ResourceTypesProto.valueOf(e.name());
  }

  public static ResourceTypes convertFromProtoFormat(ResourceTypesProto e) {
    return ResourceTypes.valueOf(e.name());
  }

  public static Map<String, Long> convertStringLongMapProtoListToMap(
      List<YarnProtos.StringLongMapProto> pList) {
    Resource tmp = Resource.newInstance(0, 0);
    Map<String, Long> ret = new HashMap<>();
    for (ResourceInformation entry : tmp.getResources()) {
      ret.put(entry.getName(), 0L);
    }
    if (pList != null) {
      for (YarnProtos.StringLongMapProto p : pList) {
        ret.put(p.getKey(), p.getValue());
      }
    }
    return ret;
  }

  public static List<YarnProtos.StringLongMapProto> convertMapToStringLongMapProtoList(
      Map<String, Long> map) {
    List<YarnProtos.StringLongMapProto> ret = new ArrayList<>();
    for (Map.Entry<String, Long> entry : map.entrySet()) {
      YarnProtos.StringLongMapProto.Builder tmp =
          YarnProtos.StringLongMapProto.newBuilder();
      tmp.setKey(entry.getKey());
      tmp.setValue(entry.getValue());
      ret.add(tmp.build());
    }
    return ret;
  }
}


