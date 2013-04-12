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

package org.apache.hadoop.yarn.util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationResourceUsageReportPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAccessTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationResourceUsageReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceVisibilityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueACLProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationStateProto;

import com.google.protobuf.ByteString;

public class ProtoUtils {


  /*
   * ContainerState
   */
  private static String CONTAINER_STATE_PREFIX = "C_";
  public static ContainerStateProto convertToProtoFormat(ContainerState e) {
    return ContainerStateProto.valueOf(CONTAINER_STATE_PREFIX + e.name());
  }
  public static ContainerState convertFromProtoFormat(ContainerStateProto e) {
    return ContainerState.valueOf(e.name().replace(CONTAINER_STATE_PREFIX, ""));
  }

  /*
   * NodeState
   */
  private static String NODE_STATE_PREFIX = "NS_";
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

  /**
   * Convert a protobuf token into a rpc token and set its service
   * 
   * @param protoToken the yarn token
   * @param serviceAddr the connect address for the service
   * @return rpc token
   */
  public static <T extends TokenIdentifier> Token<T> convertFromProtoFormat(
      org.apache.hadoop.yarn.api.records.Token protoToken,
      InetSocketAddress serviceAddr) {
    Token<T> token = new Token<T>(protoToken.getIdentifier().array(),
                                  protoToken.getPassword().array(),
                                  new Text(protoToken.getKind()),
                                  new Text(protoToken.getService()));
    if (serviceAddr != null) {
      SecurityUtil.setTokenService(token, serviceAddr);
    }
    return token;
  }
}
