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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusConfigChangeProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;

/**
 * This is a server side utility class that handles
 * common logic to to parameter reconfiguration.
 */
public final class ReconfigurationProtocolServerSideUtils {
  private ReconfigurationProtocolServerSideUtils() {
  }

  public static ListReconfigurablePropertiesResponseProto
      listReconfigurableProperties(
          List<String> reconfigurableProperties) {
    ListReconfigurablePropertiesResponseProto.Builder builder =
        ListReconfigurablePropertiesResponseProto.newBuilder();
    builder.addAllName(reconfigurableProperties);
    return builder.build();
  }

  public static GetReconfigurationStatusResponseProto getReconfigurationStatus(
      ReconfigurationTaskStatus status) {
    GetReconfigurationStatusResponseProto.Builder builder =
        GetReconfigurationStatusResponseProto.newBuilder();

    builder.setStartTime(status.getStartTime());
    if (status.stopped()) {
      builder.setEndTime(status.getEndTime());
      assert status.getStatus() != null;
      for (Map.Entry<PropertyChange, Optional<String>> result : status
          .getStatus().entrySet()) {
        GetReconfigurationStatusConfigChangeProto.Builder changeBuilder =
            GetReconfigurationStatusConfigChangeProto.newBuilder();
        PropertyChange change = result.getKey();
        changeBuilder.setName(change.prop);
        changeBuilder.setOldValue(change.oldVal != null ? change.oldVal : "");
        if (change.newVal != null) {
          changeBuilder.setNewValue(change.newVal);
        }
        if (result.getValue().isPresent()) {
          // Get full stack trace.
          changeBuilder.setErrorMessage(result.getValue().get());
        }
        builder.addChanges(changeBuilder);
      }
    }
    return builder.build();
  }
}