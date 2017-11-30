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

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusConfigChangeProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;

import com.google.common.collect.Maps;

/**
 * This is a client side utility class that handles
 * common logic to to parameter reconfiguration.
 */
public final class ReconfigurationProtocolUtils {
  private ReconfigurationProtocolUtils() {
  }

  public static ReconfigurationTaskStatus getReconfigurationStatus(
      GetReconfigurationStatusResponseProto response) {
    Map<PropertyChange, Optional<String>> statusMap = null;
    long startTime;
    long endTime = 0;

    startTime = response.getStartTime();
    if (response.hasEndTime()) {
      endTime = response.getEndTime();
    }
    if (response.getChangesCount() > 0) {
      statusMap = Maps.newHashMap();
      for (GetReconfigurationStatusConfigChangeProto change : response
          .getChangesList()) {
        PropertyChange pc = new PropertyChange(change.getName(),
            change.getNewValue(), change.getOldValue());
        String errorMessage = null;
        if (change.hasErrorMessage()) {
          errorMessage = change.getErrorMessage();
        }
        statusMap.put(pc, Optional.ofNullable(errorMessage));
      }
    }
    return new ReconfigurationTaskStatus(startTime, endTime, statusMap);
  }
}
