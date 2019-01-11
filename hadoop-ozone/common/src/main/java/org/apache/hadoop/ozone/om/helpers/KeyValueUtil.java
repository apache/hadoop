/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.KeyValue;

/**
 * Convert from/to hdds KeyValue protobuf structure.
 */
public final class KeyValueUtil {
  private KeyValueUtil() {
  }

  /**
   * Parse Key,Value map data from protobuf representation.
   */
  public static Map<String, String> getFromProtobuf(List<KeyValue> metadata) {
    return metadata.stream()
        .collect(Collectors.toMap(KeyValue::getKey,
            KeyValue::getValue));
  }

  /**
   * Encode key value map to protobuf.
   */
  public static List<KeyValue> toProtobuf(Map<String, String> keyValueMap) {
    List<KeyValue> metadataList = new LinkedList<>();
    for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
      metadataList.add(KeyValue.newBuilder().setKey(entry.getKey()).
          setValue(entry.getValue()).build());
    }
    return metadataList;
  }
}