
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

package org.apache.hadoop.ozone.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Version response class.
 */
public class VersionResponse {
  private final int version;
  private final Map<String, String> values;

  /**
   * Creates a version response class.
   * @param version
   * @param values
   */
  public VersionResponse(int version, Map<String, String> values) {
    this.version = version;
    this.values = values;
  }

  /**
   * Creates a version Response class.
   * @param version
   */
  public VersionResponse(int version) {
    this.version = version;
    this.values = new HashMap<>();
  }

  /**
   * Returns a new Builder.
   * @return - Builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns this class from protobuf message.
   * @param response - SCMVersionResponseProto
   * @return VersionResponse
   */
  public static VersionResponse getFromProtobuf(SCMVersionResponseProto
                                                    response) {
    return new VersionResponse(response.getSoftwareVersion(),
        response.getKeysList().stream()
            .collect(Collectors.toMap(KeyValue::getKey,
                KeyValue::getValue)));
  }

  /**
   * Adds a value to version Response.
   * @param key - String
   * @param value - String
   */
  public void put(String key, String value) {
    if (this.values.containsKey(key)) {
      throw new IllegalArgumentException("Duplicate key in version response");
    }
    values.put(key, value);
  }

  /**
   * Return a protobuf message.
   * @return SCMVersionResponseProto.
   */
  public SCMVersionResponseProto getProtobufMessage() {

    List<KeyValue> list = new LinkedList<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      list.add(KeyValue.newBuilder().setKey(entry.getKey()).
          setValue(entry.getValue()).build());
    }
    return
        SCMVersionResponseProto.newBuilder()
            .setSoftwareVersion(this.version)
            .addAllKeys(list).build();
  }

  public String getValue(String key) {
    return this.values.get(key);
  }

  /**
   * Builder class.
   */
  public static class Builder {
    private int version;
    private Map<String, String> values;

    Builder() {
      values = new HashMap<>();
    }

    /**
     * Sets the version.
     * @param ver - version
     * @return Builder
     */
    public Builder setVersion(int ver) {
      this.version = ver;
      return this;
    }

    /**
     * Adds a value to version Response.
     * @param key - String
     * @param value - String
     */
    public Builder addValue(String key, String value) {
      if (this.values.containsKey(key)) {
        throw new IllegalArgumentException("Duplicate key in version response");
      }
      values.put(key, value);
      return this;
    }

    /**
     * Builds the version response.
     * @return VersionResponse.
     */
    public VersionResponse build() {
      return new VersionResponse(this.version, this.values);
    }
  }
}
