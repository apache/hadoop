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
package org.apache.hadoop.yarn.server.timelineservice.storage.apptoflow;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;

/**
 * Represents a rowkey for the app_flow table.
 */
public class AppToFlowRowKey {
  private final String clusterId;
  private final String appId;

  public AppToFlowRowKey(String clusterId, String appId) {
    this.clusterId = clusterId;
    this.appId = appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getAppId() {
    return appId;
  }

  /**
   * Constructs a row key prefix for the app_flow table as follows:
   * {@code clusterId!AppId}
   *
   * @param clusterId
   * @param appId
   * @return byte array with the row key
   */
  public static byte[] getRowKey(String clusterId, String appId) {
    return Bytes.toBytes(Separator.QUALIFIERS.joinEncoded(clusterId, appId));
  }

  /**
   * Given the raw row key as bytes, returns the row key as an object.
   */
  public static AppToFlowRowKey parseRowKey(byte[] rowKey) {
    byte[][] rowKeyComponents = Separator.QUALIFIERS.split(rowKey);

    if (rowKeyComponents.length < 2) {
      throw new IllegalArgumentException("the row key is not valid for " +
          "the app-to-flow table");
    }

    String clusterId = Bytes.toString(rowKeyComponents[0]);
    String appId = Bytes.toString(rowKeyComponents[1]);
    return new AppToFlowRowKey(clusterId, appId);
  }
}
