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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Encodes and decodes {@link ApplicationId} for row keys.
 * App ID is stored in row key as 12 bytes, cluster timestamp section of app id
 * (long - 8 bytes) followed by sequence id section of app id (int - 4 bytes).
 */
public final class AppIdKeyConverter implements KeyConverter<String> {

  public AppIdKeyConverter() {
  }

  /*
   * (non-Javadoc)
   *
   * Converts/encodes a string app Id into a byte representation for (row) keys.
   * For conversion, we extract cluster timestamp and sequence id from the
   * string app id (calls ConverterUtils#toApplicationId(String) for
   * conversion) and then store it in a byte array of length 12 (8 bytes (long)
   * for cluster timestamp followed 4 bytes(int) for sequence id). Both cluster
   * timestamp and sequence id are inverted so that the most recent cluster
   * timestamp and highest sequence id appears first in the table (i.e.
   * application id appears in a descending order).
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #encode(java.lang.Object)
   */
  @Override
  public byte[] encode(String appIdStr) {
    ApplicationId appId = ApplicationId.fromString(appIdStr);
    byte[] appIdBytes = new byte[getKeySize()];
    byte[] clusterTs = Bytes.toBytes(
        LongConverter.invertLong(appId.getClusterTimestamp()));
    System.arraycopy(clusterTs, 0, appIdBytes, 0, Bytes.SIZEOF_LONG);
    byte[] seqId = Bytes.toBytes(
        HBaseTimelineStorageUtils.invertInt(appId.getId()));
    System.arraycopy(seqId, 0, appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
    return appIdBytes;
  }

  /*
   * (non-Javadoc)
   *
   * Converts/decodes a 12 byte representation of app id for (row) keys to an
   * app id in string format which can be returned back to client.
   * For decoding, 12 bytes are interpreted as 8 bytes of inverted cluster
   * timestamp(long) followed by 4 bytes of inverted sequence id(int). Calls
   * ApplicationId#toString to generate string representation of app id.
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter
   * #decode(byte[])
   */
  @Override
  public String decode(byte[] appIdBytes) {
    if (appIdBytes.length != getKeySize()) {
      throw new IllegalArgumentException("Invalid app id in byte format");
    }
    long clusterTs = LongConverter.invertLong(
        Bytes.toLong(appIdBytes, 0, Bytes.SIZEOF_LONG));
    int seqId = HBaseTimelineStorageUtils.invertInt(
        Bytes.toInt(appIdBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT));
    return HBaseTimelineStorageUtils.convertApplicationIdToString(
        ApplicationId.newInstance(clusterTs, seqId));
  }

  /**
   * Returns the size of app id after encoding.
   *
   * @return size of app id after encoding.
   */
  public static int getKeySize() {
    return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
  }
}
