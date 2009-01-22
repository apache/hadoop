/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest.descriptors;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class TimestampsDescriptor implements ISerializable {
  Map<Long, String> timestamps = new HashMap<Long, String>();

  public void add(long timestamp, byte[] tableName, byte[] rowName) {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toString(tableName));
    sb.append("/row/");
    sb.append(Bytes.toString(rowName));
    sb.append('/');
    sb.append(timestamp);

    timestamps.put(timestamp, sb.toString());
  }

  /**
   * @return the timestamps
   */
  public Map<Long, String> getTimestamps() {
    return timestamps;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.ISerializable#restSerialize(org
   * .apache.hadoop.hbase.rest.serializer.IRestSerializer)
   */
  public void restSerialize(IRestSerializer serializer)
      throws HBaseRestException {
    serializer.serializeTimestamps(this);
  }

}
