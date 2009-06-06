/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class KeyValueTestUtil {

  public static KeyValue create(
      String row,
      String family,
      String qualifier,
      long timestamp,
      String value)
  {
    return create(row, family, qualifier, timestamp, KeyValue.Type.Put, value);
  }

  public static KeyValue create(
      String row,
      String family,
      String qualifier,
      long timestamp,
      KeyValue.Type type,
      String value)
  {
      return new KeyValue(
          Bytes.toBytes(row),
          Bytes.toBytes(family),
          Bytes.toBytes(qualifier),
          timestamp,
          type,
          Bytes.toBytes(value)
      );
  }
}
