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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHRegionInfo extends HBaseTestCase {
  public void testCreateHRegionInfoName() throws Exception {
    String tableName = "tablename";
    final byte [] tn = Bytes.toBytes(tableName);
    String startKey = "startkey";
    final byte [] sk = Bytes.toBytes(startKey);
    String id = "id";
    byte [] name = HRegionInfo.createRegionName(tn, sk, id);
    String nameStr = Bytes.toString(name);
    assertEquals(nameStr, tableName + "," + startKey + "," + id);
  }
}
