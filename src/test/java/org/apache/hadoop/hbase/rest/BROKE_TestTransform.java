/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.util.Bytes;

public class BROKE_TestTransform extends HBaseRESTClusterTestBase {
  static final String TABLE = "TestTransform";
  static final String CFA = "a";
  static final String CFB = "b";
  static final String COLUMN_1 = CFA + ":1";
  static final String COLUMN_2 = CFB + ":2";
  static final String ROW_1 = "testrow1";
  static final byte[] VALUE_1 = Bytes.toBytes("testvalue1");
  static final byte[] VALUE_2 = Bytes.toBytes("testvalue2");
  static final byte[] VALUE_2_BASE64 = Bytes.toBytes("dGVzdHZhbHVlMg==");

  Client client;
  HBaseAdmin admin;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    client = new Client(new Cluster().add("localhost", testServletPort));
    admin = new HBaseAdmin(conf);
    if (admin.tableExists(TABLE)) {
      return;
    }
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(new HColumnDescriptor(CFA));
    HColumnDescriptor cfB = new HColumnDescriptor(CFB);
    cfB.setValue("Transform$1", "*:Base64");
    htd.addFamily(cfB);
    admin.createTable(htd);
  }

  @Override
  protected void tearDown() throws Exception {
    client.shutdown();
    super.tearDown();
  }

  public void testTransform() throws Exception {
    String path1 = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_1;
    String path2 = "/" + TABLE + "/" + ROW_1 + "/" + COLUMN_2;

    // store value 1
    Response response = client.put(path1, MIMETYPE_BINARY, VALUE_1);
    assertEquals(response.getCode(), 200);

    // store value 2 (stargate should transform into base64)
    response = client.put(path2, MIMETYPE_BINARY, VALUE_2);
    assertEquals(response.getCode(), 200);

    // get the table contents directly
    HTable table = new HTable(TABLE);
    Get get = new Get(Bytes.toBytes(ROW_1));
    get.addFamily(Bytes.toBytes(CFA));
    get.addFamily(Bytes.toBytes(CFB));
    Result result = table.get(get);
    // value 1 should not be transformed
    byte[] value = result.getValue(Bytes.toBytes(CFA), Bytes.toBytes("1"));
    assertNotNull(value);
    assertTrue(Bytes.equals(value, VALUE_1));
    // value 2 should have been base64 encoded
    value = result.getValue(Bytes.toBytes(CFB), Bytes.toBytes("2"));
    assertNotNull(value);
    assertTrue(Bytes.equals(value, VALUE_2_BASE64));
    table.close();

    // stargate should decode the transformed value back to original bytes
    response = client.get(path2, MIMETYPE_BINARY);
    assertEquals(response.getCode(), 200);
    value = response.getBody();
    assertTrue(Bytes.equals(value, VALUE_2));
  }
}
