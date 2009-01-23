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
package org.apache.hadoop.hbase;


import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Test HBase Writables serializations
 */
public class TestSerialization extends HBaseTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testHbaseMapWritable() throws Exception {
    HbaseMapWritable<byte [], byte []> hmw =
      new HbaseMapWritable<byte[], byte[]>();
    hmw.put("key".getBytes(), "value".getBytes());
    byte [] bytes = Writables.getBytes(hmw);
    hmw = (HbaseMapWritable<byte[], byte[]>)
      Writables.getWritable(bytes, new HbaseMapWritable<byte [], byte []>());
    assertTrue(hmw.size() == 1);
    assertTrue(Bytes.equals("value".getBytes(), hmw.get("key".getBytes())));
  }
  
  public void testHMsg() throws Exception {
    HMsg  m = new HMsg(HMsg.Type.MSG_REGIONSERVER_QUIESCE);
    byte [] mb = Writables.getBytes(m);
    HMsg deserializedHMsg = (HMsg)Writables.getWritable(mb, new HMsg());
    assertTrue(m.equals(deserializedHMsg));
    m = new HMsg(HMsg.Type.MSG_REGIONSERVER_QUIESCE,
      new HRegionInfo(new HTableDescriptor(getName()),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY),
        "Some message".getBytes());
    mb = Writables.getBytes(m);
    deserializedHMsg = (HMsg)Writables.getWritable(mb, new HMsg());
    assertTrue(m.equals(deserializedHMsg));
  }
  
  public void testTableDescriptor() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName());
    byte [] mb = Writables.getBytes(htd);
    HTableDescriptor deserializedHtd =
      (HTableDescriptor)Writables.getWritable(mb, new HTableDescriptor());
    assertEquals(htd.getNameAsString(), deserializedHtd.getNameAsString());
  }

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  public void testRowResult() throws Exception {
    HbaseMapWritable<byte [], Cell> m = new HbaseMapWritable<byte [], Cell>();
    byte [] b = Bytes.toBytes(getName());
    m.put(b, new Cell(b, System.currentTimeMillis()));
    RowResult rr = new RowResult(b, m);
    byte [] mb = Writables.getBytes(rr);
    RowResult deserializedRr =
      (RowResult)Writables.getWritable(mb, new RowResult());
    assertTrue(Bytes.equals(rr.getRow(), deserializedRr.getRow()));
    byte [] one = rr.get(b).getValue();
    byte [] two = deserializedRr.get(b).getValue();
    assertTrue(Bytes.equals(one, two));
    Writables.copyWritable(rr, deserializedRr);
    one = rr.get(b).getValue();
    two = deserializedRr.get(b).getValue();
    assertTrue(Bytes.equals(one, two));
    
  }

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  public void testRegionInfo() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(getName());
    String [] families = new String [] {"info:", "anchor:"};
    for (int i = 0; i < families.length; i++) {
      htd.addFamily(new HColumnDescriptor(families[i]));
    }
    HRegionInfo hri = new HRegionInfo(htd,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    byte [] hrib = Writables.getBytes(hri);
    HRegionInfo deserializedHri =
      (HRegionInfo)Writables.getWritable(hrib, new HRegionInfo());
    assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
    assertEquals(hri.getTableDesc().getFamilies().size(),
      deserializedHri.getTableDesc().getFamilies().size());
  }
  
  /**
   * Test ServerInfo serialization
   * @throws Exception
   */
  public void testServerInfo() throws Exception {
    HServerInfo hsi = new HServerInfo(new HServerAddress("0.0.0.0:123"), -1,
      1245);
    byte [] b = Writables.getBytes(hsi);
    HServerInfo deserializedHsi =
      (HServerInfo)Writables.getWritable(b, new HServerInfo());
    assertTrue(hsi.equals(deserializedHsi));
  }
  
  /**
   * Test BatchUpdate serialization
   * @throws Exception
   */
  public void testBatchUpdate() throws Exception {
    // Add row named 'testName'.
    BatchUpdate bu = new BatchUpdate(getName());
    // Add a column named same as row.
    bu.put(getName(), getName().getBytes());
    byte [] b = Writables.getBytes(bu);
    BatchUpdate bubu =
      (BatchUpdate)Writables.getWritable(b, new BatchUpdate());
    // Assert rows are same.
    assertTrue(Bytes.equals(bu.getRow(), bubu.getRow()));
    // Assert has same number of BatchOperations.
    int firstCount = 0;
    for (BatchOperation bo: bubu) {
      firstCount++;
    }
    // Now deserialize again into same instance to ensure we're not
    // accumulating BatchOperations on each deserialization.
    BatchUpdate bububu = (BatchUpdate)Writables.getWritable(b, bubu);
    // Assert rows are same again.
    assertTrue(Bytes.equals(bu.getRow(), bububu.getRow()));
    int secondCount = 0;
    for (BatchOperation bo: bububu) {
      secondCount++;
    }
    assertEquals(firstCount, secondCount);
  }
}