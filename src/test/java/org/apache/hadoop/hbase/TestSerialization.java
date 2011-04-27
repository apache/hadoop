/**
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


import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.DataInputBuffer;
import org.junit.Test;

/**
 * Test HBase Writables serializations
 */
public class TestSerialization {

  @Test public void testCompareFilter() throws Exception {
    Filter f = new RowFilter(CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes("testRowOne-2")));
    byte [] bytes = Writables.getBytes(f);
    Filter ff = (Filter)Writables.getWritable(bytes, new RowFilter());
    assertNotNull(ff);
  }

  @Test public void testKeyValue() throws Exception {
    final String name = "testKeyValue";
    byte [] row = Bytes.toBytes(name);
    byte [] family = Bytes.toBytes(name);
    byte [] qualifier = Bytes.toBytes(name);
    KeyValue original = new KeyValue(row, family, qualifier);
    byte [] bytes = Writables.getBytes(original);
    KeyValue newone = (KeyValue)Writables.getWritable(bytes, new KeyValue());
    assertTrue(KeyValue.COMPARATOR.compare(original, newone) == 0);
  }

  @SuppressWarnings("unchecked")
  @Test public void testHbaseMapWritable() throws Exception {
    HbaseMapWritable<byte [], byte []> hmw =
      new HbaseMapWritable<byte[], byte[]>();
    hmw.put("key".getBytes(), "value".getBytes());
    byte [] bytes = Writables.getBytes(hmw);
    hmw = (HbaseMapWritable<byte[], byte[]>)
      Writables.getWritable(bytes, new HbaseMapWritable<byte [], byte []>());
    assertTrue(hmw.size() == 1);
    assertTrue(Bytes.equals("value".getBytes(), hmw.get("key".getBytes())));
  }

  @Test public void testTableDescriptor() throws Exception {
    final String name = "testTableDescriptor";
    HTableDescriptor htd = createTableDescriptor(name);
    byte [] mb = Writables.getBytes(htd);
    HTableDescriptor deserializedHtd =
      (HTableDescriptor)Writables.getWritable(mb, new HTableDescriptor());
    assertEquals(htd.getNameAsString(), deserializedHtd.getNameAsString());
  }

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  @Test public void testRegionInfo() throws Exception {
    HRegionInfo hri = createRandomRegion("testRegionInfo");
    byte [] hrib = Writables.getBytes(hri);
    HRegionInfo deserializedHri =
      (HRegionInfo)Writables.getWritable(hrib, new HRegionInfo());
    assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
    assertEquals(hri.getTableDesc().getFamilies().size(),
      deserializedHri.getTableDesc().getFamilies().size());
  }

  @Test public void testRegionInfos() throws Exception {
    HRegionInfo hri = createRandomRegion("testRegionInfos");
    byte [] hrib = Writables.getBytes(hri);
    byte [] triple = new byte [3 * hrib.length];
    System.arraycopy(hrib, 0, triple, 0, hrib.length);
    System.arraycopy(hrib, 0, triple, hrib.length, hrib.length);
    System.arraycopy(hrib, 0, triple, hrib.length * 2, hrib.length);
    List<HRegionInfo> regions = Writables.getHRegionInfos(triple, 0, triple.length);
    assertTrue(regions.size() == 3);
    assertTrue(regions.get(0).equals(regions.get(1)));
    assertTrue(regions.get(0).equals(regions.get(2)));
  }

  private HRegionInfo createRandomRegion(final String name) {
    HTableDescriptor htd = new HTableDescriptor(name);
    String [] families = new String [] {"info", "anchor"};
    for (int i = 0; i < families.length; i++) {
      htd.addFamily(new HColumnDescriptor(families[i]));
    }
    return new HRegionInfo(htd, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW);
  }

  @Test public void testPut() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    byte[] qf2 = "qf2".getBytes();
    byte[] qf3 = "qf3".getBytes();
    byte[] qf4 = "qf4".getBytes();
    byte[] qf5 = "qf5".getBytes();
    byte[] qf6 = "qf6".getBytes();
    byte[] qf7 = "qf7".getBytes();
    byte[] qf8 = "qf8".getBytes();

    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();

    Put put = new Put(row);
    put.add(fam, qf1, ts, val);
    put.add(fam, qf2, ts, val);
    put.add(fam, qf3, ts, val);
    put.add(fam, qf4, ts, val);
    put.add(fam, qf5, ts, val);
    put.add(fam, qf6, ts, val);
    put.add(fam, qf7, ts, val);
    put.add(fam, qf8, ts, val);

    byte[] sb = Writables.getBytes(put);
    Put desPut = (Put)Writables.getWritable(sb, new Put());

    //Timing test
//    long start = System.nanoTime();
//    desPut = (Put)Writables.getWritable(sb, new Put());
//    long stop = System.nanoTime();
//    System.out.println("timer " +(stop-start));

    assertTrue(Bytes.equals(put.getRow(), desPut.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()){
      assertTrue(desPut.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desPut.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }


  @Test public void testPut2() throws Exception{
    byte[] row = "testAbort,,1243116656250".getBytes();
    byte[] fam = "historian".getBytes();
    byte[] qf1 = "creation".getBytes();

    long ts = 9223372036854775807L;
    byte[] val = "dont-care".getBytes();

    Put put = new Put(row);
    put.add(fam, qf1, ts, val);

    byte[] sb = Writables.getBytes(put);
    Put desPut = (Put)Writables.getWritable(sb, new Put());

    assertTrue(Bytes.equals(put.getRow(), desPut.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()){
      assertTrue(desPut.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desPut.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }


  @Test public void testDelete() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();

    long ts = System.currentTimeMillis();

    Delete delete = new Delete(row);
    delete.deleteColumn(fam, qf1, ts);

    byte[] sb = Writables.getBytes(delete);
    Delete desDelete = (Delete)Writables.getWritable(sb, new Delete());

    assertTrue(Bytes.equals(delete.getRow(), desDelete.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry :
        delete.getFamilyMap().entrySet()){
      assertTrue(desDelete.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desDelete.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }

  @Test public void testGet() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();

    long ts = System.currentTimeMillis();
    int maxVersions = 2;
    long lockid = 5;
    RowLock rowLock = new RowLock(lockid);

    Get get = new Get(row, rowLock);
    get.addColumn(fam, qf1);
    get.setTimeRange(ts, ts+1);
    get.setMaxVersions(maxVersions);

    byte[] sb = Writables.getBytes(get);
    Get desGet = (Get)Writables.getWritable(sb, new Get());

    assertTrue(Bytes.equals(get.getRow(), desGet.getRow()));
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for(Map.Entry<byte[], NavigableSet<byte[]>> entry :
        get.getFamilyMap().entrySet()){
      assertTrue(desGet.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desGet.getFamilyMap().get(entry.getKey());
      for(byte [] qualifier : set){
        assertTrue(desSet.contains(qualifier));
      }
    }

    assertEquals(get.getLockId(), desGet.getLockId());
    assertEquals(get.getMaxVersions(), desGet.getMaxVersions());
    TimeRange tr = get.getTimeRange();
    TimeRange desTr = desGet.getTimeRange();
    assertEquals(tr.getMax(), desTr.getMax());
    assertEquals(tr.getMin(), desTr.getMin());
  }


  @Test public void testScan() throws Exception {

    byte[] startRow = "startRow".getBytes();
    byte[] stopRow  = "stopRow".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();

    long ts = System.currentTimeMillis();
    int maxVersions = 2;

    Scan scan = new Scan(startRow, stopRow);
    scan.addColumn(fam, qf1);
    scan.setTimeRange(ts, ts+1);
    scan.setMaxVersions(maxVersions);

    byte[] sb = Writables.getBytes(scan);
    Scan desScan = (Scan)Writables.getWritable(sb, new Scan());

    assertTrue(Bytes.equals(scan.getStartRow(), desScan.getStartRow()));
    assertTrue(Bytes.equals(scan.getStopRow(), desScan.getStopRow()));
    assertEquals(scan.getCacheBlocks(), desScan.getCacheBlocks());
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;

    for(Map.Entry<byte[], NavigableSet<byte[]>> entry :
        scan.getFamilyMap().entrySet()){
      assertTrue(desScan.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desScan.getFamilyMap().get(entry.getKey());
      for(byte[] column : set){
        assertTrue(desSet.contains(column));
      }

      // Test filters are serialized properly.
      scan = new Scan(startRow);
      final String name = "testScan";
      byte [] prefix = Bytes.toBytes(name);
      scan.setFilter(new PrefixFilter(prefix));
      sb = Writables.getBytes(scan);
      desScan = (Scan)Writables.getWritable(sb, new Scan());
      Filter f = desScan.getFilter();
      assertTrue(f instanceof PrefixFilter);
    }

    assertEquals(scan.getMaxVersions(), desScan.getMaxVersions());
    TimeRange tr = scan.getTimeRange();
    TimeRange desTr = desScan.getTimeRange();
    assertEquals(tr.getMax(), desTr.getMax());
    assertEquals(tr.getMin(), desTr.getMin());
  }

  @Test public void testResultEmpty() throws Exception {
    List<KeyValue> keys = new ArrayList<KeyValue>();
    Result r = new Result(keys);
    assertTrue(r.isEmpty());
    byte [] rb = Writables.getBytes(r);
    Result deserializedR = (Result)Writables.getWritable(rb, new Result());
    assertTrue(deserializedR.isEmpty());
  }


  @Test public void testResult() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);

    Result result = new Result(new KeyValue[]{kvA, kvB});

    byte [] rb = Writables.getBytes(result);
    Result deResult = (Result)Writables.getWritable(rb, new Result());

    assertTrue("results are not equivalent, first key mismatch",
        result.sorted()[0].equals(deResult.sorted()[0]));

    assertTrue("results are not equivalent, second key mismatch",
        result.sorted()[1].equals(deResult.sorted()[1]));

    // Test empty Result
    Result r = new Result();
    byte [] b = Writables.getBytes(r);
    Result deserialized = (Result)Writables.getWritable(b, new Result());
    assertEquals(r.size(), deserialized.size());
  }

  @Test public void testResultDynamicBuild() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);

    Result result = new Result(new KeyValue[]{kvA, kvB});

    byte [] rb = Writables.getBytes(result);


    // Call getRow() first
    Result deResult = (Result)Writables.getWritable(rb, new Result());
    byte [] row = deResult.getRow();
    assertTrue(Bytes.equals(row, rowA));

    // Call sorted() first
    deResult = (Result)Writables.getWritable(rb, new Result());
    assertTrue("results are not equivalent, first key mismatch",
        result.sorted()[0].equals(deResult.sorted()[0]));
    assertTrue("results are not equivalent, second key mismatch",
        result.sorted()[1].equals(deResult.sorted()[1]));

    // Call raw() first
    deResult = (Result)Writables.getWritable(rb, new Result());
    assertTrue("results are not equivalent, first key mismatch",
        result.raw()[0].equals(deResult.raw()[0]));
    assertTrue("results are not equivalent, second key mismatch",
        result.raw()[1].equals(deResult.raw()[1]));


  }

  @Test public void testResultArray() throws Exception {
    byte [] rowA = Bytes.toBytes("rowA");
    byte [] famA = Bytes.toBytes("famA");
    byte [] qfA = Bytes.toBytes("qfA");
    byte [] valueA = Bytes.toBytes("valueA");

    byte [] rowB = Bytes.toBytes("rowB");
    byte [] famB = Bytes.toBytes("famB");
    byte [] qfB = Bytes.toBytes("qfB");
    byte [] valueB = Bytes.toBytes("valueB");

    KeyValue kvA = new KeyValue(rowA, famA, qfA, valueA);
    KeyValue kvB = new KeyValue(rowB, famB, qfB, valueB);


    Result result1 = new Result(new KeyValue[]{kvA, kvB});
    Result result2 = new Result(new KeyValue[]{kvB});
    Result result3 = new Result(new KeyValue[]{kvB});

    Result [] results = new Result [] {result1, result2, result3};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    byte [] rb = byteStream.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    Result [] deResults = Result.readArray(in);

    assertTrue(results.length == deResults.length);

    for(int i=0;i<results.length;i++) {
      KeyValue [] keysA = results[i].sorted();
      KeyValue [] keysB = deResults[i].sorted();
      assertTrue(keysA.length == keysB.length);
      for(int j=0;j<keysA.length;j++) {
        assertTrue("Expected equivalent keys but found:\n" +
            "KeyA : " + keysA[j].toString() + "\n" +
            "KeyB : " + keysB[j].toString() + "\n" +
            keysA.length + " total keys, " + i + "th so far"
            ,keysA[j].equals(keysB[j]));
      }
    }

  }

  @Test public void testResultArrayEmpty() throws Exception {
    List<KeyValue> keys = new ArrayList<KeyValue>();
    Result r = new Result(keys);
    Result [] results = new Result [] {r};

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);

    Result.writeArray(out, results);

    results = null;

    byteStream = new ByteArrayOutputStream();
    out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    byte [] rb = byteStream.toByteArray();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    Result [] deResults = Result.readArray(in);

    assertTrue(deResults.length == 0);

    results = new Result[0];

    byteStream = new ByteArrayOutputStream();
    out = new DataOutputStream(byteStream);
    Result.writeArray(out, results);

    rb = byteStream.toByteArray();

    in = new DataInputBuffer();
    in.reset(rb, 0, rb.length);

    deResults = Result.readArray(in);

    assertTrue(deResults.length == 0);

  }

  @Test public void testTimeRange() throws Exception{
    TimeRange tr = new TimeRange(0,5);
    byte [] mb = Writables.getBytes(tr);
    TimeRange deserializedTr =
      (TimeRange)Writables.getWritable(mb, new TimeRange());

    assertEquals(tr.getMax(), deserializedTr.getMax());
    assertEquals(tr.getMin(), deserializedTr.getMin());

  }

  @Test public void testKeyValue2() throws Exception {
    final String name = "testKeyValue2";
    byte[] row = name.getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf = "qf".getBytes();
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();

    KeyValue kv = new KeyValue(row, fam, qf, ts, val);

    byte [] mb = Writables.getBytes(kv);
    KeyValue deserializedKv =
      (KeyValue)Writables.getWritable(mb, new KeyValue());
    assertTrue(Bytes.equals(kv.getBuffer(), deserializedKv.getBuffer()));
    assertEquals(kv.getOffset(), deserializedKv.getOffset());
    assertEquals(kv.getLength(), deserializedKv.getLength());
  }

  protected static final int MAXVERSIONS = 3;
  protected final static byte [] fam1 = Bytes.toBytes("colfamily1");
  protected final static byte [] fam2 = Bytes.toBytes("colfamily2");
  protected final static byte [] fam3 = Bytes.toBytes("colfamily3");
  protected static final byte [][] COLUMNS = {fam1, fam2, fam3};

  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name, MAXVERSIONS);
  }

  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  protected HTableDescriptor createTableDescriptor(final String name,
      final int versions) {
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(new HColumnDescriptor(fam1, versions,
      HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
      HColumnDescriptor.DEFAULT_BLOCKSIZE, HConstants.FOREVER,
      HColumnDescriptor.DEFAULT_BLOOMFILTER,
      HConstants.REPLICATION_SCOPE_LOCAL));
    htd.addFamily(new HColumnDescriptor(fam2, versions,
        HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
        HColumnDescriptor.DEFAULT_BLOCKSIZE, HConstants.FOREVER,
        HColumnDescriptor.DEFAULT_BLOOMFILTER,
        HConstants.REPLICATION_SCOPE_LOCAL));
    htd.addFamily(new HColumnDescriptor(fam3, versions,
        HColumnDescriptor.DEFAULT_COMPRESSION, false, false,
        HColumnDescriptor.DEFAULT_BLOCKSIZE,  HConstants.FOREVER,
        HColumnDescriptor.DEFAULT_BLOOMFILTER,
        HConstants.REPLICATION_SCOPE_LOCAL));
    return htd;
  }
}
