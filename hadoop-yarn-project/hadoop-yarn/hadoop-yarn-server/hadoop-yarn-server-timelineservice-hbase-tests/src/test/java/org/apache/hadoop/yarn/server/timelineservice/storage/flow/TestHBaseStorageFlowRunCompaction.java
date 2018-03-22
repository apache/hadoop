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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.DataGeneratorForTest;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineServerUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.LongConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimestampGenerator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the FlowRun and FlowActivity Tables.
 */
public class TestHBaseStorageFlowRunCompaction {

  private static HBaseTestingUtility util;

  private static final String METRIC1 = "MAP_SLOT_MILLIS";
  private static final String METRIC2 = "HDFS_BYTES_READ";

  private final byte[] aRowKey = Bytes.toBytes("a");
  private final byte[] aFamily = Bytes.toBytes("family");
  private final byte[] aQualifier = Bytes.toBytes("qualifier");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
    DataGeneratorForTest.createSchema(util.getConfiguration());
  }

  /**
   * writes non numeric data into flow run table.
   * reads it back
   *
   * @throws Exception
   */
  @Test
  public void testWriteNonNumericData() throws Exception {
    String rowKey = "nonNumericRowKey";
    String column = "nonNumericColumnName";
    String value = "nonNumericValue";
    byte[] rowKeyBytes = Bytes.toBytes(rowKey);
    byte[] columnNameBytes = Bytes.toBytes(column);
    byte[] valueBytes = Bytes.toBytes(value);
    Put p = new Put(rowKeyBytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnNameBytes,
        valueBytes);
    Configuration hbaseConf = util.getConfiguration();
    Connection conn = null;
    conn = ConnectionFactory.createConnection(hbaseConf);
    Table flowRunTable = conn.getTable(
        BaseTableRW.getTableName(hbaseConf,
            FlowRunTableRW.TABLE_NAME_CONF_NAME,
            FlowRunTableRW.DEFAULT_TABLE_NAME));
    flowRunTable.put(p);

    Get g = new Get(rowKeyBytes);
    Result r = flowRunTable.get(g);
    assertNotNull(r);
    assertTrue(r.size() >= 1);
    Cell actualValue = r.getColumnLatestCell(
        FlowRunColumnFamily.INFO.getBytes(), columnNameBytes);
    assertNotNull(CellUtil.cloneValue(actualValue));
    assertEquals(Bytes.toString(CellUtil.cloneValue(actualValue)), value);
  }

  @Test
  public void testWriteScanBatchLimit() throws Exception {
    String rowKey = "nonNumericRowKey";
    String column = "nonNumericColumnName";
    String value = "nonNumericValue";
    String column2 = "nonNumericColumnName2";
    String value2 = "nonNumericValue2";
    String column3 = "nonNumericColumnName3";
    String value3 = "nonNumericValue3";
    String column4 = "nonNumericColumnName4";
    String value4 = "nonNumericValue4";

    byte[] rowKeyBytes = Bytes.toBytes(rowKey);
    byte[] columnNameBytes = Bytes.toBytes(column);
    byte[] valueBytes = Bytes.toBytes(value);
    byte[] columnName2Bytes = Bytes.toBytes(column2);
    byte[] value2Bytes = Bytes.toBytes(value2);
    byte[] columnName3Bytes = Bytes.toBytes(column3);
    byte[] value3Bytes = Bytes.toBytes(value3);
    byte[] columnName4Bytes = Bytes.toBytes(column4);
    byte[] value4Bytes = Bytes.toBytes(value4);

    Put p = new Put(rowKeyBytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnNameBytes,
        valueBytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName2Bytes,
        value2Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName3Bytes,
        value3Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName4Bytes,
        value4Bytes);

    Configuration hbaseConf = util.getConfiguration();
    Connection conn = null;
    conn = ConnectionFactory.createConnection(hbaseConf);
    Table flowRunTable = conn.getTable(
        BaseTableRW.getTableName(hbaseConf,
            FlowRunTableRW.TABLE_NAME_CONF_NAME,
            FlowRunTableRW.DEFAULT_TABLE_NAME));
    flowRunTable.put(p);

    String rowKey2 = "nonNumericRowKey2";
    byte[] rowKey2Bytes = Bytes.toBytes(rowKey2);
    p = new Put(rowKey2Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnNameBytes,
        valueBytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName2Bytes,
        value2Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName3Bytes,
        value3Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName4Bytes,
        value4Bytes);
    flowRunTable.put(p);

    String rowKey3 = "nonNumericRowKey3";
    byte[] rowKey3Bytes = Bytes.toBytes(rowKey3);
    p = new Put(rowKey3Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnNameBytes,
        valueBytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName2Bytes,
        value2Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName3Bytes,
        value3Bytes);
    p.addColumn(FlowRunColumnFamily.INFO.getBytes(), columnName4Bytes,
        value4Bytes);
    flowRunTable.put(p);

    Scan s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    s.setStartRow(rowKeyBytes);
    // set number of cells to fetch per scanner next invocation
    int batchLimit = 2;
    s.setBatch(batchLimit);
    ResultScanner scanner = flowRunTable.getScanner(s);
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      assertTrue(result.rawCells().length <= batchLimit);
      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowRunColumnFamily.INFO.getBytes());
      assertTrue(values.size() <= batchLimit);
    }

    s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    s.setStartRow(rowKeyBytes);
    // set number of cells to fetch per scanner next invocation
    batchLimit = 3;
    s.setBatch(batchLimit);
    scanner = flowRunTable.getScanner(s);
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      assertTrue(result.rawCells().length <= batchLimit);
      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowRunColumnFamily.INFO.getBytes());
      assertTrue(values.size() <= batchLimit);
    }

    s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    s.setStartRow(rowKeyBytes);
    // set number of cells to fetch per scanner next invocation
    batchLimit = 1000;
    s.setBatch(batchLimit);
    scanner = flowRunTable.getScanner(s);
    int rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      assertTrue(result.rawCells().length <= batchLimit);
      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowRunColumnFamily.INFO.getBytes());
      assertTrue(values.size() <= batchLimit);
      // we expect all back in one next call
      assertEquals(4, values.size());
      rowCount++;
    }
    // should get back 1 row with each invocation
    // if scan batch is set sufficiently high
    assertEquals(3, rowCount);

    // test with a negative number
    // should have same effect as setting it to a high number
    s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    s.setStartRow(rowKeyBytes);
    // set number of cells to fetch per scanner next invocation
    batchLimit = -2992;
    s.setBatch(batchLimit);
    scanner = flowRunTable.getScanner(s);
    rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      assertEquals(4, result.rawCells().length);
      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowRunColumnFamily.INFO.getBytes());
      // we expect all back in one next call
      assertEquals(4, values.size());
      rowCount++;
    }
    // should get back 1 row with each invocation
    // if scan batch is set sufficiently high
    assertEquals(3, rowCount);
  }

  @Test
  public void testWriteFlowRunCompaction() throws Exception {
    String cluster = "kompaction_cluster1";
    String user = "kompaction_FlowRun__user1";
    String flow = "kompaction_flowRun_flow_name";
    String flowVersion = "AF1021C19F1351";
    long runid = 1449526652000L;

    int start = 10;
    int count = 2000;
    int appIdSuffix = 1;
    HBaseTimelineWriterImpl hbi = null;
    long insertTs = System.currentTimeMillis() - count;
    Configuration c1 = util.getConfiguration();
    TimelineEntities te1 = null;
    TimelineEntity entityApp1 = null;
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(user);
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);

      // now insert count * ( 100 + 100) metrics
      // each call to getEntityMetricsApp1 brings back 100 values
      // of metric1 and 100 of metric2
      for (int i = start; i < start + count; i++) {
        String appName = "application_10240000000000_" + appIdSuffix;
        insertTs++;
        te1 = new TimelineEntities();
        entityApp1 = TestFlowDataGenerator.getEntityMetricsApp1(insertTs, c1);
        te1.addEntity(entityApp1);
        hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
            runid, appName), te1, remoteUser);

        appName = "application_2048000000000_7" + appIdSuffix;
        insertTs++;
        te1 = new TimelineEntities();
        entityApp1 = TestFlowDataGenerator.getEntityMetricsApp2(insertTs);
        te1.addEntity(entityApp1);
        hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
            runid, appName), te1, remoteUser);
      }
    } finally {
      String appName = "application_10240000000000_" + appIdSuffix;
      te1 = new TimelineEntities();
      entityApp1 = TestFlowDataGenerator.getEntityMetricsApp1Complete(
          insertTs + 1, c1);
      te1.addEntity(entityApp1);
      if (hbi != null) {
        hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
            runid, appName), te1, remoteUser);
        hbi.flush();
        hbi.close();
      }
    }

    // check in flow run table
    TableName flowRunTable = BaseTableRW.getTableName(c1,
        FlowRunTableRW.TABLE_NAME_CONF_NAME, FlowRunTableRW.DEFAULT_TABLE_NAME);
    HRegionServer server = util.getRSForFirstRegionInTable(flowRunTable);

    // flush and compact all the regions of the primary table
    int regionNum = HBaseTimelineServerUtils.flushCompactTableRegions(
        server, flowRunTable);
    assertTrue("Didn't find any regions for primary table!",
        regionNum > 0);

    // check flow run for one flow many apps
    checkFlowRunTable(cluster, user, flow, runid, c1, 4);
  }


  private void checkFlowRunTable(String cluster, String user, String flow,
      long runid, Configuration c1, int valueCount) throws IOException {
    Scan s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    byte[] startRow = new FlowRunRowKey(cluster, user, flow, runid).getRowKey();
    s.setStartRow(startRow);
    String clusterStop = cluster + "1";
    byte[] stopRow =
        new FlowRunRowKey(clusterStop, user, flow, runid).getRowKey();
    s.setStopRow(stopRow);
    Connection conn = ConnectionFactory.createConnection(c1);
    Table table1 = conn.getTable(
        BaseTableRW.getTableName(c1,
            FlowRunTableRW.TABLE_NAME_CONF_NAME,
            FlowRunTableRW.DEFAULT_TABLE_NAME));
    ResultScanner scanner = table1.getScanner(s);

    int rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      Map<byte[], byte[]> values = result.getFamilyMap(FlowRunColumnFamily.INFO
          .getBytes());
      assertEquals(valueCount, values.size());

      rowCount++;
      // check metric1
      byte[] q = ColumnHelper.getColumnQualifier(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(), METRIC1);
      assertTrue(values.containsKey(q));
      assertEquals(141, Bytes.toLong(values.get(q)));

      // check metric2
      q = ColumnHelper.getColumnQualifier(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(), METRIC2);
      assertTrue(values.containsKey(q));
      assertEquals(57, Bytes.toLong(values.get(q)));
    }
    assertEquals(1, rowCount);
  }


  private FlowScanner getFlowScannerForTestingCompaction() {
    // create a FlowScanner object with the sole purpose of invoking a process
    // summation;
    // okay to pass in nulls for the constructor arguments
    // because all we want to do is invoke the process summation
    FlowScanner fs = new FlowScanner(null, null,
        FlowScannerOperation.MAJOR_COMPACTION);
    assertNotNull(fs);
    return fs;
  }

  @Test
  public void checkProcessSummationMoreCellsSumFinal2()
      throws IOException {
    long cellValue1 = 1236L;
    long cellValue2 = 28L;
    long cellValue3 = 1236L;
    long cellValue4 = 1236L;
    FlowScanner fs = getFlowScannerForTestingCompaction();

    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();
    long cell1Ts = 1200120L;
    long cell2Ts = TimestampGenerator.getSupplementedTimestamp(
        System.currentTimeMillis(), "application_123746661110_11202");
    long cell3Ts = 1277719L;
    long cell4Ts = currentTimestamp - 10;

    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);

    List<Tag> tags = new ArrayList<>();
    Tag t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM_FINAL.getTagType(),
        "application_1234588888_91188");
    tags.add(t);
    byte[] tagByteArray =
        HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    // create a cell with a VERY old timestamp and attribute SUM_FINAL
    Cell c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, cell1Ts, Bytes.toBytes(cellValue1), tagByteArray);
    currentColumnCells.add(c1);

    tags = new ArrayList<>();
    t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM_FINAL.getTagType(),
        "application_12700000001_29102");
    tags.add(t);
    tagByteArray = HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    // create a cell with a recent timestamp and attribute SUM_FINAL
    Cell c2 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, cell2Ts, Bytes.toBytes(cellValue2), tagByteArray);
    currentColumnCells.add(c2);

    tags = new ArrayList<>();
    t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM.getTagType(),
        "application_191780000000001_8195");
    tags.add(t);
    tagByteArray = HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    // create a cell with a VERY old timestamp but has attribute SUM
    Cell c3 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, cell3Ts, Bytes.toBytes(cellValue3), tagByteArray);
    currentColumnCells.add(c3);

    tags = new ArrayList<>();
    t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM.getTagType(),
        "application_191780000000001_98104");
    tags.add(t);
    tagByteArray = HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    // create a cell with a VERY old timestamp but has attribute SUM
    Cell c4 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, cell4Ts, Bytes.toBytes(cellValue4), tagByteArray);
    currentColumnCells.add(c4);

    List<Cell> cells =
        fs.processSummationMajorCompaction(currentColumnCells,
            new LongConverter(), currentTimestamp);
    assertNotNull(cells);

    // we should be getting back 4 cells
    // one is the flow sum cell
    // two are the cells with SUM attribute
    // one cell with SUM_FINAL
    assertEquals(4, cells.size());

    for (int i = 0; i < cells.size(); i++) {
      Cell returnedCell = cells.get(0);
      assertNotNull(returnedCell);

      long returnTs = returnedCell.getTimestamp();
      long returnValue = Bytes.toLong(CellUtil
          .cloneValue(returnedCell));
      if (returnValue == cellValue2) {
        assertTrue(returnTs == cell2Ts);
      } else if (returnValue == cellValue3) {
        assertTrue(returnTs == cell3Ts);
      } else if (returnValue == cellValue4) {
        assertTrue(returnTs == cell4Ts);
      } else if (returnValue == cellValue1) {
        assertTrue(returnTs != cell1Ts);
        assertTrue(returnTs > cell1Ts);
        assertTrue(returnTs >= currentTimestamp);
      } else {
        // raise a failure since we expect only these two values back
        Assert.fail();
      }
    }
  }

  // tests with many cells
  // of type SUM and SUM_FINAL
  // all cells of SUM_FINAL will expire
  @Test
  public void checkProcessSummationMoreCellsSumFinalMany() throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();
    int count = 200000;

    long cellValueFinal = 1000L;
    long cellValueNotFinal = 28L;

    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();
    long cellTsFinalStart = 10001120L;
    long cellTsFinal = cellTsFinalStart;
    long cellTsNotFinalStart = currentTimestamp - 5;
    long cellTsNotFinal = cellTsNotFinalStart;

    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);
    List<Tag> tags = null;
    Tag t = null;
    Cell c1 = null;

    // insert SUM_FINAL cells
    for (int i = 0; i < count; i++) {
      tags = new ArrayList<>();
      t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM_FINAL.getTagType(),
          "application_123450000" + i + "01_19" + i);
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      // create a cell with a VERY old timestamp and attribute SUM_FINAL
      c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily, aQualifier,
          cellTsFinal, Bytes.toBytes(cellValueFinal), tagByteArray);
      currentColumnCells.add(c1);
      cellTsFinal++;
    }

    // add SUM cells
    for (int i = 0; i < count; i++) {
      tags = new ArrayList<>();
      t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM.getTagType(),
          "application_1987650000" + i + "83_911" + i);
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      // create a cell with attribute SUM
      c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily, aQualifier,
          cellTsNotFinal, Bytes.toBytes(cellValueNotFinal), tagByteArray);
      currentColumnCells.add(c1);
      cellTsNotFinal++;
    }

    List<Cell> cells =
        fs.processSummationMajorCompaction(currentColumnCells,
            new LongConverter(), currentTimestamp);
    assertNotNull(cells);

    // we should be getting back count + 1 cells
    // one is the flow sum cell
    // others are the cells with SUM attribute
    assertEquals(count + 1, cells.size());

    for (int i = 0; i < cells.size(); i++) {
      Cell returnedCell = cells.get(0);
      assertNotNull(returnedCell);

      long returnTs = returnedCell.getTimestamp();
      long returnValue = Bytes.toLong(CellUtil
          .cloneValue(returnedCell));
      if (returnValue == (count * cellValueFinal)) {
        assertTrue(returnTs > (cellTsFinalStart + count));
        assertTrue(returnTs >= currentTimestamp);
      } else if ((returnValue >= cellValueNotFinal)
          && (returnValue <= cellValueNotFinal * count)) {
        assertTrue(returnTs >= cellTsNotFinalStart);
        assertTrue(returnTs <= cellTsNotFinalStart * count);
      } else {
        // raise a failure since we expect only these values back
        Assert.fail();
      }
    }
  }

  // tests with many cells
  // of type SUM and SUM_FINAL
  // NOT cells of SUM_FINAL will expire
  @Test
  public void checkProcessSummationMoreCellsSumFinalVariedTags()
      throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();
    int countFinal = 20100;
    int countNotFinal = 1000;
    int countFinalNotExpire = 7009;

    long cellValueFinal = 1000L;
    long cellValueNotFinal = 28L;

    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();
    long cellTsFinalStart = 10001120L;
    long cellTsFinal = cellTsFinalStart;

    long cellTsFinalStartNotExpire = TimestampGenerator
        .getSupplementedTimestamp(System.currentTimeMillis(),
            "application_10266666661166_118821");
    long cellTsFinalNotExpire = cellTsFinalStartNotExpire;

    long cellTsNotFinalStart = currentTimestamp - 5;
    long cellTsNotFinal = cellTsNotFinalStart;

    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);
    List<Tag> tags = null;
    Tag t = null;
    Cell c1 = null;

    // insert SUM_FINAL cells which will expire
    for (int i = 0; i < countFinal; i++) {
      tags = new ArrayList<>();
      t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM_FINAL.getTagType(),
          "application_123450000" + i + "01_19" + i);
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      // create a cell with a VERY old timestamp and attribute SUM_FINAL
      c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily, aQualifier,
          cellTsFinal, Bytes.toBytes(cellValueFinal), tagByteArray);
      currentColumnCells.add(c1);
      cellTsFinal++;
    }

    // insert SUM_FINAL cells which will NOT expire
    for (int i = 0; i < countFinalNotExpire; i++) {
      tags = new ArrayList<>();
      t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM_FINAL.getTagType(),
          "application_123450000" + i + "01_19" + i);
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      // create a cell with a VERY old timestamp and attribute SUM_FINAL
      c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily, aQualifier,
          cellTsFinalNotExpire, Bytes.toBytes(cellValueFinal), tagByteArray);
      currentColumnCells.add(c1);
      cellTsFinalNotExpire++;
    }

    // add SUM cells
    for (int i = 0; i < countNotFinal; i++) {
      tags = new ArrayList<>();
      t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM.getTagType(),
          "application_1987650000" + i + "83_911" + i);
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      // create a cell with attribute SUM
      c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily, aQualifier,
          cellTsNotFinal, Bytes.toBytes(cellValueNotFinal), tagByteArray);
      currentColumnCells.add(c1);
      cellTsNotFinal++;
    }

    List<Cell> cells =
        fs.processSummationMajorCompaction(currentColumnCells,
            new LongConverter(), currentTimestamp);
    assertNotNull(cells);

    // we should be getting back
    // countNotFinal + countFinalNotExpire + 1 cells
    // one is the flow sum cell
    // count = the cells with SUM attribute
    // count = the cells with SUM_FINAL attribute but not expired
    assertEquals(countFinalNotExpire + countNotFinal + 1, cells.size());

    for (int i = 0; i < cells.size(); i++) {
      Cell returnedCell = cells.get(0);
      assertNotNull(returnedCell);

      long returnTs = returnedCell.getTimestamp();
      long returnValue = Bytes.toLong(CellUtil
          .cloneValue(returnedCell));
      if (returnValue == (countFinal * cellValueFinal)) {
        assertTrue(returnTs > (cellTsFinalStart + countFinal));
        assertTrue(returnTs >= currentTimestamp);
      } else if (returnValue == cellValueNotFinal) {
        assertTrue(returnTs >= cellTsNotFinalStart);
        assertTrue(returnTs <= cellTsNotFinalStart + countNotFinal);
      } else if (returnValue == cellValueFinal){
        assertTrue(returnTs >= cellTsFinalStartNotExpire);
        assertTrue(returnTs <= cellTsFinalStartNotExpire + countFinalNotExpire);
      } else {
        // raise a failure since we expect only these values back
        Assert.fail();
      }
    }
  }

  @Test
  public void testProcessSummationMoreCellsSumFinal() throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();
    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();
    long cellValue1 = 1236L;
    long cellValue2 = 28L;

    List<Tag> tags = new ArrayList<>();
    Tag t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM_FINAL.getTagType(),
        "application_1234588888_999888");
    tags.add(t);
    byte[] tagByteArray =
        HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);

    // create a cell with a VERY old timestamp and attribute SUM_FINAL
    Cell c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, 120L, Bytes.toBytes(cellValue1), tagByteArray);
    currentColumnCells.add(c1);

    tags = new ArrayList<>();
    t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM.getTagType(),
        "application_100000000001_119101");
    tags.add(t);
    tagByteArray = HBaseTimelineServerUtils.convertTagListToByteArray(tags);

    // create a cell with a VERY old timestamp but has attribute SUM
    Cell c2 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, 130L, Bytes.toBytes(cellValue2), tagByteArray);
    currentColumnCells.add(c2);
    List<Cell> cells = fs.processSummationMajorCompaction(currentColumnCells,
        new LongConverter(), currentTimestamp);
    assertNotNull(cells);

    // we should be getting back two cells
    // one is the flow sum cell
    // another is the cell with SUM attribute
    assertEquals(2, cells.size());

    Cell returnedCell = cells.get(0);
    assertNotNull(returnedCell);
    long inputTs1 = c1.getTimestamp();
    long inputTs2 = c2.getTimestamp();

    long returnTs = returnedCell.getTimestamp();
    long returnValue = Bytes.toLong(CellUtil
        .cloneValue(returnedCell));
    // the returned Ts will be far greater than input ts as well as the noted
    // current timestamp
    if (returnValue == cellValue2) {
      assertTrue(returnTs == inputTs2);
    } else if (returnValue == cellValue1) {
      assertTrue(returnTs >= currentTimestamp);
      assertTrue(returnTs != inputTs1);
    } else {
      // raise a failure since we expect only these two values back
      Assert.fail();
    }
  }

  @Test
  public void testProcessSummationOneCellSumFinal() throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();

    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();
    List<Tag> tags = new ArrayList<>();
    Tag t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM_FINAL.getTagType(),
        "application_123458888888_999888");
    tags.add(t);
    byte[] tagByteArray =
        HBaseTimelineServerUtils.convertTagListToByteArray(tags);
    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);

    // create a cell with a VERY old timestamp
    Cell c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, 120L, Bytes.toBytes(1110L), tagByteArray);
    currentColumnCells.add(c1);

    List<Cell> cells = fs.processSummationMajorCompaction(currentColumnCells,
        new LongConverter(), currentTimestamp);
    assertNotNull(cells);
    // we should not get the same cell back
    // but we get back the flow cell
    assertEquals(1, cells.size());

    Cell returnedCell = cells.get(0);
    // it's NOT the same cell
    assertNotEquals(c1, returnedCell);
    long inputTs = c1.getTimestamp();
    long returnTs = returnedCell.getTimestamp();
    // the returned Ts will be far greater than input ts as well as the noted
    // current timestamp
    assertTrue(returnTs > inputTs);
    assertTrue(returnTs >= currentTimestamp);
  }

  @Test
  public void testProcessSummationOneCell() throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();

    // note down the current timestamp
    long currentTimestamp = System.currentTimeMillis();

    // try for 1 cell with tag SUM
    List<Tag> tags = new ArrayList<>();
    Tag t = HBaseTimelineServerUtils.createTag(
        AggregationOperation.SUM.getTagType(),
        "application_123458888888_999888");
    tags.add(t);
    byte[] tagByteArray =
        HBaseTimelineServerUtils.convertTagListToByteArray(tags);

    SortedSet<Cell> currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);

    Cell c1 = HBaseTimelineServerUtils.createNewCell(aRowKey, aFamily,
        aQualifier, currentTimestamp, Bytes.toBytes(1110L), tagByteArray);
    currentColumnCells.add(c1);
    List<Cell> cells = fs.processSummationMajorCompaction(currentColumnCells,
        new LongConverter(), currentTimestamp);
    assertNotNull(cells);
    // we expect the same cell back
    assertEquals(1, cells.size());
    Cell c2 = cells.get(0);
    assertEquals(c1, c2);
    assertEquals(currentTimestamp, c2.getTimestamp());
  }

  @Test
  public void testProcessSummationEmpty() throws IOException {
    FlowScanner fs = getFlowScannerForTestingCompaction();
    long currentTimestamp = System.currentTimeMillis();

    LongConverter longConverter = new LongConverter();

    SortedSet<Cell> currentColumnCells = null;
    List<Cell> cells =
        fs.processSummationMajorCompaction(currentColumnCells, longConverter,
            currentTimestamp);
    assertNotNull(cells);
    assertEquals(0, cells.size());

    currentColumnCells = new TreeSet<Cell>(KeyValue.COMPARATOR);
    cells =
        fs.processSummationMajorCompaction(currentColumnCells, longConverter,
            currentTimestamp);
    assertNotNull(cells);
    assertEquals(0, cells.size());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
