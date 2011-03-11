/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.ParsedLine;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Result;

import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import static org.junit.Assert.*;

public class TestImportTsv {

  @Test
  public void testTsvParserSpecParsing() {
    TsvParser parser;

    parser = new TsvParser("HBASE_ROW_KEY", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertEquals(0, parser.getRowKeyColumnIndex());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertEquals(0, parser.getRowKeyColumnIndex());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1,col1:scol2", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(2));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(2));
    assertEquals(0, parser.getRowKeyColumnIndex());
  }

  @Test
  public void testTsvParser() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,col_b:qual,HBASE_ROW_KEY,col_d", "\t");
    assertBytesEquals(Bytes.toBytes("col_a"), parser.getFamily(0));
    assertBytesEquals(HConstants.EMPTY_BYTE_ARRAY, parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col_b"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("qual"), parser.getQualifier(1));
    assertNull(parser.getFamily(2));
    assertNull(parser.getQualifier(2));
    assertEquals(2, parser.getRowKeyColumnIndex());
    
    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c\tval_d");
    ParsedLine parsed = parser.parse(line, line.length);
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  private void checkParsing(ParsedLine parsed, Iterable<String> expected) {
    ArrayList<String> parsedCols = new ArrayList<String>();
    for (int i = 0; i < parsed.getColumnCount(); i++) {
      parsedCols.add(Bytes.toString(
          parsed.getLineBytes(),
          parsed.getColumnOffset(i),
          parsed.getColumnLength(i)));
    }
    if (!Iterables.elementsEqual(parsedCols, expected)) {
      fail("Expected: " + Joiner.on(",").join(expected) + "\n" + 
          "Got:" + Joiner.on(",").join(parsedCols));
    }
  }
  
  private void assertBytesEquals(byte[] a, byte[] b) {
    assertEquals(Bytes.toStringBinary(a), Bytes.toStringBinary(b));
  }

  /**
   * Test cases that throw BadTsvLineException
   */
  @Test(expected=BadTsvLineException.class)
  public void testTsvParserBadTsvLineExcessiveColumns() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c");
    ParsedLine parsed = parser.parse(line, line.length);
  }

  @Test(expected=BadTsvLineException.class)
  public void testTsvParserBadTsvLineZeroColumn() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("");
    ParsedLine parsed = parser.parse(line, line.length);
  }

  @Test(expected=BadTsvLineException.class)
  public void testTsvParserBadTsvLineOnlyKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("key_only");
    ParsedLine parsed = parser.parse(line, line.length);
  }

  @Test(expected=BadTsvLineException.class)
  public void testTsvParserBadTsvLineNoRowKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,HBASE_ROW_KEY", "\t");
    byte[] line = Bytes.toBytes("only_cola_data_and_no_row_key");
    ParsedLine parsed = parser.parse(line, line.length);
  }

  @Test
  public void testMROnTable()
  throws Exception {
    String TABLE_NAME = "TestTable";
    String FAMILY = "FAM";
    String INPUT_FILE = "InputFile.esv";
    
    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
        TABLE_NAME,
        INPUT_FILE
    };

    // Cluster
    HBaseTestingUtility htu1 = new HBaseTestingUtility();

    MiniHBaseCluster cluster = htu1.startMiniCluster();

    GenericOptionsParser opts = new GenericOptionsParser(cluster.getConfiguration(), args);
    Configuration conf = opts.getConfiguration();
    args = opts.getRemainingArgs();

    try {
      
      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream op = fs.create(new Path(INPUT_FILE), true);
      String line = "KEY\u001bVALUE1\u001bVALUE2\n";
      op.write(line.getBytes(HConstants.UTF8_ENCODING));
      op.close();

      final byte[] FAM = Bytes.toBytes(FAMILY);
      final byte[] TAB = Bytes.toBytes(TABLE_NAME);
      final byte[] QA = Bytes.toBytes("A");
      final byte[] QB = Bytes.toBytes("B");

      HTableDescriptor desc = new HTableDescriptor(TAB);
      desc.addFamily(new HColumnDescriptor(FAM));
      new HBaseAdmin(conf).createTable(desc);

      Job job = ImportTsv.createSubmittableJob(conf, args);
      job.waitForCompletion(false);
      assertTrue(job.isSuccessful());
      
      HTable table = new HTable(new Configuration(conf), TAB);
      boolean verified = false;
      long pause = conf.getLong("hbase.client.pause", 5 * 1000);
      int numRetries = conf.getInt("hbase.client.retries.number", 5);
      for (int i = 0; i < numRetries; i++) {
        try {
          Scan scan = new Scan();
          // Scan entire family.
          scan.addFamily(FAM);
          ResultScanner resScanner = table.getScanner(scan);
          for (Result res : resScanner) {
            assertTrue(res.size() == 2);
            List<KeyValue> kvs = res.list();
            assertEquals(toU8Str(kvs.get(0).getRow()),
                toU8Str(Bytes.toBytes("KEY")));
            assertEquals(toU8Str(kvs.get(1).getRow()),
                toU8Str(Bytes.toBytes("KEY")));
            assertEquals(toU8Str(kvs.get(0).getValue()),
                toU8Str(Bytes.toBytes("VALUE1")));
            assertEquals(toU8Str(kvs.get(1).getValue()),
                toU8Str(Bytes.toBytes("VALUE2")));
            // Only one result set is expected, so let it loop.
          }
          verified = true;
          break;
        } catch (NullPointerException e) {
          // If here, a cell was empty.  Presume its because updates came in
          // after the scanner had been opened.  Wait a while and retry.
        }
        try {
          Thread.sleep(pause);
        } catch (InterruptedException e) {
          // continue
        }
      }
      assertTrue(verified);
    } finally {
      cluster.shutdown();
    }
  }
  
  public static String toU8Str(byte[] bytes) throws UnsupportedEncodingException {
    return new String(bytes, HConstants.UTF8_ENCODING);
  }
}
