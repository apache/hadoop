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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestAcidGuarantees {
  protected static final Log LOG = LogFactory.getLog(TestAcidGuarantees.class);
  public static final byte [] TABLE_NAME = Bytes.toBytes("TestAcidGuarantees");
  public static final byte [] FAMILY_A = Bytes.toBytes("A");
  public static final byte [] FAMILY_B = Bytes.toBytes("B");
  public static final byte [] FAMILY_C = Bytes.toBytes("C");
  public static final byte [] QUALIFIER_NAME = Bytes.toBytes("data");

  public static final byte[][] FAMILIES = new byte[][] {
    FAMILY_A, FAMILY_B, FAMILY_C };

  private HBaseTestingUtility util;

  public static int NUM_COLS_TO_CHECK = 50;

  private void createTableIfMissing()
    throws IOException {
    try {
      util.createTable(TABLE_NAME, FAMILIES);
    } catch (TableExistsException tee) {
    }
  }

  public TestAcidGuarantees() {
    util = new HBaseTestingUtility();
  }
  
  public static class AtomicityWriter extends TestThread {
    Random rand = new Random();
    byte data[] = new byte[10];
    byte targetRow[];
    byte targetFamilies[][];
    HTable table;
    AtomicLong numWritten = new AtomicLong();
    
    public AtomicityWriter(TestContext ctx, byte targetRow[],
                           byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      this.targetFamilies = targetFamilies;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }
    public void doAnAction() throws Exception {
      Put p = new Put(targetRow); 
      rand.nextBytes(data);

      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          p.add(family, qualifier, data);
        }
      }
      table.put(p);
      numWritten.getAndIncrement();
    }
  }
  
  public static class AtomicityReader extends TestThread {
    byte targetRow[];
    byte targetFamilies[][];
    HTable table;
    int numVerified = 0;
    AtomicLong numRead = new AtomicLong();

    public AtomicityReader(TestContext ctx, byte targetRow[],
                           byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      this.targetFamilies = targetFamilies;
      table = new HTable(ctx.getConf(), TABLE_NAME);
    }

    public void doAnAction() throws Exception {
      Get g = new Get(targetRow);
      Result res = table.get(g);
      byte[] gotValue = null;

      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          byte thisValue[] = res.getValue(family, qualifier);
          if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
            gotFailure(gotValue, res);
          }
          numVerified++;
          gotValue = thisValue;
        }
      }
      numRead.getAndIncrement();
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numVerified).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (KeyValue kv : res.list()) {
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(kv.getValue()));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }


  public void runTestAtomicity(long millisToRun) throws Exception {
    createTableIfMissing();
    TestContext ctx = new TestContext(util.getConfiguration());
    byte row[] = Bytes.toBytes("test_row");

    List<AtomicityWriter> writers = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      AtomicityWriter writer = new AtomicityWriter(ctx, row, FAMILIES);
      writers.add(writer);
      ctx.addThread(writer);
    }

    List<AtomicityReader> readers = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      AtomicityReader reader = new AtomicityReader(ctx, row, FAMILIES);
      readers.add(reader);
      ctx.addThread(reader);
    }
    
    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();
    
    LOG.info("Finished test. Writers:");
    for (AtomicityWriter writer : writers) {
      LOG.info("  wrote " + writer.numWritten.get());
    }
    LOG.info("Readers:");
    for (AtomicityReader reader : readers) {
      LOG.info("  read " + reader.numRead.get());
    }
  }

  @Test
  public void testAtomicity() throws Exception {
    util.startMiniCluster(3);
    try {
      runTestAtomicity(20000);
    } finally {
      util.shutdownMiniCluster();
    }    
  }
  
  public static void main(String args[]) throws Exception {
    Configuration c = HBaseConfiguration.create();
    TestAcidGuarantees test = new TestAcidGuarantees();
    test.setConf(c);
    test.runTestAtomicity(5*60*1000);
  }

  private void setConf(Configuration c) {
    util = new HBaseTestingUtility(c);
  }
}
