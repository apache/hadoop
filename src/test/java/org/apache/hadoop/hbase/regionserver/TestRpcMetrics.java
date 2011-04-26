/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRpcMetrics {
  /**
   * Defines test methods to register with HBaseRpcMetrics
   */
  public interface TestMetrics {
    public void test();
  }

  /**
   * HRegionServer sub-class to register custom metrics
   */
  public static class TestRegionServer extends HRegionServer {

    public TestRegionServer(Configuration conf)
        throws IOException, InterruptedException {
      super(conf);

      // register custom metrics interface
      getRpcMetrics().createMetrics(new Class[]{TestMetrics.class}, true);
    }

    public void incTest(int amt) {
      HBaseRpcMetrics metrics = getRpcMetrics();
      // force an increment so we have something to check for
      metrics.inc(metrics.getMetricName(TestMetrics.class, "test"), amt);
    }
  }

  /**
   * Dummy metrics context to allow retrieval of values
   */
  public static class MockMetricsContext extends AbstractMetricsContext {

    public MockMetricsContext() {
      // update every 1 sec.
      setPeriod(1);
    }

    @Override
    protected void emitRecord(String contextName, String recordName,
        OutputRecord outputRecord) throws IOException {
      for (String name : outputRecord.getMetricNames()) {
        Number val = outputRecord.getMetric(name);
        if (val != null && val.intValue() > 0) {
          METRICS.put(name, Boolean.TRUE);
          LOG.debug("Set metric "+name+" to "+val);
        }
      }
    }
  }
  private static Map<String,Boolean> METRICS = new HashMap<String,Boolean>();

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Log LOG = LogFactory.getLog(TestRpcMetrics.class);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set custom metrics context
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute("rpc.class", MockMetricsContext.class.getName());
    // make sure metrics context is setup, otherwise updating won't start
    MetricsContext ctx = MetricsUtil.getContext("rpc");
    assertTrue("Wrong MetricContext implementation class",
        (ctx instanceof MockMetricsContext));

    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testCustomMetrics() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.port", 0);
    TestRegionServer rs = new TestRegionServer(TEST_UTIL.getConfiguration());
    rs.incTest(5);

    // wait for metrics context update
    Thread.sleep(1000);

    String metricName = HBaseRpcMetrics.getMetricName(TestMetrics.class, "test");
    assertTrue("Metric should have set incremented for "+metricName,
        wasSet(metricName + "_num_ops"));
  }

  public boolean wasSet(String name) {
    return METRICS.get(name) != null ? METRICS.get(name) : false;
  }
}
