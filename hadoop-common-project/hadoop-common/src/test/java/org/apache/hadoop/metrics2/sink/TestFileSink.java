/*
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

package org.apache.hadoop.metrics2.sink;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.impl.TestMetricsConfig;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestFileSink {
  
  private File outFile;

  // The 2 sample metric classes:
  @Metrics(name="testRecord1", context="test1")
  static class MyMetrics1 {
    @Metric(value={"testTag1", ""}, type=Type.TAG) 
    String testTag1() { return "testTagValue1"; }
    
    @Metric(value={"testTag2", ""}, type=Type.TAG) 
    String gettestTag2() { return "testTagValue2"; }
    
    @Metric(value={"testMetric1", "An integer gauge"},always=true) 
    MutableGaugeInt testMetric1;
    
    @Metric(value={"testMetric2", "An integer gauge"},always=true) 
    MutableGaugeInt testMetric2;

    public MyMetrics1 registerWith(MetricsSystem ms) {
      return ms.register("m1", null, this);
    }
  }
  
  @Metrics(name="testRecord2", context="test1")
  static class MyMetrics2 {
    @Metric(value={"testTag22", ""}, type=Type.TAG) 
    String testTag1() { return "testTagValue22"; }

    public MyMetrics2 registerWith(MetricsSystem ms) {
      return ms.register("m2", null, this);
    }
  }
  
  private File getTestTempFile(String prefix, String suffix) throws IOException {
    String tmpPath = System.getProperty("java.io.tmpdir", "/tmp");
    String user = System.getProperty("user.name", "unknown-user");
    File dir = new File(tmpPath + "/" + user);
    dir.mkdirs();
    return File.createTempFile(prefix, suffix, dir);
  }
  
  @Test(timeout=6000) 
  public void testFileSink() throws IOException {
    outFile = getTestTempFile("test-file-sink-", ".out");
    final String outPath = outFile.getAbsolutePath();  
    
    // NB: specify large period to avoid multiple metrics snapshotting: 
    new ConfigBuilder().add("*.period", 10000)
        .add("test.sink.mysink0.class", FileSink.class.getName())
        .add("test.sink.mysink0.filename", outPath)
        // NB: we filter by context to exclude "metricssystem" context metrics:
        .add("test.sink.mysink0.context", "test1")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("test");
    ms.start();

    final MyMetrics1 mm1 
      = new MyMetrics1().registerWith(ms);
    new MyMetrics2().registerWith(ms);

    mm1.testMetric1.incr();
    mm1.testMetric2.incr(2);

    ms.publishMetricsNow(); // publish the metrics
    ms.stop();
    ms.shutdown();

    InputStream is = null;
    ByteArrayOutputStream baos = null;
    String outFileContent = null;
    try {
      is = new FileInputStream(outFile);
      baos = new ByteArrayOutputStream((int)outFile.length());
      IOUtils.copyBytes(is, baos, 1024, true);
      outFileContent = new String(baos.toByteArray(), "UTF-8");
    } finally {
      IOUtils.cleanupWithLogger(null, baos, is);
    }

    // Check the out file content. Should be something like the following:
    //1360244820087 test1.testRecord1: Context=test1, testTag1=testTagValue1, testTag2=testTagValue2, Hostname=myhost, testMetric1=1, testMetric2=2
    //1360244820089 test1.testRecord2: Context=test1, testTag22=testTagValue22, Hostname=myhost
    
    // Note that in the below expression we allow tags and metrics to go in arbitrary order.  
    Pattern expectedContentPattern = Pattern.compile(
        // line #1:
        "^\\d+\\s+test1.testRecord1:\\s+Context=test1,\\s+" +
        "(testTag1=testTagValue1,\\s+testTag2=testTagValue2|testTag2=testTagValue2,\\s+testTag1=testTagValue1)," +
        "\\s+Hostname=.*,\\s+(testMetric1=1,\\s+testMetric2=2|testMetric2=2,\\s+testMetric1=1)" +
        // line #2:
        "$[\\n\\r]*^\\d+\\s+test1.testRecord2:\\s+Context=test1," +
        "\\s+testTag22=testTagValue22,\\s+Hostname=.*$[\\n\\r]*", 
         Pattern.MULTILINE);
     assertTrue(expectedContentPattern.matcher(outFileContent).matches());
  }
  
  @After
  public void after() {
    if (outFile != null) {
      outFile.delete();
      assertTrue(!outFile.exists());
    }
  }
}
