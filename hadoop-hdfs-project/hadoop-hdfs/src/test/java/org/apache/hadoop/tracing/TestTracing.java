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
package org.apache.hadoop.tracing;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TestTracing {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @Test
  public void testTracing() throws Exception {
    // write and read without tracing started
    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.size() == 0);
    readTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.size() == 0);

    writeWithTracing();
    readWithTracing();
  }

  public void writeWithTracing() throws Exception {
    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testWriteTraceHooks", Sampler.ALWAYS);
    writeTestFile("testWriteTraceHooks.dat");
    long endTime = System.currentTimeMillis();
    ts.close();

    String[] expectedSpanNames = {
      "testWriteTraceHooks",
      "ClientProtocol#create",
      "ClientNamenodeProtocol#create",
      "ClientProtocol#fsync",
      "ClientNamenodeProtocol#fsync",
      "ClientProtocol#complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#write",
      "DFSOutputStream#close",
      "dataStreamer",
      "OpWriteBlockProto",
      "ClientProtocol#addBlock",
      "ClientNamenodeProtocol#addBlock"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.getMap();
    Span s = map.get("testWriteTraceHooks").get(0);
    Assert.assertNotNull(s);
    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();

    // Spans homed in the top trace shoud have same trace id.
    // Spans having multiple parents (e.g. "dataStreamer" added by HDFS-7054)
    // and children of them are exception.
    String[] spansInTopTrace = {
      "testWriteTraceHooks",
      "ClientProtocol#create",
      "ClientNamenodeProtocol#create",
      "ClientProtocol#fsync",
      "ClientNamenodeProtocol#fsync",
      "ClientProtocol#complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#write",
      "DFSOutputStream#close",
    };
    for (String desc : spansInTopTrace) {
      for (Span span : map.get(desc)) {
        Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
      }
    }

    // test for timeline annotation added by HADOOP-11242
    Assert.assertEquals("called",
        map.get("ClientProtocol#create")
           .get(0).getTimelineAnnotations()
           .get(0).getMessage());

    SetSpanReceiver.clear();
  }

  public void readWithTracing() throws Exception {
    String fileName = "testReadTraceHooks.dat";
    writeTestFile(fileName);
    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testReadTraceHooks", Sampler.ALWAYS);
    readTestFile(fileName);
    ts.close();
    long endTime = System.currentTimeMillis();

    String[] expectedSpanNames = {
      "testReadTraceHooks",
      "ClientProtocol#getBlockLocations",
      "ClientNamenodeProtocol#getBlockLocations",
      "OpReadBlockProto"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.getMap();
    Span s = map.get("testReadTraceHooks").get(0);
    Assert.assertNotNull(s);

    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();
    Assert.assertTrue(spanStart - startTime < 100);
    Assert.assertTrue(spanEnd - endTime < 100);

    // There should only be one trace id as it should all be homed in the
    // top trace.
    for (Span span : SetSpanReceiver.getSpans()) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
    SetSpanReceiver.clear();
  }

  private void writeTestFile(String testFileName) throws Exception {
    Path filePath = new Path(testFileName);
    FSDataOutputStream stream = dfs.create(filePath);
    for (int i = 0; i < 10; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(102400).getBytes();
      stream.write(data);
    }
    stream.hsync();
    stream.close();
  }

  private void readTestFile(String testFileName) throws Exception {
    Path filePath = new Path(testFileName);
    FSDataInputStream istream = dfs.open(filePath, 10240);
    ByteBuffer buf = ByteBuffer.allocate(10240);

    int count = 0;
    try {
      while (istream.read(buf) > 0) {
        count += 1;
        buf.clear();
        istream.seek(istream.getPos() + 5);
      }
    } catch (IOException ioe) {
      // Ignore this it's probably a seek after eof.
    } finally {
      istream.close();
    }
  }

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.set(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
  }

  @Before
  public void startCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    SetSpanReceiver.clear();
  }

  @After
  public void shutDown() throws IOException {
    cluster.shutdown();
  }

}
