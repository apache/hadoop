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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTracing {
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  private Tracer prevTracer;

  private final static Configuration TRACING_CONF;
  private final static Configuration NO_TRACING_CONF;

  static {
    NO_TRACING_CONF = new Configuration();
    NO_TRACING_CONF.setLong("dfs.blocksize", 100 * 1024);

    TRACING_CONF = new Configuration(NO_TRACING_CONF);
    TRACING_CONF.set(CommonConfigurationKeys.FS_CLIENT_HTRACE_PREFIX +
        Tracer.SPAN_RECEIVER_CLASSES_KEY,
        SetSpanReceiver.class.getName());
    TRACING_CONF.set(CommonConfigurationKeys.FS_CLIENT_HTRACE_PREFIX +
        Tracer.SAMPLER_CLASSES_KEY, "AlwaysSampler");
  }

  @Test
  public void testTracing() throws Exception {
    // write and read without tracing started
    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
    Assert.assertEquals(0, SetSpanReceiver.size());
    readTestFile(fileName);
    Assert.assertEquals(0, SetSpanReceiver.size());

    writeTestFile("testReadTraceHooks.dat");

    FsTracer.clear();
    Tracer tracer = FsTracer.get(TRACING_CONF);
    writeWithTracing(tracer);
    readWithTracing(tracer);
  }

  private void writeWithTracing(Tracer tracer) throws Exception {
    long startTime = System.currentTimeMillis();
    TraceScope ts = tracer.newScope("testWriteTraceHooks");
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
        Assert.assertEquals(ts.getSpan().getSpanId().getHigh(),
                            span.getSpanId().getHigh());
      }
    }

    // test for timeline annotation added by HADOOP-11242
    Assert.assertEquals("called",
        map.get("ClientProtocol#create")
           .get(0).getTimelineAnnotations()
           .get(0).getMessage());

    SetSpanReceiver.clear();
  }

  private void readWithTracing(Tracer tracer) throws Exception {
    long startTime = System.currentTimeMillis();
    TraceScope ts = tracer.newScope("testReadTraceHooks");
    readTestFile("testReadTraceHooks.dat");
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
      System.out.println(span.toJson());
    }
    for (Span span : SetSpanReceiver.getSpans()) {
      Assert.assertEquals(ts.getSpan().getSpanId().getHigh(),
                          span.getSpanId().getHigh());
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

  @Before
  public void startCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(NO_TRACING_CONF)
        .numDataNodes(3)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    SetSpanReceiver.clear();
  }

  @After
  public void shutDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    FsTracer.clear();
  }
}
