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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Supplier;

public class TestTracing {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @Test
  public void testTracing() throws Exception {
    // write and read without tracing started
    String fileName = "testTracingDisabled.dat";
    writeTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);
    readTestFile(fileName);
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);

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
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.create",
      "ClientNamenodeProtocol#create",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.fsync",
      "ClientNamenodeProtocol#fsync",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#writeChunk",
      "DFSOutputStream#close",
      "dataStreamer",
      "OpWriteBlockProto",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.addBlock",
      "ClientNamenodeProtocol#addBlock"
    };
    assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    Span s = map.get("testWriteTraceHooks").get(0);
    Assert.assertNotNull(s);
    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();

    // Spans homed in the top trace shoud have same trace id.
    // Spans having multiple parents (e.g. "dataStreamer" added by HDFS-7054)
    // and children of them are exception.
    String[] spansInTopTrace = {
      "testWriteTraceHooks",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.create",
      "ClientNamenodeProtocol#create",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.fsync",
      "ClientNamenodeProtocol#fsync",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.complete",
      "ClientNamenodeProtocol#complete",
      "newStreamForCreate",
      "DFSOutputStream#writeChunk",
      "DFSOutputStream#close",
    };
    for (String desc : spansInTopTrace) {
      for (Span span : map.get(desc)) {
        Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
      }
    }
    SetSpanReceiver.SetHolder.spans.clear();
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
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.getBlockLocations",
      "ClientNamenodeProtocol#getBlockLocations",
      "OpReadBlockProto"
    };
    assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    Span s = map.get("testReadTraceHooks").get(0);
    Assert.assertNotNull(s);

    long spanStart = s.getStartTimeMillis();
    long spanEnd = s.getStopTimeMillis();
    Assert.assertTrue(spanStart - startTime < 100);
    Assert.assertTrue(spanEnd - endTime < 100);

    // There should only be one trace id as it should all be homed in the
    // top trace.
    for (Span span : SetSpanReceiver.SetHolder.spans.values()) {
      Assert.assertEquals(ts.getSpan().getTraceId(), span.getTraceId());
    }
    SetSpanReceiver.SetHolder.spans.clear();
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
    SetSpanReceiver.SetHolder.spans.clear();
  }

  @After
  public void shutDown() throws IOException {
    cluster.shutdown();
  }

  static void assertSpanNamesFound(final String[] expectedSpanNames) {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
          for (String spanName : expectedSpanNames) {
            if (!map.containsKey(spanName)) {
              return false;
            }
          }
          return true;
        }
      }, 100, 1000);
    } catch (TimeoutException e) {
      Assert.fail("timed out to get expected spans: " + e.getMessage());
    } catch (InterruptedException e) {
      Assert.fail("interrupted while waiting spans: " + e.getMessage());
    }
  }

  /**
   * Span receiver that puts all spans into a single set.
   * This is useful for testing.
   * <p/>
   * We're not using HTrace's POJOReceiver here so as that doesn't
   * push all the metrics to a static place, and would make testing
   * SpanReceiverHost harder.
   */
  public static class SetSpanReceiver implements SpanReceiver {

    public SetSpanReceiver(HTraceConfiguration conf) {
    }

    public void receiveSpan(Span span) {
      SetHolder.spans.put(span.getSpanId(), span);
    }

    public void close() {
    }

    public static class SetHolder {
      public static ConcurrentHashMap<Long, Span> spans = 
          new ConcurrentHashMap<Long, Span>();
          
      public static int size() {
        return spans.size();
      }

      public static Map<String, List<Span>> getMap() {
        Map<String, List<Span>> map = new HashMap<String, List<Span>>();

        for (Span s : spans.values()) {
          List<Span> l = map.get(s.getDescription());
          if (l == null) {
            l = new LinkedList<Span>();
            map.put(s.getDescription(), l);
          }
          l.add(s);
        }
        return map;
      }
    }
  }
}
