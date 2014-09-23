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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.htrace.HTraceConfiguration;
import org.htrace.Sampler;
import org.htrace.Span;
import org.htrace.SpanReceiver;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.junit.AfterClass;
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
  private static SpanReceiverHost spanReceiverHost;

  @Test
  public void testGetSpanReceiverHost() throws Exception {
    Configuration c = new Configuration();
    // getting instance already loaded.
    c.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY, "");
    SpanReceiverHost s = SpanReceiverHost.getInstance(c);
    Assert.assertEquals(spanReceiverHost, s);
  }

  @Test
  public void testWriteTraceHooks() throws Exception {
    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testWriteTraceHooks", Sampler.ALWAYS);
    Path file = new Path("traceWriteTest.dat");
    FSDataOutputStream stream = dfs.create(file);

    for (int i = 0; i < 10; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(102400).getBytes();
      stream.write(data);
    }
    stream.hflush();
    stream.close();
    long endTime = System.currentTimeMillis();
    ts.close();

    String[] expectedSpanNames = {
      "testWriteTraceHooks",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.create",
      "org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface.create",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.fsync",
      "org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface.fsync",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.complete",
      "org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface.complete",
      "DFSOutputStream",
      "OpWriteBlockProto",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.addBlock",
      "org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface.addBlock"
    };
    assertSpanNamesFound(expectedSpanNames);

    // The trace should last about the same amount of time as the test
    Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
    Span s = map.get("testWriteTraceHooks").get(0);
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
  }

  @Test
  public void testWriteWithoutTraceHooks() throws Exception {
    Path file = new Path("withoutTraceWriteTest.dat");
    FSDataOutputStream stream = dfs.create(file);
    for (int i = 0; i < 10; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(102400).getBytes();
      stream.write(data);
    }
    stream.hflush();
    stream.close();
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);
  }

  @Test
  public void testReadTraceHooks() throws Exception {
    String fileName = "traceReadTest.dat";
    Path filePath = new Path(fileName);

    // Create the file.
    FSDataOutputStream ostream = dfs.create(filePath);
    for (int i = 0; i < 50; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(10240).getBytes();
      ostream.write(data);
    }
    ostream.close();


    long startTime = System.currentTimeMillis();
    TraceScope ts = Trace.startSpan("testReadTraceHooks", Sampler.ALWAYS);
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
    ts.getSpan().addTimelineAnnotation("count: " + count);
    long endTime = System.currentTimeMillis();
    ts.close();

    String[] expectedSpanNames = {
      "testReadTraceHooks",
      "org.apache.hadoop.hdfs.protocol.ClientProtocol.getBlockLocations",
      "org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface.getBlockLocations",
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
  }

  @Test
  public void testReadWithoutTraceHooks() throws Exception {
    String fileName = "withoutTraceReadTest.dat";
    Path filePath = new Path(fileName);

    // Create the file.
    FSDataOutputStream ostream = dfs.create(filePath);
    for (int i = 0; i < 50; i++) {
      byte[] data = RandomStringUtils.randomAlphabetic(10240).getBytes();
      ostream.write(data);
    }
    ostream.close();

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
    Assert.assertTrue(SetSpanReceiver.SetHolder.size() == 0);
  }

  @Before
  public void cleanSet() {
    SetSpanReceiver.SetHolder.spans.clear();
  }

  @BeforeClass
  public static void setupCluster() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        SetSpanReceiver.class.getName());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
    spanReceiverHost = SpanReceiverHost.getInstance(conf);
  }

  @AfterClass
  public static void shutDown() throws IOException {
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

    public void configure(HTraceConfiguration conf) {
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
