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

package org.apache.hadoop.metrics2.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaSink30;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaSink31;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaMetricsTestHelper;
import org.junit.Test;

public class TestGangliaMetrics {
  public static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  private final String[] expectedMetrics =
    { "test.s1rec.C1",
      "test.s1rec.G1",
      "test.s1rec.Xxx",
      "test.s1rec.Yyy",
      "test.s1rec.S1NumOps",
      "test.s1rec.S1AvgTime" };

  @Test
  public void testTagsForPrefix() throws Exception {
    ConfigBuilder cb = new ConfigBuilder()
      .add("test.sink.ganglia.tagsForPrefix.all", "*")
      .add("test.sink.ganglia.tagsForPrefix.some", "NumActiveSinks, " +
              "NumActiveSources")
      .add("test.sink.ganglia.tagsForPrefix.none", "");
    GangliaSink30 sink = new GangliaSink30();
    sink.init(cb.subset("test.sink.ganglia"));

    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "all"));
    tags.add(new MetricsTag(MsInfo.NumActiveSources, "foo"));
    tags.add(new MetricsTag(MsInfo.NumActiveSinks, "bar"));
    tags.add(new MetricsTag(MsInfo.NumAllSinks, "haa"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host"));
    Set<AbstractMetric> metrics = new HashSet<AbstractMetric>();
    MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long) 1, tags, metrics);

    StringBuilder sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals(".NumActiveSources=foo.NumActiveSinks=bar.NumAllSinks=haa", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "some"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals(".NumActiveSources=foo.NumActiveSinks=bar", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "none"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals("", sb.toString());

    tags.set(0, new MetricsTag(MsInfo.Context, "nada"));
    sb = new StringBuilder();
    sink.appendPrefix(record, sb);
    assertEquals("", sb.toString());
  }
  
  @Test public void testGangliaMetrics2() throws Exception {
    // Setting long interval to avoid periodic publishing.
    // We manually publish metrics by MeticsSystem#publishMetricsNow here.
    ConfigBuilder cb = new ConfigBuilder().add("*.period", 120)
        .add("test.sink.gsink30.context", "test") // filter out only "test"
        .add("test.sink.gsink31.context", "test") // filter out only "test"
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));

    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);

    final int expectedCountFromGanglia30 = expectedMetrics.length;
    final int expectedCountFromGanglia31 = 2 * expectedMetrics.length;

    // Setup test for GangliaSink30
    AbstractGangliaSink gsink30 = new GangliaSink30();
    gsink30.init(cb.subset("test"));
    MockDatagramSocket mockds30 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink30, mockds30);

    // Setup test for GangliaSink31
    AbstractGangliaSink gsink31 = new GangliaSink31();
    gsink31.init(cb.subset("test"));
    MockDatagramSocket mockds31 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink31, mockds31);

    // register the sinks
    ms.register("gsink30", "gsink30 desc", gsink30);
    ms.register("gsink31", "gsink31 desc", gsink31);
    ms.publishMetricsNow(); // publish the metrics

    ms.stop();

    // check GanfliaSink30 data
    checkMetrics(mockds30.getCapturedSend(), expectedCountFromGanglia30);

    // check GanfliaSink31 data
    checkMetrics(mockds31.getCapturedSend(), expectedCountFromGanglia31);
  }


  // check the expected against the actual metrics
  private void checkMetrics(List<byte[]> bytearrlist, int expectedCount) {
    boolean[] foundMetrics = new boolean[expectedMetrics.length];
    for (byte[] bytes : bytearrlist) {
      String binaryStr = new String(bytes, Charsets.UTF_8);
      for (int index = 0; index < expectedMetrics.length; index++) {
        if (binaryStr.indexOf(expectedMetrics[index]) >= 0) {
          foundMetrics[index] = true;
          break;
        }
      }
    }

    for (int index = 0; index < foundMetrics.length; index++) {
      if (!foundMetrics[index]) {
        assertTrue("Missing metrics: " + expectedMetrics[index], false);
      }
    }

    assertEquals("Mismatch in record count: ",
        expectedCount, bytearrlist.size());
  }

  @SuppressWarnings("unused")
  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  /**
   * This class is used to capture data send to Ganglia servers.
   *
   * Initial attempt was to use mockito to mock and capture but
   * while testing figured out that mockito is keeping the reference
   * to the byte array and since the sink code reuses the byte array
   * hence all the captured byte arrays were pointing to one instance.
   */
  private class MockDatagramSocket extends DatagramSocket {
    private List<byte[]> capture;

    /**
     * @throws SocketException
     */
    public MockDatagramSocket() throws SocketException {
      capture = new ArrayList<byte[]>();
    }

    /* (non-Javadoc)
     * @see java.net.DatagramSocket#send(java.net.DatagramPacket)
     */
    @Override
    public synchronized void send(DatagramPacket p) throws IOException {
      // capture the byte arrays
      byte[] bytes = new byte[p.getLength()];
      System.arraycopy(p.getData(), p.getOffset(), bytes, 0, p.getLength());
      capture.add(bytes);
    }

    /**
     * @return the captured byte arrays
     */
    synchronized List<byte[]> getCapturedSend() {
      return capture;
    }
  }
}
