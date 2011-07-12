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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.lib.AbstractMetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaSink30;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaSink31;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaMetricsTestHelper;
import org.junit.Test;

public class TestGangliaMetrics {
  public static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  private final String[] expectedMetrics =
    {"test.s1rec.c1",
     "test.s1rec.g1",
     "test.s1rec.s1_num_ops",
     "test.s1rec.s1_avg_time"};

  @Test public void testGangliaMetrics2() throws Exception {
    ConfigBuilder cb = new ConfigBuilder().add("default.period", 10)
        .add("test.sink.gsink30.context", "test") // filter out only "test"
        .add("test.sink.gsink31.context", "test") // filter out only "test"
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));

    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.s1.add(0);

    AbstractGangliaSink gsink30 = new GangliaSink30();
    gsink30.init(cb.subset("test"));
    MockDatagramSocket mockds30 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink30, mockds30);

    AbstractGangliaSink gsink31 = new GangliaSink31();
    gsink31.init(cb.subset("test"));
    MockDatagramSocket mockds31 = new MockDatagramSocket();
    GangliaMetricsTestHelper.setDatagramSocket(gsink31, mockds31);

    ms.register("gsink30", "gsink30 desc", gsink30);
    ms.register("gsink31", "gsink31 desc", gsink31);
    ms.onTimerEvent();  // trigger something interesting
    ms.stop();

    // check GanfliaSink30 data
    checkMetrics(mockds30.getCapturedSend());

    // check GanfliaSink31 data
    checkMetrics(mockds31.getCapturedSend());
  }


  private void checkMetrics(List<byte[]> bytearrlist) {
    boolean[] foundMetrics = new boolean[expectedMetrics.length];
    for (byte[] bytes : bytearrlist) {
      String binaryStr = new String(bytes);
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
  }

  @SuppressWarnings("unused")
  private static class TestSource extends AbstractMetricsSource {
    final MetricMutableCounterLong c1;
    final MetricMutableGaugeLong g1;
    final MetricMutableStat s1;

    TestSource(String name) {
      super(name);
      registry.setContext("test");
      c1 = registry.newCounter("c1", "c1 desc", 1L);
      g1 = registry.newGauge("g1", "g1 desc", 2L);
      s1 = registry.newStat("s1", "s1 desc", "ops", "time");
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
  private static class MockDatagramSocket extends DatagramSocket {
    private ArrayList<byte[]> capture;

    public MockDatagramSocket() throws SocketException {
      capture = new  ArrayList<byte[]>();
    }
    /* (non-Javadoc)
     * @see java.net.DatagramSocket#send(java.net.DatagramPacket)
     */
    @Override
    public void send(DatagramPacket p) throws IOException {
      byte[] bytes = new byte[p.getLength()];
      System.arraycopy(p.getData(), p.getOffset(), bytes, 0, p.getLength());
      capture.add(bytes);
    }

    ArrayList<byte[]> getCapturedSend() {
      return capture;
    }
  }
}
