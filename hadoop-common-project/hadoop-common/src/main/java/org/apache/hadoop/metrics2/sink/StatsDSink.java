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

package org.apache.hadoop.metrics2.sink;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metrics sink that writes metrics to a StatsD daemon.
 * This sink will produce metrics of the form
 * '[hostname].servicename.context.name.metricname:value|type'
 * where hostname is optional. This is useful when sending to
 * a daemon that is running on the localhost and will add the
 * hostname to the metric (such as the
 * <a href="https://collectd.org/">CollectD</a> StatsD plugin).
 * <br>
 * To configure this plugin, you will need to add the following
 * entries to your hadoop-metrics2.properties file:
 * <br>
 * <pre>
 * *.sink.statsd.class=org.apache.hadoop.metrics2.sink.StatsDSink
 * [prefix].sink.statsd.server.host=
 * [prefix].sink.statsd.server.port=
 * [prefix].sink.statsd.skip.hostname=true|false (optional)
 * [prefix].sink.statsd.service.name=NameNode (name you want for service)
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StatsDSink implements MetricsSink, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(StatsDSink.class);
  private static final String PERIOD = ".";
  private static final String SERVER_HOST_KEY = "server.host";
  private static final String SERVER_PORT_KEY = "server.port";
  private static final String HOST_NAME_KEY = "host.name";
  private static final String SERVICE_NAME_KEY = "service.name";
  private static final String SKIP_HOSTNAME_KEY = "skip.hostname";
  private boolean skipHostname = false;
  private String hostName = null;
  private String serviceName = null;
  private StatsD statsd = null;

  @Override
  public void init(SubsetConfiguration conf) {
    // Get StatsD host configurations.
    final String serverHost = conf.getString(SERVER_HOST_KEY);
    final int serverPort = Integer.parseInt(conf.getString(SERVER_PORT_KEY));

    skipHostname = conf.getBoolean(SKIP_HOSTNAME_KEY, false);
    if (!skipHostname) {
      hostName = conf.getString(HOST_NAME_KEY, null);
      if (null == hostName) {
        hostName = NetUtils.getHostname();
      }
    }

    serviceName = conf.getString(SERVICE_NAME_KEY, null);

    statsd = new StatsD(serverHost, serverPort);
  }

  @Override
  public void putMetrics(MetricsRecord record) {

    String hn = hostName;
    String ctx = record.context();
    String sn = serviceName;

    for (MetricsTag tag : record.tags()) {
      if (tag.info().name().equals(MsInfo.Hostname.name())
          && tag.value() != null) {
        hn = tag.value();
      } else if (tag.info().name().equals(MsInfo.Context.name())
          && tag.value() != null) {
        ctx = tag.value();
      } else if (tag.info().name().equals(MsInfo.ProcessName.name())
          && tag.value() != null) {
        sn = tag.value();
      }
    }

    StringBuilder buf = new StringBuilder();
    if (!skipHostname && hn != null) {
      int idx = hn.indexOf(".");
      if (idx == -1) {
        buf.append(hn).append(PERIOD);
      } else {
        buf.append(hn.substring(0, idx)).append(PERIOD);
      }
    }
    buf.append(sn).append(PERIOD)
        .append(ctx).append(PERIOD)
        .append(record.name().replaceAll("\\.", "-")).append(PERIOD);

    // Collect datapoints.
    for (AbstractMetric metric : record.metrics()) {
      String type = null;
      if (metric.type().equals(MetricType.COUNTER)) {
        type = "c";
      } else if (metric.type().equals(MetricType.GAUGE)) {
        type = "g";
      }
      StringBuilder line = new StringBuilder();
      line.append(buf.toString())
          .append(metric.name().replace(' ', '_'))
          .append(":")
          .append(metric.value())
          .append("|")
          .append(type);
      writeMetric(line.toString());
    }

  }

  public void writeMetric(String line) {
    try {
      statsd.write(line);
    } catch (IOException e) {
      LOG.warn("Error sending metrics to StatsD", e);
      throw new MetricsException("Error writing metric to StatsD", e);
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws IOException {
    statsd.close();
  }

  /**
   * Class that sends UDP packets to StatsD daemon.
   *
   */
  public static class StatsD {

    private DatagramSocket socket = null;
    private DatagramPacket packet = null;
    private String serverHost;
    private int serverPort;

    public StatsD(String serverHost, int serverPort) {
      this.serverHost = serverHost;
      this.serverPort = serverPort;
    }

    public void createSocket() throws IOException {
      try {
        InetSocketAddress address =
            new InetSocketAddress(this.serverHost, this.serverPort);
        socket = new DatagramSocket();
        packet =
            new DatagramPacket("".getBytes(StandardCharsets.UTF_8), 0, 0,
                address.getAddress(), this.serverPort);
      } catch (IOException ioe) {
        throw NetUtils.wrapException(this.serverHost, this.serverPort,
            "localhost", 0, ioe);
      }
    }

    public void write(String msg) throws IOException {
      if (null == socket) {
        createSocket();
      }
      LOG.debug("Sending metric: {}", msg);
      packet.setData(msg.getBytes(StandardCharsets.UTF_8));
      socket.send(packet);
    }

    public void close() throws IOException {
      try {
        if (socket != null) {
          socket.close();
        }
      } finally {
        socket = null;
      }
    }

  }

}
