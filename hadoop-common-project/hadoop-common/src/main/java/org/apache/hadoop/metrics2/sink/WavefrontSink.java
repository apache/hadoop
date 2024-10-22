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

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * A metrics sink that writes to a Wavefront proxy.
 * Wavefront proxy will then relay the metrics to Wavefront server.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WavefrontSink implements MetricsSink, Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(WavefrontSink.class);
  private static final String SERVER_HOST_KEY = "server_host";
  private static final String SERVER_PORT_KEY = "server_port";
  private static final String METRICS_PREFIX = "metrics_prefix";
  private static final String METRICS_SOURCE = "metrics_source";

  private String metricsPrefix = null;
  private String metricsSource = null;
  private Wavefront wavefront = null;

  @Override
  public void init(SubsetConfiguration conf) {
    // Get Wavefront host configurations.
    final String serverHost = conf.getString(SERVER_HOST_KEY);
    final int serverPort = Integer
        .parseInt(conf.getString(SERVER_PORT_KEY));

    // Get Wavefront metrics graph prefix.
    metricsPrefix = conf.getString(METRICS_PREFIX);
    if (metricsPrefix == null) {
      metricsPrefix = "";
    }

    // get metrics source, if defined
    metricsSource = conf.getString(METRICS_SOURCE);

    wavefront = new Wavefront(serverHost, serverPort);
    wavefront.connect();
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    StringBuilder lines = new StringBuilder();
    StringBuilder metricsPathPrefix = new StringBuilder();
    StringBuilder pointTags = new StringBuilder("");
    String source = metricsSource;

    // Configure the hierarchical place to display the graph.
    metricsPathPrefix.append(metricsPrefix).append(".")
            .append(record.context()).append(".").append(record.name());

    for (MetricsTag tag : record.tags()) {
      if (tag.value() != null) {
        pointTags.append(" ");
        pointTags.append(tag.name());
        pointTags.append("=\"");
        pointTags.append(tag.value().replaceAll("\\\"", "\\\\\""));
        pointTags.append("\"");

        // if point tag contains key of hostname,
        // use it instead.
        if(tag.name().equalsIgnoreCase(MsInfo.Hostname.name())) {
          source = tag.value();
        }
      }
    }

    // if source is not defined, default to unknown.
    if(source == null || source.trim().length() == 0) {
      source = "unknown";
    }

    // The record timestamp is in milliseconds while wavefront expects an epoc
    // time in seconds.
    long timestamp = record.timestamp() / 1000L;

    // Collect datapoints.
    for (AbstractMetric metric : record.metrics()) {
      lines.append(
          metricsPathPrefix.toString() + "."
                  + metric.name().replace(' ', '.')).append(" ")
          .append(metric.value()).append(" ").append(timestamp)
          .append(" source=").append(source)
          .append(pointTags.toString())
          .append("\n");
    }

    try {
      wavefront.write(lines.toString());
    } catch (Exception e) {
      LOG.warn("Error sending metrics to Wavefront", e);
      try {
        wavefront.close();
      } catch (Exception e1) {
        throw new MetricsException("Error closing connection to Wavefront", e1);
      }
    }
  }

  @Override
  public void flush() {
    try {
      wavefront.flush();
    } catch (Exception e) {
      LOG.warn("Error flushing metrics to Wavefront", e);
      try {
        wavefront.close();
      } catch (Exception e1) {
        throw new MetricsException("Error closing connection to Wavefront", e1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    wavefront.close();
  }

  /**
   * Wavefront connection and input/output handler.
   */
  public static class Wavefront {
    private final static int MAX_CONNECTION_FAILURES = 5;
    private String serverHost;
    private int serverPort;
    private Writer writer = null;
    private Socket socket = null;
    private int connectionFailures = 0;

    public Wavefront(String serverHost, int serverPort) {
      this.serverHost = serverHost;
      this.serverPort = serverPort;
    }

    public void connect() {
      if (isConnected()) {
        throw new MetricsException("Already connected to Wavefront proxy");
      }
      if (tooManyConnectionFailures()) {
        // return silently (there was ERROR in logs when we reached limit for
        // the first time)
        return;
      }
      try {
        // Open a connection to Wavefront proxy server.
        socket = new Socket(serverHost, serverPort);
        writer = new OutputStreamWriter(socket.getOutputStream(),
            StandardCharsets.UTF_8);
      } catch (Exception e) {
        connectionFailures++;
        if (tooManyConnectionFailures()) {
          // first time when connection limit reached, report to logs
          LOG.error("Too many connection failures, would not try to connect "
              + "again.");
        }
        throw new MetricsException("Error creating connection, "
            + serverHost + ":" + serverPort, e);
      }
    }

    public void write(String msg) throws IOException {
      if (!isConnected()) {
        connect();
      }
      if (isConnected()) {
        writer.write(msg);
      }
    }

    public void flush() throws IOException {
      if (isConnected()) {
        writer.flush();
      }
    }

    public boolean isConnected() {
      return socket != null && socket.isConnected() && !socket.isClosed();
    }

    public void close() throws IOException {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ex) {
        if (socket != null) {
          socket.close();
        }
      } finally {
        socket = null;
        writer = null;
      }
    }

    private boolean tooManyConnectionFailures() {
      return connectionFailures > MAX_CONNECTION_FAILURES;
    }
  }
}
