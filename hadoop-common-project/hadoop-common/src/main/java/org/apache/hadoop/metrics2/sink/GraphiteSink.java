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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * A metrics sink that writes to a Graphite server
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GraphiteSink implements MetricsSink, Closeable {
    private static final Logger LOG =
        LoggerFactory.getLogger(GraphiteSink.class);
    private static final String SERVER_HOST_KEY = "server_host";
    private static final String SERVER_PORT_KEY = "server_port";
    private static final String METRICS_PREFIX = "metrics_prefix";
    private String metricsPrefix = null;
    private Graphite graphite = null;

    @Override
    public void init(SubsetConfiguration conf) {
        // Get Graphite host configurations.
        final String serverHost = conf.getString(SERVER_HOST_KEY);
        final int serverPort = Integer.parseInt(conf.getString(SERVER_PORT_KEY));

        // Get Graphite metrics graph prefix.
        metricsPrefix = conf.getString(METRICS_PREFIX);
        if (metricsPrefix == null)
            metricsPrefix = "";

        graphite = new Graphite(serverHost, serverPort);
        graphite.connect();
    }

    @Override
    public void putMetrics(MetricsRecord record) {
        StringBuilder lines = new StringBuilder();
        StringBuilder metricsPathPrefix = new StringBuilder();

        // Configure the hierarchical place to display the graph.
        metricsPathPrefix.append(metricsPrefix).append(".")
                .append(record.context()).append(".").append(record.name());

        for (MetricsTag tag : record.tags()) {
            if (tag.value() != null) {
                metricsPathPrefix.append(".");
                metricsPathPrefix.append(tag.name());
                metricsPathPrefix.append("=");
                metricsPathPrefix.append(tag.value());
            }
        }

        // The record timestamp is in milliseconds while Graphite expects an epoc time in seconds.
        long timestamp = record.timestamp() / 1000L;

        // Collect datapoints.
        for (AbstractMetric metric : record.metrics()) {
            lines.append(
                    metricsPathPrefix.toString() + "."
                            + metric.name().replace(' ', '.')).append(" ")
                    .append(metric.value()).append(" ").append(timestamp)
                    .append("\n");
        }

        try {
          graphite.write(lines.toString());
        } catch (Exception e) {
          LOG.warn("Error sending metrics to Graphite", e);
          try {
            graphite.close();
          } catch (Exception e1) {
            throw new MetricsException("Error closing connection to Graphite", e1);
          }
        }
    }

    @Override
    public void flush() {
      try {
        graphite.flush();
      } catch (Exception e) {
        LOG.warn("Error flushing metrics to Graphite", e);
        try {
          graphite.close();
        } catch (Exception e1) {
          throw new MetricsException("Error closing connection to Graphite", e1);
        }
      }
    }

    @Override
    public void close() throws IOException {
      graphite.close();
    }

    public static class Graphite {
      private final static int MAX_CONNECTION_FAILURES = 5;

      private String serverHost;
      private int serverPort;
      private Writer writer = null;
      private Socket socket = null;
      private int connectionFailures = 0;

      public Graphite(String serverHost, int serverPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
      }

      public void connect() {
        if (isConnected()) {
          throw new MetricsException("Already connected to Graphite");
        }
        if (tooManyConnectionFailures()) {
          // return silently (there was ERROR in logs when we reached limit for the first time)
          return;
        }
        try {
          // Open a connection to Graphite server.
          socket = new Socket(serverHost, serverPort);
        writer = new OutputStreamWriter(socket.getOutputStream(),
                StandardCharsets.UTF_8);
        } catch (Exception e) {
          connectionFailures++;
          if (tooManyConnectionFailures()) {
            // first time when connection limit reached, report to logs
            LOG.error("Too many connection failures, would not try to connect again.");
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
