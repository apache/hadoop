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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.Closeable;
import java.net.Socket;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * A metrics sink that writes to a Graphite server
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GraphiteSink implements MetricsSink, Closeable {
    private static final Log LOG = LogFactory.getLog(GraphiteSink.class);
    private static final String SERVER_HOST_KEY = "server_host";
    private static final String SERVER_PORT_KEY = "server_port";
    private static final String METRICS_PREFIX = "metrics_prefix";
    private Writer writer = null;
    private String metricsPrefix = null;
    private Socket socket = null;

    @Override
    public void init(SubsetConfiguration conf) {
        // Get Graphite host configurations.
        String serverHost = conf.getString(SERVER_HOST_KEY);
        Integer serverPort = Integer.parseInt(conf.getString(SERVER_PORT_KEY));

        // Get Graphite metrics graph prefix.
        metricsPrefix = conf.getString(METRICS_PREFIX);
        if (metricsPrefix == null)
            metricsPrefix = "";

        try {
            // Open an connection to Graphite server.
            socket = new Socket(serverHost, serverPort);
            writer = new OutputStreamWriter(socket.getOutputStream());
        } catch (Exception e) {
            throw new MetricsException("Error creating connection, "
                    + serverHost + ":" + serverPort, e);
        }
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

        // Round the timestamp to second as Graphite accepts it in such format.
        int timestamp = Math.round(record.timestamp() / 1000.0f);

        // Collect datapoints.
        for (AbstractMetric metric : record.metrics()) {
            lines.append(
                    metricsPathPrefix.toString() + "."
                            + metric.name().replace(' ', '.')).append(" ")
                    .append(metric.value()).append(" ").append(timestamp)
                    .append("\n");
        }

        try {
            if(writer != null){
              writer.write(lines.toString());
            } else {
              throw new MetricsException("Writer in GraphiteSink is null!");
            }
        } catch (Exception e) {
            throw new MetricsException("Error sending metrics", e);
        }
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        } catch (Exception e) {
            throw new MetricsException("Error flushing metrics", e);
        }
    }

    @Override
    public void close() throws IOException {
      try {
        IOUtils.closeStream(writer);
        writer = null;
        LOG.info("writer in GraphiteSink is closed!");
      } catch (Throwable e){
        throw new MetricsException("Error closing writer", e);
      } finally {
        if (socket != null && !socket.isClosed()) {
          socket.close();
          socket = null;
          LOG.info("socket in GraphiteSink is closed!");
        }
      }
    }
}
