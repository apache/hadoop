/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A metrics sink that writes to a Kafka broker. This requires you to configure
 * a broker_list and a topic in the metrics2 configuration file. The broker_list
 * must contain a comma-separated list of kafka broker host and ports. The topic
 * will contain only one topic.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KafkaSink implements MetricsSink, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
  public static final String BROKER_LIST = "broker_list";
  public static final String TOPIC = "topic";

  private String hostname = null;
  private String brokerList = null;
  private String topic = null;
  private Producer<Integer, byte[]> producer = null;

  private final DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private final DateTimeFormatter timeFormat =
      DateTimeFormatter.ofPattern("HH:mm:ss");
  private final ZoneId zoneId = ZoneId.systemDefault();

  public void setProducer(Producer<Integer, byte[]> p) {
    this.producer = p;
  }

  @Override
  public void init(SubsetConfiguration conf) {
    // Get Kafka broker configuration.
    Properties props = new Properties();
    brokerList = conf.getString(BROKER_LIST);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Broker list " + brokerList);
    }
    props.put("bootstrap.servers", brokerList);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Kafka brokers: " + brokerList);
    }

    // Get Kafka topic configuration.
    topic = conf.getString(TOPIC);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Kafka topic " + topic);
    }
    if (Strings.isNullOrEmpty(topic)) {
      throw new MetricsException("Kafka topic can not be null");
    }

    // Set the rest of Kafka configuration.
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("request.required.acks", "0");

    // Set the hostname once and use it in every message.
    hostname = "null";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.warn("Error getting Hostname, going to continue");
    }

    try {
      // Create the producer object.
      producer = new KafkaProducer<Integer, byte[]>(props);
    } catch (Exception e) {
      throw new MetricsException("Error creating Producer, " + brokerList, e);
    }
  }

  @Override
  public void putMetrics(MetricsRecord record) {

    if (producer == null) {
      throw new MetricsException("Producer in KafkaSink is null!");
    }

    // Create the json object.
    StringBuilder jsonLines = new StringBuilder();

    long timestamp = record.timestamp();
    Instant instant = Instant.ofEpochMilli(timestamp);
    LocalDateTime ldt = LocalDateTime.ofInstant(instant, zoneId);
    String date = ldt.format(dateFormat);
    String time = ldt.format(timeFormat);

    // Collect datapoints and populate the json object.
    jsonLines.append("{\"hostname\": \"" + hostname);
    jsonLines.append("\", \"timestamp\": " + timestamp);
    jsonLines.append(", \"date\": \"" + date);
    jsonLines.append("\",\"time\": \"" + time);
    jsonLines.append("\",\"name\": \"" + record.name() + "\" ");
    for (MetricsTag tag : record.tags()) {
      jsonLines.append(
          ", \"" + tag.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
      jsonLines.append(" \"" + tag.value().toString() + "\"");
    }
    for (AbstractMetric metric : record.metrics()) {
      jsonLines.append(", \""
          + metric.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
      jsonLines.append(" \"" + metric.value().toString() + "\"");
    }
    jsonLines.append("}");
    LOG.debug("kafka message: " + jsonLines.toString());

    // Create the record to be sent from the json.
    ProducerRecord<Integer, byte[]> data = new ProducerRecord<Integer, byte[]>(
        topic, jsonLines.toString().getBytes(Charset.forName("UTF-8")));

    // Send the data to the Kafka broker. Here is an example of this data:
    // {"hostname": "...", "timestamp": 1436913651516,
    // "date": "2015-6-14","time": "22:40:51","context": "yarn","name":
    // "QueueMetrics, "running_0": "1", "running_60": "0", "running_300": "0",
    // "running_1440": "0", "AppsSubmitted": "1", "AppsRunning": "1",
    // "AppsPending": "0", "AppsCompleted": "0", "AppsKilled": "0",
    // "AppsFailed": "0", "AllocatedMB": "134656", "AllocatedVCores": "132",
    // "AllocatedContainers": "132", "AggregateContainersAllocated": "132",
    // "AggregateContainersReleased": "0", "AvailableMB": "0",
    // "AvailableVCores": "0", "PendingMB": "275456", "PendingVCores": "269",
    // "PendingContainers": "269", "ReservedMB": "0", "ReservedVCores": "0",
    // "ReservedContainers": "0", "ActiveUsers": "1", "ActiveApplications": "1"}
    Future<RecordMetadata> future = producer.send(data);
    jsonLines.setLength(0);
    try {
      future.get();
    } catch (InterruptedException e) {
      throw new MetricsException("Error sending data", e);
    } catch (ExecutionException e) {
      throw new MetricsException("Error sending data", e);
    }
  }

  @Override
  public void flush() {
    LOG.debug("Kafka seems not to have any flush() mechanism!");
  }

  @Override
  public void close() throws IOException {
    // Close the producer and set it to null.
    try {
      producer.close();
    } catch (RuntimeException e) {
      throw new MetricsException("Error closing producer", e);
    } finally {
      producer = null;
    }
  }
}
