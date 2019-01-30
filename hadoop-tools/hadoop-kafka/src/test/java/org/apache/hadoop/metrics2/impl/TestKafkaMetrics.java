/*
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

import com.google.common.collect.Lists;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.sink.KafkaSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringJoiner;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This tests that the KafkaSink properly formats the Kafka message.
 */
public class TestKafkaMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKafkaMetrics.class);
  private KafkaSink kafkaSink;

  enum KafkaMetricsInfo implements MetricsInfo {
    KafkaMetrics("Kafka related metrics etc."), KafkaCounter(
        "Kafka counter."), KafkaTag("Kafka tag.");
    // metrics

    private final String desc;

    KafkaMetricsInfo(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
          .add("name=" + name())
          .add("description=" + desc)
          .toString();
    }
  }

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testPutMetrics() throws Exception {
    // Create a record by mocking MetricsRecord class.
    MetricsRecord record = mock(MetricsRecord.class);
    when(record.tags()).thenReturn(Lists
        .newArrayList(new MetricsTag(KafkaMetricsInfo.KafkaTag, "test_tag")));
    when(record.timestamp()).thenReturn(System.currentTimeMillis());

    // Create a metric using AbstractMetric class.
    AbstractMetric metric = new AbstractMetric(KafkaMetricsInfo.KafkaCounter) {
      @Override
      public Number value() {
        return new Integer(123);
      }

      @Override
      public MetricType type() {
        return null;
      }

      @Override
      public void visit(MetricsVisitor visitor) {

      }
    };

    // Create a list of metrics.
    Iterable<AbstractMetric> metrics = Lists.newArrayList(metric);
    when(record.name()).thenReturn("Kafka record name");
    when(record.metrics()).thenReturn(metrics);
    SubsetConfiguration conf = mock(SubsetConfiguration.class);
    when(conf.getString(KafkaSink.BROKER_LIST)).thenReturn("localhost:9092");
    String topic = "myTestKafkaTopic";
    when(conf.getString(KafkaSink.TOPIC)).thenReturn(topic);

    // Create the KafkaSink object and initialize it.
    kafkaSink = new KafkaSink();
    kafkaSink.init(conf);

    // Create a mock KafkaProducer as a producer for KafkaSink.
    Producer<Integer, byte[]> mockProducer = mock(KafkaProducer.class);
    kafkaSink.setProducer(mockProducer);

    // Create the json object from the record.
    StringBuilder jsonLines = recordToJson(record);
    if (LOG.isDebugEnabled()) {
      LOG.debug("kafka message: " + jsonLines.toString());
    }

    // Send the record and store the result in a mock Future.
    Future<RecordMetadata> f = mock(Future.class);
    when(mockProducer.send(any())).thenReturn(f);
    kafkaSink.putMetrics(record);

    // Get the argument and verity it.
    ArgumentCaptor<ProducerRecord> argument =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockProducer).send(argument.capture());

    // Compare the received data with the original one.
    ProducerRecord<Integer, byte[]> data = (argument.getValue());
    String jsonResult = new String(data.value());
    if (LOG.isDebugEnabled()) {
      LOG.debug("kafka result: " + jsonResult);
    }
    assertEquals(jsonLines.toString(), jsonResult);
  }

  StringBuilder recordToJson(MetricsRecord record) {
    // Create a json object from a metrics record.
    StringBuilder jsonLines = new StringBuilder();
    Long timestamp = record.timestamp();
    Date currDate = new Date(timestamp);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String date = dateFormat.format(currDate);
    SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
    String time = timeFormat.format(currDate);
    String hostname = new String("null");
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.warn("Error getting Hostname, going to continue");
    }
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
    for (AbstractMetric m : record.metrics()) {
      jsonLines.append(
          ", \"" + m.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
      jsonLines.append(" \"" + m.value().toString() + "\"");
    }
    jsonLines.append("}");
    return jsonLines;
  }
}
