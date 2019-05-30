/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.utils;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Adapter JMX bean to publish all the Rocksdb metrics.
 */
public class RocksDBStoreMBean implements DynamicMBean, MetricsSource {

  private Statistics statistics;

  private Set<String> histogramAttributes = new HashSet<>();

  private String contextName;

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBStoreMBean.class);

  public final static String ROCKSDB_CONTEXT_PREFIX = "Rocksdb_";

  public RocksDBStoreMBean(Statistics statistics, String dbName) {
    this.contextName = ROCKSDB_CONTEXT_PREFIX + dbName;
    this.statistics = statistics;
    histogramAttributes.add("Average");
    histogramAttributes.add("Median");
    histogramAttributes.add("Percentile95");
    histogramAttributes.add("Percentile99");
    histogramAttributes.add("StandardDeviation");
  }

  public static RocksDBStoreMBean create(Statistics statistics,
                                         String contextName) {

    RocksDBStoreMBean rocksDBStoreMBean = new RocksDBStoreMBean(
        statistics, contextName);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    MetricsSource metricsSource = ms.getSource(rocksDBStoreMBean.contextName);
    if (metricsSource != null) {
      return (RocksDBStoreMBean)metricsSource;
    } else {
      return ms.register(rocksDBStoreMBean.contextName,
          "RocksDB Metrics",
          rocksDBStoreMBean);
    }
  }

  @Override
  public Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    for (String histogramAttribute : histogramAttributes) {
      if (attribute.endsWith("_" + histogramAttribute.toUpperCase())) {
        String keyName = attribute
            .substring(0, attribute.length() - histogramAttribute.length() - 1);
        try {
          HistogramData histogram =
              statistics.getHistogramData(HistogramType.valueOf(keyName));
          try {
            Method method =
                HistogramData.class.getMethod("get" + histogramAttribute);
            return method.invoke(histogram);
          } catch (Exception e) {
            throw new ReflectionException(e,
                "Can't read attribute " + attribute);
          }
        } catch (IllegalArgumentException exception) {
          throw new AttributeNotFoundException(
              "No such attribute in RocksDB stats: " + attribute);
        }
      }
    }
    try {
      return statistics.getTickerCount(TickerType.valueOf(attribute));
    } catch (IllegalArgumentException ex) {
      throw new AttributeNotFoundException(
          "No such attribute in RocksDB stats: " + attribute);
    }
  }

  @Override
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {

  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList result = new AttributeList();
    for (String attributeName : attributes) {
      try {
        Object value = getAttribute(attributeName);
        result.add(value);
      } catch (Exception e) {
        //TODO
      }
    }
    return result;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature)
      throws MBeanException, ReflectionException {
    return null;
  }

  @Override
  public MBeanInfo getMBeanInfo() {

    List<MBeanAttributeInfo> attributes = new ArrayList<>();
    for (TickerType tickerType : TickerType.values()) {
      attributes.add(new MBeanAttributeInfo(tickerType.name(), "long",
          "RocksDBStat: " + tickerType.name(), true, false, false));
    }
    for (HistogramType histogramType : HistogramType.values()) {
      for (String histogramAttribute : histogramAttributes) {
        attributes.add(new MBeanAttributeInfo(
            histogramType.name() + "_" + histogramAttribute.toUpperCase(),
            "long", "RocksDBStat: " + histogramType.name(), true, false,
            false));
      }
    }

    return new MBeanInfo("", "RocksDBStat",
        attributes.toArray(new MBeanAttributeInfo[0]), null, null, null);

  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean b) {
    MetricsRecordBuilder rb = metricsCollector.addRecord(contextName);
    getHistogramData(rb);
    getTickerTypeData(rb);
  }

  /**
   * Collect all histogram metrics from RocksDB statistics.
   * @param rb Metrics Record Builder.
   */
  private void getHistogramData(MetricsRecordBuilder rb) {
    for (HistogramType histogramType : HistogramType.values()) {
      HistogramData histogram =
          statistics.getHistogramData(
              HistogramType.valueOf(histogramType.name()));
      for (String histogramAttribute : histogramAttributes) {
        try {
          Method method =
              HistogramData.class.getMethod("get" + histogramAttribute);
          double metricValue =  (double) method.invoke(histogram);
          rb.addGauge(Interns.info(histogramType.name() + "_" +
                  histogramAttribute.toUpperCase(), "RocksDBStat"),
              metricValue);
        } catch (Exception e) {
          LOG.error("Error reading histogram data {} ", e);
        }
      }
    }
  }

  /**
   * Collect all Counter metrics from RocksDB statistics.
   * @param rb Metrics Record Builder.
   */
  private void getTickerTypeData(MetricsRecordBuilder rb) {
    for (TickerType tickerType : TickerType.values()) {
      rb.addCounter(Interns.info(tickerType.name(), "RocksDBStat"),
          statistics.getTickerCount(tickerType));
    }
  }

}
