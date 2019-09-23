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

package org.apache.hadoop.metrics2.lib;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MutableMetricsFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(MutableMetricsFactory.class);

  MutableMetric newForField(Field field, Metric annotation,
                            MetricsRegistry registry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("field "+ field +" with annotation "+ annotation);
    }
    MetricsInfo info = getInfo(annotation, field);
    MutableMetric metric = newForField(field, annotation);
    if (metric != null) {
      registry.add(info.name(), metric);
      return metric;
    }
    final Class<?> cls = field.getType();
    if (cls == MutableCounterInt.class) {
      return registry.newCounter(info, 0);
    }
    if (cls == MutableCounterLong.class) {
      return registry.newCounter(info, 0L);
    }
    if (cls == MutableGaugeInt.class) {
      return registry.newGauge(info, 0);
    }
    if (cls == MutableGaugeLong.class) {
      return registry.newGauge(info, 0L);
    }
    if (cls == MutableGaugeFloat.class) {
      return registry.newGauge(info, 0f);
    }
    if (cls == MutableRate.class) {
      return registry.newRate(info.name(), info.description(),
                              annotation.always());
    }
    if (cls == MutableRates.class) {
      return new MutableRates(registry);
    }
    if (cls == MutableRatesWithAggregation.class) {
      return registry.newRatesWithAggregation(info.name());
    }
    if (cls == MutableStat.class) {
      return registry.newStat(info.name(), info.description(),
                              annotation.sampleName(), annotation.valueName(),
                              annotation.always());
    }
    if (cls == MutableRollingAverages.class) {
      return registry.newMutableRollingAverages(info.name(),
          annotation.valueName());
    }
    if (cls == MutableQuantiles.class) {
      return registry.newQuantiles(info.name(), annotation.about(),
          annotation.sampleName(), annotation.valueName(), annotation.interval());
    }
    throw new MetricsException("Unsupported metric field "+ field.getName() +
                               " of type "+ field.getType().getName());
  }

  MutableMetric newForMethod(Object source, Method method, Metric annotation,
                             MetricsRegistry registry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("method "+ method +" with annotation "+ annotation);
    }
    MetricsInfo info = getInfo(annotation, method);
    MutableMetric metric = newForMethod(source, method, annotation);
    metric = metric != null ? metric :
        new MethodMetric(source, method, info, annotation.type());
    registry.add(info.name(), metric);
    return metric;
  }

  /**
   * Override to handle custom mutable metrics for fields
   * @param field of the metric
   * @param annotation  of the field
   * @return a new metric object or null
   */
  protected MutableMetric newForField(Field field, Metric annotation) {
    return null;
  }

  /**
   * Override to handle custom mutable metrics for methods
   * @param source the metrics source object
   * @param method to return the metric
   * @param annotation of the method
   * @return a new metric object or null
   */
  protected MutableMetric newForMethod(Object source, Method method,
                                       Metric annotation) {
    return null;
  }

  protected MetricsInfo getInfo(Metric annotation, Field field) {
    return getInfo(annotation, getName(field));
  }

  protected String getName(Field field) {
    return StringUtils.capitalize(field.getName());
  }

  protected MetricsInfo getInfo(Metric annotation, Method method) {
    return getInfo(annotation, getName(method));
  }

  protected MetricsInfo getInfo(Class<?> cls, Metrics annotation) {
    String name = annotation.name();
    String about = annotation.about();
    String name2 = name.isEmpty() ? cls.getSimpleName() : name;
    return Interns.info(name2, about.isEmpty() ? name2 : about);
  }

  /**
   * Remove the prefix "get", if any, from the method name. Return the
   * capacitalized method name."
   */
  protected String getName(Method method) {
    String methodName = method.getName();
    if (methodName.startsWith("get")) {
      return StringUtils.capitalize(methodName.substring(3));
    }
    return StringUtils.capitalize(methodName);
  }

  protected MetricsInfo getInfo(Metric annotation, String defaultName) {
    String[] value = annotation.value();
    if (value.length == 2) {
      // Use name and description from the annotation
      return Interns.info(value[0], value[1]);
    }
    if (value.length == 1) {
      // Use description from the annotation and method name as metric name
      return Interns.info(defaultName, value[0]);
    }
    // Use method name as metric name and description
    return Interns.info(defaultName, defaultName);
  }
}
