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

import java.util.ArrayList;
import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricCounter;
import org.apache.hadoop.metrics2.MetricGauge;
import org.apache.hadoop.metrics2.MetricsTag;

import org.apache.hadoop.metrics2.MetricsVisitor;

/**
 * Helper class to build MBeanInfo from metrics records
 */
class MBeanInfoBuilder implements MetricsVisitor {

  private final String name, description;
  private List<MBeanAttributeInfo> attrs;
  private Iterable<MetricsRecordImpl> recs;
  private int curRecNo;

  MBeanInfoBuilder(String name, String desc) {
    this.name = name;
    description = desc;
    attrs = new ArrayList<MBeanAttributeInfo>();
  }

  MBeanInfoBuilder reset(Iterable<MetricsRecordImpl> recs) {
    this.recs = recs;
    attrs.clear();
    return this;
  }

  MBeanAttributeInfo newAttrInfo(String name, String desc, String type) {
    return new MBeanAttributeInfo(getAttrName(name), type, desc,
                                  true, false, false); // read-only, non-is
  }

  MBeanAttributeInfo newAttrInfo(Metric m, String type) {
    return newAttrInfo(m.name(), m.description(), type);
  }

  public void gauge(MetricGauge<Integer> metric, int value) {
    attrs.add(newAttrInfo(metric, "java.lang.Integer"));
  }

  public void gauge(MetricGauge<Long> metric, long value) {
    attrs.add(newAttrInfo(metric, "java.lang.Long"));
  }

  public void gauge(MetricGauge<Float> metric, float value) {
    attrs.add(newAttrInfo(metric, "java.lang.Float"));
  }

  public void gauge(MetricGauge<Double> metric, double value) {
    attrs.add(newAttrInfo(metric, "java.lang.Double"));
  }

  public void counter(MetricCounter<Integer> metric, int value) {
    attrs.add(newAttrInfo(metric, "java.lang.Integer"));
  }

  public void counter(MetricCounter<Long> metric, long value) {
    attrs.add(newAttrInfo(metric, "java.lang.Long"));
  }

  String getAttrName(String name) {
    return curRecNo > 0 ? name +"."+ curRecNo : name;
  }

  MBeanInfo get() {
    curRecNo = 0;
    for (MetricsRecordImpl rec : recs) {
      for (MetricsTag t : rec.tags()) {
        attrs.add(newAttrInfo("tag."+ t.name(), t.description(),
                  "java.lang.String"));
      }
      for (Metric m : rec.metrics()) {
        m.visit(this);
      }
      ++curRecNo;
    }
    MBeanAttributeInfo[] attrsArray = new MBeanAttributeInfo[attrs.size()];
    return new MBeanInfo(name, description, attrs.toArray(attrsArray),
                         null, null, null); // no ops/ctors/notifications
  }
}
