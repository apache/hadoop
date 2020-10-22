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

import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
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
    attrs = Lists.newArrayList();
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

  MBeanAttributeInfo newAttrInfo(MetricsInfo info, String type) {
    return newAttrInfo(info.name(), info.description(), type);
  }

  @Override
  public void gauge(MetricsInfo info, int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void gauge(MetricsInfo info, long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
  }

  @Override
  public void gauge(MetricsInfo info, float value) {
    attrs.add(newAttrInfo(info, "java.lang.Float"));
  }

  @Override
  public void gauge(MetricsInfo info, double value) {
    attrs.add(newAttrInfo(info, "java.lang.Double"));
  }

  @Override
  public void counter(MetricsInfo info, int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void counter(MetricsInfo info, long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
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
      for (AbstractMetric m : rec.metrics()) {
        m.visit(this);
      }
      ++curRecNo;
    }
    MetricsSystemImpl.LOG.debug("{}", attrs);
    MBeanAttributeInfo[] attrsArray = new MBeanAttributeInfo[attrs.size()];
    return new MBeanInfo(name, description, attrs.toArray(attrsArray),
                         null, null, null); // no ops/ctors/notifications
  }
}
