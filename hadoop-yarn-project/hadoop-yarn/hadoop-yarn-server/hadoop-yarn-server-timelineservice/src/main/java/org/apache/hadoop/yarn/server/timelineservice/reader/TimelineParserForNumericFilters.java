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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * Used for parsing numerical filters such as metric filters.
 */
@Private
@Unstable
class TimelineParserForNumericFilters extends TimelineParserForCompareExpr {

  public TimelineParserForNumericFilters(String expression) {
    super(expression, "Metric Filter");
  }

  protected TimelineFilter createFilter() {
    return new TimelineCompareFilter();
  }

  @Override
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp,
      boolean keyMustExistFlag) {
    ((TimelineCompareFilter)getCurrentFilter()).setCompareOp(
        compareOp, keyMustExistFlag);
  }

  protected Object parseValue(String strValue) throws TimelineParseException {
    Object value = null;
    try {
      value = GenericObjectMapper.OBJECT_READER.readValue(strValue);
    } catch (IOException e) {
      throw new TimelineParseException("Value cannot be parsed.");
    }
    if (value == null || !(TimelineStorageUtils.isIntegralValue(value))) {
      throw new TimelineParseException("Value is not a number.");
    }
    return value;
  }

  protected void setValueToCurrentFilter(Object value) {
    TimelineFilter currentFilter = getCurrentFilter();
    if (currentFilter != null) {
      ((TimelineCompareFilter)currentFilter).setValue(value);
    }
  }
}
