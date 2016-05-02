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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;

/**
 * Used for parsing key-value filters such as config and info filters.
 */
@Private
@Unstable
class TimelineParserForKVFilters extends TimelineParserForCompareExpr {
  // Indicates if value has to be interpreted as a string.
  private final boolean valueAsString;
  public TimelineParserForKVFilters(String expression, boolean valAsStr) {
    super(expression, "Config/Info Filter");
    this.valueAsString = valAsStr;
  }

  protected TimelineFilter createFilter() {
    return new TimelineKeyValueFilter();
  }

  protected Object parseValue(String strValue) {
    if (!valueAsString) {
      try {
        return GenericObjectMapper.OBJECT_READER.readValue(strValue);
      } catch (IOException e) {
        return strValue;
      }
    } else {
      return strValue;
    }
  }

  @Override
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp,
      boolean keyMustExistFlag) throws TimelineParseException {
    if (compareOp != TimelineCompareOp.EQUAL &&
        compareOp != TimelineCompareOp.NOT_EQUAL) {
      throw new TimelineParseException("TimelineCompareOp for kv-filter " +
          "should be EQUAL or NOT_EQUAL");
    }
    ((TimelineKeyValueFilter)getCurrentFilter()).setCompareOp(
        compareOp, keyMustExistFlag);
  }

  @Override
  protected void setValueToCurrentFilter(Object value) {
    TimelineFilter currentFilter = getCurrentFilter();
    if (currentFilter != null) {
      ((TimelineKeyValueFilter)currentFilter).setValue(value);
    }
  }
}
