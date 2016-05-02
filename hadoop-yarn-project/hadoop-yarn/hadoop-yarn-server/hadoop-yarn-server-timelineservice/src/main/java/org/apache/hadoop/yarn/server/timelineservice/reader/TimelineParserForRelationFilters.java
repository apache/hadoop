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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * Used for parsing relation filters.
 */
@Private
@Unstable
class TimelineParserForRelationFilters extends
    TimelineParserForEqualityExpr {
  private final String valueDelimiter;
  public TimelineParserForRelationFilters(String expression, char valuesDelim,
      String valueDelim) {
    super(expression, "Relation Filter", valuesDelim);
    valueDelimiter = valueDelim;
  }

  @Override
  protected TimelineFilter createFilter() {
    return new TimelineKeyValuesFilter();
  }

  @Override
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp) {
    ((TimelineKeyValuesFilter)getCurrentFilter()).setCompareOp(compareOp);
  }

  @Override
  protected void setValueToCurrentFilter(String value)
       throws TimelineParseException {
    if (value != null) {
      String[] pairStrs = value.split(valueDelimiter);
      if (pairStrs.length < 2) {
        throw new TimelineParseException("Invalid relation filter expression");
      }
      String key = pairStrs[0].trim();
      Set<Object> values = new HashSet<Object>();
      for (int i = 1; i < pairStrs.length; i++) {
        values.add(pairStrs[i].trim());
      }
      ((TimelineKeyValuesFilter)getCurrentFilter()).
          setKeyAndValues(key, values);
    }
  }
}
