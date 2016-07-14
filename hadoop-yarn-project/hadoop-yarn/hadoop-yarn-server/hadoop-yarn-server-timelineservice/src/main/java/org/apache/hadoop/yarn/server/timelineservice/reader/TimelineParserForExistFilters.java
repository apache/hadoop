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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;

/**
 * Used for parsing existence filters such as event filters. These filters
 * check for existence of a value. For example, in case of event filters, they
 * check if an event exists or not and accordingly return an entity.
 */
@Private
@Unstable
class TimelineParserForExistFilters extends TimelineParserForEqualityExpr {

  public TimelineParserForExistFilters(String expression, char delimiter) {
    super(expression, "Event Filter", delimiter);
  }

  protected TimelineFilter createFilter() {
    return new TimelineExistsFilter();
  }

  protected void setValueToCurrentFilter(String value) {
    ((TimelineExistsFilter)getCurrentFilter()).setValue(value);
  }

  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp) {
    ((TimelineExistsFilter)getCurrentFilter()).setCompareOp(compareOp);
  }
}
