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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Implementation of {@link TimelineFilter} that represents an ordered list of
 * timeline filters which will then be evaluated with a specified boolean
 * operator {@link Operator#AND} or {@link Operator#OR}. Since you can use
 * timeline filter lists as children of timeline filter lists, you can create a
 * hierarchy of filters to be evaluated.
 */
@Private
@Unstable
public class TimelineFilterList extends TimelineFilter {
  /**
   * Specifies how filters in the filter list will be evaluated. AND means all
   * the filters should match and OR means atleast one should match.
   */
  @Private
  @Unstable
  public static enum Operator {
    AND,
    OR
  }

  private Operator operator;
  private List<TimelineFilter> filterList = new ArrayList<TimelineFilter>();

  public TimelineFilterList(TimelineFilter...filters) {
    this(Operator.AND, filters);
  }

  public TimelineFilterList(Operator op, TimelineFilter...filters) {
    this.operator = op;
    this.filterList = new ArrayList<TimelineFilter>(Arrays.asList(filters));
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.LIST;
  }

  /**
   * Get the filter list.
   *
   * @return filterList
   */
  public List<TimelineFilter> getFilterList() {
    return filterList;
  }

  /**
   * Get the operator.
   *
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator op) {
    operator = op;
  }

  public void addFilter(TimelineFilter filter) {
    filterList.add(filter);
  }
}