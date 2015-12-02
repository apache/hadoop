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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Filter class which represents filter to be applied based on key-value pair
 * and the relation between them represented by different relational operators.
 */
@Private
@Unstable
public class TimelineCompareFilter extends TimelineFilter {

  private TimelineCompareOp compareOp;
  private String key;
  private Object value;

  public TimelineCompareFilter() {
  }

  public TimelineCompareFilter(TimelineCompareOp op, String key, Object val) {
    this.compareOp = op;
    this.key = key;
    this.value = val;
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.COMPARE;
  }

  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  public String getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }
}