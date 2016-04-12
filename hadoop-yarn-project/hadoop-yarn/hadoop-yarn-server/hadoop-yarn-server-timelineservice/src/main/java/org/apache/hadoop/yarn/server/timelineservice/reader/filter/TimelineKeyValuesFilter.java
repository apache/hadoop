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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Filter class which represents filter to be applied based on multiple values
 * for a key and these values being equal or not equal to values in back-end
 * store.
 */
@Private
@Unstable
public class TimelineKeyValuesFilter extends TimelineFilter {
  private final TimelineCompareOp compareOp;
  private final String key;
  private final Set<Object> values;
  public TimelineKeyValuesFilter(TimelineCompareOp op, String key,
      Set<Object> values) {
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for multi value "
          + "equality filter should be EQUAL or NOT_EQUAL");
    }
    this.compareOp = op;
    this.key = key;
    this.values = values;
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.KEY_VALUES;
  }

  public String getKey() {
    return key;
  }

  public Set<Object> getValues() {
    return values;
  }

  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s:%s)",
        this.getClass().getSimpleName(), this.compareOp.name(),
        this.key, (values == null) ? "" : values.toString());
  }
}
