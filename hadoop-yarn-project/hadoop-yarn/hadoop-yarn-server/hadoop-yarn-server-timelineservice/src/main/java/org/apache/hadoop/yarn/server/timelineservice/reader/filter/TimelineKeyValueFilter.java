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
 * being equal or not to the values in back-end store.
 */
@Private
@Unstable
public class TimelineKeyValueFilter extends TimelineCompareFilter {
  public TimelineKeyValueFilter() {
  }

  public TimelineKeyValueFilter(TimelineCompareOp op, String key, Object val,
      boolean keyMustExistFlag) {
    super(op, key, val, keyMustExistFlag);
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for equality"
          + " filter should be EQUAL or NOT_EQUAL");
    }
  }

  public TimelineKeyValueFilter(TimelineCompareOp op, String key, Object val) {
    this(op, key, val, true);
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.KEY_VALUE;
  }

  public void setCompareOp(TimelineCompareOp timelineCompareOp,
      boolean keyExistFlag) {
    if (timelineCompareOp != TimelineCompareOp.EQUAL &&
        timelineCompareOp != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for equality"
          + " filter should be EQUAL or NOT_EQUAL");
    }
    super.setCompareOp(timelineCompareOp, keyExistFlag);
  }
}
