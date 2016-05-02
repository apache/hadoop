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
  // If comparison operator is NOT_EQUAL, this flag decides if we should return
  // the entity if key does not exist.
  private boolean keyMustExist = true;

  public TimelineCompareFilter() {
  }

  public TimelineCompareFilter(TimelineCompareOp op, String key, Object val,
       boolean keyMustExistFlag) {
    this.compareOp = op;
    this.key = key;
    this.value = val;
    if (op == TimelineCompareOp.NOT_EQUAL) {
      this.keyMustExist = keyMustExistFlag;
    } else {
      this.keyMustExist = true;
    }
  }

  public TimelineCompareFilter(TimelineCompareOp op, String key, Object val) {
    this(op, key, val, true);
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

  public void setKey(String keyToBeSet) {
    key = keyToBeSet;
  }

  public Object getValue() {
    return value;
  }

  public void setCompareOp(TimelineCompareOp timelineCompareOp,
      boolean keyExistFlag) {
    this.compareOp = timelineCompareOp;
    if (timelineCompareOp == TimelineCompareOp.NOT_EQUAL) {
      this.keyMustExist = keyExistFlag;
    }
  }

  public void setValue(Object val) {
    value = val;
  }

  public boolean getKeyMustExist() {
    return keyMustExist;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + (keyMustExist ? 1231 : 1237);
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimelineCompareFilter other = (TimelineCompareFilter) obj;
    if (compareOp != other.compareOp) {
      return false;
    }
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (keyMustExist != other.keyMustExist) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s:%s:%b)",
        this.getClass().getSimpleName(), this.compareOp.name(),
        this.key, this.value, this.keyMustExist);
  }
}