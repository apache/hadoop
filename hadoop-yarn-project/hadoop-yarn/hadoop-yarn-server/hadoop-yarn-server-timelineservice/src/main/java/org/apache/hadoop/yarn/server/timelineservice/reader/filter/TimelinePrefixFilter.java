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
 * Filter class which represents filter to be applied based on prefixes.
 * Prefixes can either match or not match.
 */
@Private
@Unstable
public class TimelinePrefixFilter extends TimelineFilter {

  private TimelineCompareOp compareOp;
  private String prefix;

  public TimelinePrefixFilter() {
  }

  public TimelinePrefixFilter(TimelineCompareOp op, String prefix) {
    this.prefix = prefix;
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("CompareOp for prefix filter should " +
          "be EQUAL or NOT_EQUAL");
    }
    this.compareOp = op;
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.PREFIX;
  }

  public String getPrefix() {
    return prefix;
  }

  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  @Override
  public String toString() {
    return String.format("%s (%s %s)",
        this.getClass().getSimpleName(), this.compareOp.name(), this.prefix);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
    result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
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
    TimelinePrefixFilter other = (TimelinePrefixFilter) obj;
    if (compareOp != other.compareOp) {
      return false;
    }
    if (prefix == null) {
      if (other.prefix != null) {
        return false;
      }
    } else if (!prefix.equals(other.prefix)){
      return false;
    }
    return true;
  }
}