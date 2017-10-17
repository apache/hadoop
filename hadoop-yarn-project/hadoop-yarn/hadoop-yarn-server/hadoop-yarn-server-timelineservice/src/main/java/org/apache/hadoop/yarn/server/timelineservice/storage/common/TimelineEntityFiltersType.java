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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter.TimelineFilterType;

/**
 * Used to define which filter to match.
 */
enum TimelineEntityFiltersType {
  CONFIG {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUE;
    }
  },
  INFO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUE;
    }
  },
  METRIC {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.COMPARE;
    }
  },
  EVENT {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.EXISTS;
    }
  },
  IS_RELATED_TO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUES;
    }
  },
  RELATES_TO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUES;
    }
  };

  /**
   * Checks whether filter type is valid for the filter being matched.
   *
   * @param filterType filter type.
   * @return true, if its a valid filter, false otherwise.
   */
  abstract boolean isValidFilter(TimelineFilterType filterType);
}