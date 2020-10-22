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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Class to carry the offline aggregation information for storage level
 * implementations. There are currently two predefined aggregation info
 * instances that represent flow and user level offline aggregations. Depend on
 * its implementation, a storage class may use an OfflineAggregationInfo object
 * to decide behaviors dynamically.
 */
public final class OfflineAggregationInfo {
  /**
   * Default flow level aggregation table name.
   */
  @VisibleForTesting
  public static final String FLOW_AGGREGATION_TABLE_NAME
      = "yarn_timeline_flow_aggregation";
  /**
   * Default user level aggregation table name.
   */
  public static final String USER_AGGREGATION_TABLE_NAME
      = "yarn_timeline_user_aggregation";

  // These lists are not taking effects in table creations.
  private static final String[] FLOW_AGGREGATION_PK_LIST = {
      "user", "cluster", "flow_name"
  };
  private static final String[] USER_AGGREGATION_PK_LIST = {
      "user", "cluster"
  };

  private final String tableName;
  private final String[] primaryKeyList;
  private final PrimaryKeyStringSetter primaryKeyStringSetter;

  private OfflineAggregationInfo(String table, String[] pkList,
      PrimaryKeyStringSetter formatter) {
    tableName = table;
    primaryKeyList = pkList;
    primaryKeyStringSetter = formatter;
  }

  private interface PrimaryKeyStringSetter {
    int setValues(PreparedStatement ps, TimelineCollectorContext context,
        String[] extraInfo, int startPos) throws SQLException;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getPrimaryKeyList() {
    return primaryKeyList.clone();
  }

  public int setStringsForPrimaryKey(PreparedStatement ps,
      TimelineCollectorContext context, String[] extraInfo, int startPos)
      throws SQLException {
    return primaryKeyStringSetter.setValues(ps, context, extraInfo, startPos);
  }

  public static final OfflineAggregationInfo FLOW_AGGREGATION =
      new OfflineAggregationInfo(FLOW_AGGREGATION_TABLE_NAME,
          FLOW_AGGREGATION_PK_LIST,
          new PrimaryKeyStringSetter() {
          @Override
          public int setValues(PreparedStatement ps,
              TimelineCollectorContext context, String[] extraInfo,
              int startPos) throws SQLException {
            int idx = startPos;
            ps.setString(idx++, context.getUserId());
            ps.setString(idx++, context.getClusterId());
            ps.setString(idx++, context.getFlowName());
            return idx;
          }
        });

  public static final OfflineAggregationInfo USER_AGGREGATION =
      new OfflineAggregationInfo(USER_AGGREGATION_TABLE_NAME,
          USER_AGGREGATION_PK_LIST,
          new PrimaryKeyStringSetter() {
          @Override
          public int setValues(PreparedStatement ps,
              TimelineCollectorContext context, String[] extraInfo,
              int startPos) throws SQLException {
            int idx = startPos;
            ps.setString(idx++, context.getUserId());
            ps.setString(idx++, context.getClusterId());
            return idx;
          }
        });
}
